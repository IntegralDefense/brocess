import sqlite3
import logging
import os


class DBEngine(object):
    def __init__(self, connectstring):
        self.connectstring = connectstring
        self.connection = None

    def open(self):
        if not self.connection:
            try:
                self.connection = sqlite3.connect(self.connectstring)
            except:
                return False
        return True

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def _destruct(self):
        self.close()
        if self.connectionstring == ":memory:":
            return
        os.remove(self.connectstring)

    def engine(self):
        return "sqlite"


class LogDB(object):
    def __init__(self, dbengine):
        self.dbengine = dbengine
        self.version = "1.0"
        self.commit_count = 0
        self.commit_limit = 1000

    def _getCursor(self):
        if hasattr(self, '_cursor'):
            return getattr(self, '_cursor')

        setattr(self, '_cursor', self.dbengine.connection.cursor())
        return getattr(self, '_cursor')

    def close(self):
        self.dbengine.connection.commit()
        self.dbengine.connection.close()
        self.dbengine.connection = None

    def _commit(self):
        self.commit_count += 1
        if self.commit_count >= self.commit_limit:
            self.dbengine.connection.commit()
            self.commit_count = 0

    def _exists(self, tablename):
        cursor = self._getCursor()
        cursor.execute("select count(type) from sqlite_master where tbl_name=?;", (tablename,))
        result = int(cursor.fetchone()[0])
        return True if result else False

    def _create_properties(self):
        cursor = self._getCursor()
        cursor.execute("create table if not exists properties (label TEXT UNIQUE,value TEXT);")
        cursor.execute("INSERT INTO properties (label,value) VALUES (?,?)", ("VERSION", self.version))
        self._commit()

    def _checkVersion(self):
        cursor = self._getCursor()
        cursor.execute("select value from properties where label=?", ("VERSION",))
        result = cursor.fetchone()
        if len(result) != 1:
            logging.critical("Database corruption.  Cannot determine version number.")
            return False
        if int(float(result[0])) != int(float(self.version)):
            logging.info("Database version mismatch.  Database version=" + result[0] + ", API version=" + self.version)
            return False
        if float(result[0]) < float(self.version):
            logging.info(
                "Database version mismatch.  Database version " + result[
                    0] + " less than API version " + self.version)
            return False
        return True

    def instantiate(self):
        if not self._exists("properties"):
            self._create_properties()
        else:
            if not self._checkVersion():
                return False
        cursor = self._getCursor()
        cursor.execute(
            "create table if not exists connlog (sourceip not null, destip not null, destport INTEGER not null, "
            " numconnections INTEGER, firstconnectdate, PRIMARY KEY(sourceip,destip,destport))"
        )
        cursor.execute(
            "create table if not exists connerr (sourceip not null, destip not null, destport INTEGER not null, "
            " numconnections INTEGER, firstconnectdate, PRIMARY KEY(sourceip,destip,destport))"
        )
        cursor.execute(
            "create table if not exists smtplog (source not null, destination not null, numconnections integer, "
            "firstconnectdate, PRIMARY KEY(source,destination))"
        )
        cursor.execute(
            "create table if not exists httplog (host not null, numconnections integer, firstconnectdate, "
            "PRIMARY KEY(host))"
        )
        self._commit()
        return True

    def destruct(self):
        self.dbengine._destruct()

    def add_conn_record(self, data):
        cursor = self._getCursor()
        if data["conn_state"] == "SF":
            cursor.execute(
                "insert or ignore into connlog (sourceip,destip,destport,numconnections,firstconnectdate) "
                "values (?,?,?,0,?)",
                (data["id.orig_h"], data["id.resp_h"], data["id.resp_p"], data["ts"])
            )
            cursor.execute(
                "update connlog set numconnections=numconnections+1 where "
                "sourceip=? and destip=? and destport=?",
                (data["id.orig_h"], data["id.resp_h"], data["id.resp_p"])
            )
        else:
            cursor.execute(
                "insert or ignore into connlog (sourceip,destip,destport,numconnections,firstconnectdate) "
                "values (?,?,?,0,?)",
                (data["id.orig_h"], data["id.resp_h"], data["id.resp_p"], data["ts"])
            )
            cursor.execute(
                "update connlog set numconnections=numconnections+1 where "
                "sourceip=? and destip=? and destport=?",
                (data["id.orig_h"], data["id.resp_h"], data["id.resp_p"])
            )

        self._commit()
        return

    def add_smtp_record(self, data):
        cursor = self._getCursor()
        cursor.execute("select count(*) from smtplog where source=? and destination=?",
                       (data["source"], data["destination"]))
        result = int(cursor.fetchone()[0])
        if result == 0:
            cursor.execute(
                "insert into smtplog (source,destination,numconnections,firstconnectdate) "
                "values (?,?,1,?)",
                (data["mailfrom"], data["rcptto"], data["ts"])
            )
        else:
            cursor.execute(
                "update smtplog set numconnections=numconnections+1 where source=? and destination=?",
                (data["mailfrom"], data["rcptto"])
            )
        self._commit()
        return

    def add_http_record(self, data):
        try:
            cursor = self._getCursor()
            cursor.execute(
                "insert or ignore into httplog (host,numconnections,firstconnectdate) "
                "values (?,?,?)",
                (data["host"], 1, data["ts"])
            )
            cursor.execute(
                "update httplog set numconnections=numconnections+1 where "
                "host=?",
                (data["host"], )
            )
            self._commit()
            #cursor.close()
        except:
            logging.error("MYSQLDB: Error processing: " + repr(data))
        return
