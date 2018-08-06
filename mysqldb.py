import pymysql
import logging
import os


class DBConnectStringError(Exception):
    pass


class DBEngine(object):
    def __init__(self, connectstring):
        parts = connectstring.split(";")
        connectvals = {}
        for part in parts:
            try:
                label, value = part.split("=")
            except:
                raise DBConnectStringError
            connectvals[label.lower()] = value
        if "server" not in connectvals.keys():
            connectvals["server"] = "localhost"
        if "database" not in connectvals.keys() or "uid" not in connectvals.keys() or "pwd" not in connectvals.keys():
            raise DBConnectStringError
        self.connectvals = connectvals
        self.connection = None

    def open(self):
        if not self.connection:
            try:
                self.connection = pymysql.connect(
                    host=self.connectvals["server"],
                    user=self.connectvals["uid"],
                    password=self.connectvals["pwd"],
                    db=self.connectvals["database"]
                )
            except:
                return False
        return True

    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def _destruct(self):
        if not self.connection:
            return
        with self.connection.cursor() as cur:
            cur.execute("drop table if exists properties,connlog,smtplog")
        self.connection.commit()
        self.close()

    def engine(self):
        return "mysql"


class LogDB(object):
    def __init__(self, dbengine):
        self.dbengine = dbengine
        self.version = "1.0"

    def _getCursor(self):
        return self.dbengine.connection.cursor()

    def _commit(self):
        self.dbengine.connection.commit()

    def _exists(self, tablename):
        cursor = self._getCursor()
        cursor.execute("select count(table_name) from information_schema.tables where "
                       "table_schema=%s and table_name=%s",
                       (self.dbengine.connectvals["database"], tablename))
        result = int(cursor.fetchone()[0])
        cursor.close()
        return True if result else False

    def _create_properties(self):
        cursor = self._getCursor()
        cursor.execute("create table if not exists properties (label varchar(255) UNIQUE,value varchar(255));")
        cursor.execute("INSERT INTO properties (label,value) VALUES (%s,%s)", ("VERSION", self.version))
        self._commit()
        cursor.close()

    def _checkVersion(self):
        cursor = self._getCursor()
        cursor.execute("select value from properties where label=%s", ("VERSION",))
        result = cursor.fetchone()
        cursor.close()
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
            "create table if not exists connlog (sourceip varchar(15) not null, destip varchar(15) not null, "
            "destport INTEGER(11) not null, "
            "numconnections INTEGER(11), firstconnectdate DOUBLE, PRIMARY KEY(sourceip,destip,destport))"
        )
        cursor.execute(
            "create table if not exists connerr (sourceip varchar(15) not null, destip varchar(15) not null, "
            "destport INTEGER(11) not null, "
            "numconnections INTEGER(11), firstconnectdate DOUBLE, PRIMARY KEY(sourceip,destip,destport))"
        )
        cursor.execute(
            "create table if not exists smtplog (source varchar(255) not null, destination varchar(255) not null, "
            "numconnections integer(11), "
            "firstconnectdate DOUBLE, PRIMARY KEY(source,destination))"
        )
        cursor.execute(
            "create table if not exists httplog (host varchar(255) not null, numconnections integer(11),"
            "firstconnectdate DOUBLE, PRIMARY KEY(host))"
        )
        self._commit()
        cursor.close()
        return True

    def destruct(self):
        self.dbengine._destruct()

    def add_conn_record(self, data):
        try:
            cursor = self._getCursor()
            if data["conn_state"] == "SF":
                cursor.execute(
                    "insert into connlog (sourceip,destip,destport,numconnections,firstconnectdate) values (%s,%s,%s,1,%s)"
                    "on duplicate key update numconnections=numconnections+1",
                    (data["id.orig_h"], data["id.resp_h"], data["id.resp_p"], data["ts"])
                )
            else:
                cursor.execute(
                    "insert into connerr (sourceip,destip,destport,numconnections,firstconnectdate) values (%s,%s,%s,1,%s)"
                    "on duplicate key update numconnections=numconnections+1",
                    (data["id.orig_h"], data["id.resp_h"], data["id.resp_p"], data["ts"])
                )
            self._commit()
            cursor.close()
        except:
            logging.error("MYSQLDB: Error processing: " + repr(data))
        return

    def add_smtp_record(self, data):
        try:
            cursor = self._getCursor()
            cursor.execute(
                "insert into smtplog (source,destination,numconnections,firstconnectdate) "
                "values (%s,%s,1,%s) on duplicate key update numconnections=numconnections+1",
                (data["mailfrom"], data["rcptto"], data["ts"])
            )
            self._commit()
            cursor.close()
        except:
            logging.error("MYSQLDB: Error processing: " + repr(data))
        return

    def add_http_record(self, data):
        try:
            cursor = self._getCursor()
            cursor.execute(
                "insert into httplog (host,numconnections,firstconnectdate) values (%s,%s,%s) "
                "on duplicate key update numconnections=numconnections+1",
                (data["host"], 1, data["ts"])
            )
            self._commit()
            cursor.close()
        except:
            logging.error("MYSQLDB: Error processing: " + repr(data))
        return
