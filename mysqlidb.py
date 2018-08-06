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
        if self._count:
            self.dbengine.connection.commit()
            self._count = 0

        if self.connection:
            self.connection.close()
            self.connection = None

    #def _destruct(self):
        #if not self.connection:
            #return
        #with self.connection.cursor() as cur:
            #cur.execute("drop table if exists properties,connlog,smtplog,conndb")
        #self.connection.commit()
        #self.close()

    def engine(self):
        return "mysqli"


class LogDB(object):
    def __init__(self, dbengine):
        self.dbengine = dbengine
        self.version = "1.0"
        self._cursor = None
        self._cursor_count = 0
        self._cursor_count_limit = 1000

    def _getCursor(self):
        if self._cursor:
            return self._cursor
        
        self._cursor = self.dbengine.connection.cursor()
        return self._cursor

    def _commit(self):
        self._cursor_count += 1
        if self._cursor_count >= self._cursor_count_limit:
            self._cursor_count = 0
            self.dbengine.connection.commit()

    def _exists(self, tablename):
        return True

    def _create_properties(self):
        pass

    def _checkVersion(self):
        return True

    def instantiate(self):
        pass

    #def destruct(self):
        #self.dbengine._destruct()

    def add_conn_record(self, data):
        try:
            cursor = self._getCursor()
            if data["conn_state"] == "SF":
                cursor.execute(
                    "insert into connlog (sourceip,destip,destport,numconnections,firstconnectdate) "
                    "values (inet_aton(%s),inet_aton(%s),%s,1,%s)"
                    "on duplicate key update numconnections=numconnections+1",
                    (data["id.orig_h"], data["id.resp_h"], data["id.resp_p"], data["ts"])
                )
            else:
                cursor.execute(
                    "insert into connerr (sourceip,destip,destport,numconnections,firstconnectdate) "
                    "values (inet_aton(%s),inet_aton(%s),%s,1,%s)"
                    "on duplicate key update numconnections=numconnections+1",
                    (data["id.orig_h"], data["id.resp_h"], data["id.resp_p"], data["ts"])
                )
            self._commit()
            #cursor.close()
        except Exception as e:
            logging.error("MYSQLIDB: Error processing {}: {}".format(repr(data), e))
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
            #cursor.close()
        except Exception as e:
            logging.error("MYSQLIDB: Error processing {}: {}".format(repr(data), e))
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
            #cursor.close()
        except Exception as e:
            logging.error("MYSQLIDB: Error processing {}: {}".format(repr(data), e))
        return
