#!/usr/bin/env python3
import argparse
import configparser
import fnmatch
import gzip
import logging
import logging.config
import os
import socket
import sys
import time
import traceback


class LogProcess(object):
    def __init__(self, dbtype, database):
        try:
            dbe = __import__(dbtype + "db")
        except Exception as e:
            logging.critical("Error loading database module: " + dbtype + ": " + str(e))
            sys.exit(-1)
        self.dbengine = dbe.DBEngine(database)
        if not self.dbengine.open():
            logging.critical("Could not connect to mysql database: " + database)
            sys.exit(-1)
        self.db = dbe.LogDB(self.dbengine)
        self.db.instantiate()
        self.props = {}

    def _process_prop(self, line):
        if line.startswith("#close"):
            return
        line = str(line)[1:]
        if line.startswith("separator"):
            label, value = line.split()
            value = chr(int(value.replace("\\", "0"), 16))
        else:
            label, value = line.split(self.props["separator"], 1)
        if label not in ["fields", "types"]:
            self.props[label] = value
        else:
            self.props[label] = value.split(self.props["separator"])
        logging.debug("label " + label + "=" + str(self.props[label]))

    def _get_line_data(self, line):
        elements = str(line).split(self.props["separator"])
        if len(elements) != len(self.props["fields"]):
            logging.error("ERROR processing line: " + line)
            logging.error(
                "Number of elements is: " + str(len(elements)) + ", should be: " + str(len(self.props["fields"])))
            return
        data = {}
        i = 0
        for item in self.props["fields"]:
            data[item] = elements[i]
            i += 1
        return data

    def _parse_line(self, line):
        raise NotImplemented

    def start(self, filepath):
        logging.info("parsing " + filepath)
        benchmarktime = time.time()
        numrecords = 0
        with gzip.open(filepath, "rb") as f:
            try:
                for line in f:
                    numrecords += 1
                    line = line.decode().strip()
                    if line == "":
                        return
                    if not line.startswith("#"):
                        self._parse_line(line)
                    else:
                        self._process_prop(line)

                self.db.close()

            except EOFError:
                logging.error(filepath + " has a compression error.  Skipping to next file.")
            except:
                logging.error(traceback.format_exc())
                sys.exit(0)
        benchmarktime = time.time() - benchmarktime
        if numrecords == 0:
            return 0
        return benchmarktime, numrecords, numrecords / benchmarktime


class ConnLog(LogProcess):
    def __init__(self, dbtype, database, whitelist_src_ips, whitelist_dest_ips, whitelist_dest_ports):
        super().__init__(dbtype, database)
        self.whitelist_dest_ips = whitelist_dest_ips
        self.whitelist_dest_ports = whitelist_dest_ports
        self.whitelist_src_ips = whitelist_src_ips

    def _parse_line(self, line):
        data = self._get_line_data(line)
        if data["id.resp_h"] in self.whitelist_dest_ips or data["id.resp_p"] in self.whitelist_dest_ports or data[
            "id.orig_h"] in self.whitelist_src_ips:
            return
        try:
            socket.inet_aton(data["id.orig_h"])
        except:
            logging.debug("Invalid sourceip IPv4 address " + data["id.orig_h"] + ", skipping...")
            return
        try:
            socket.inet_aton(data["id.resp_h"])
        except:
            logging.debug("Invalid destip IPv4 address " + data["id.resp_h"] + ", skipping...")
            return
        self.db.add_conn_record(data)


class SMTPLog(LogProcess):
    def __init__(self, dbtype, database, whitelist_source, whitelist_destination):
        super().__init__(dbtype, database)
        self.whitelist_source = whitelist_source
        self.whitelist_destination = whitelist_destination

    def _parse_line(self, line):
        data = self._get_line_data(line)

        mailfrom = data["mailfrom"].strip().lower()
        if mailfrom.startswith('<'):
            mailfrom = mailfrom[1:]
        if mailfrom.endswith('>'):
            mailfrom = mailfrom[:-1]

        if mailfrom == '' or mailfrom == '-':
            return

        data["mailfrom"] = mailfrom
        
        # rcptto can be a list of email addresses
        for rcptto in data["rcptto"].split(','):
            rcptto = rcptto.strip().lower()
            if rcptto.startswith('<'):
                rcptto = rcptto[1:]
            if rcptto.endswith('>'):
                rcptto = rcptto[:-1]

            if data["mailfrom"] in self.whitelist_source or rcptto in self.whitelist_destination:
                continue

            data["rcptto"] = rcptto
            if data["rcptto"] == '' or data["rcptto"] == '-':
                continue

            self.db.add_smtp_record(data)

class HTTPLog(LogProcess):
    def __init__(self, dbtype, database):
        super().__init__(dbtype, database)

    def _parse_line(self, line):
        data = self._get_line_data(line)

        # skip these blank entries
        if data['host'] == '-' or data['host'] == '':
            return

        # we want to track each component of the FQDN by itself
        # example www.facebook.com
        # com [1]
        # facebook.com [1]
        # www.facebook.com [1]
        fqdn_split = data['host'].split('.')
        fqdn_split.reverse()
        current_fqdn = []
        for domain_part in fqdn_split:
            domain_part = domain_part.lower()
            current_fqdn.insert(0, domain_part)
            data['host'] = '.'.join(current_fqdn)
            self.db.add_http_record(data)

parser = argparse.ArgumentParser(description="Process a Bro log and place it in a database.")
parser.add_argument("-L", "--logging-config-path", action="store", default="brocess_logging.ini", 
                    dest="logging_config_path", help="Path to logging configuration file.")
parser.add_argument("-t", "--dbtype", action="store", dest="dbtype",
                    help="The type of database to use: sqlite, mysql, or mysqli")
parser.add_argument("-d", "--database", action="store", dest="database",
                    help="The database connection string to use.  This is simply a filename or :memory: for"
                         "sqlite databases and \"host,database,username,password\" for mysql")
parser.add_argument("-e", "--eventlog", action="store", dest="eventlog",
                    help="Path to debug message file.")
parser.add_argument("-c", "--connlog", action="store", dest="connlog",
                    help="Specify the pattern of the connection log to process")
parser.add_argument("-m", "--smtplog", action="store", dest="smtplog",
                    help="Specify the pattern of the smtp log to process")
parser.add_argument("-w", "--httplog", action="store", dest="httplog",
                    help="Specify the pattern of the http log to process")
parser.add_argument("-i", "--inifile", action="store", dest="inifile",
                    help="Specify the path to the ini file")
parser.add_argument("-r", "--remove", action="store_true", dest="remove",
                    help="Remove the file from filesystem when finished.")
parser.add_argument("filename", help="The filename to process.  If the filename does not match the patterns "
                                     "provided (in --connlog or --smtplog), the program will exit with an "
                                     "error")


def reconcileINI(args):
    whitelists = {"conn_dest_whitelist_ips": {}, "conn_dest_whitelist_ports": {}, "smtp_whitelist_source": {},
                  "smtp_whitelist_destination": {}, "conn_src_whitelist_ips": {}}

    homedir = os.path.split(sys.argv[0])[0]
    if not args.inifile:
        args.inifile = os.path.join(homedir, "brocess.ini")
    config = configparser.ConfigParser()
    config.read(args.inifile)
    args.logformatline = config.get("main", "logformat", fallback="[%(asctime)s] [%(filename)s:%(lineno)d] "
                                    "[%(threadName)s] [%(process)d] [%(levelname)s] - %(message)s")

    if not config:
        return args, whitelists
    if not args.dbtype:
        args.dbtype = config.get("main", "dbtype", fallback=None)
    if not args.database:
        args.database = config.get(args.dbtype, "database", fallback=None)
    if not args.eventlog:
        args.eventlog = config.get("main", "eventlog", fallback=None)
    if not args.connlog:
        args.connlog = config.get("watchlogs", "connlog", fallback=None)
    if not args.smtplog:
        args.smtplog = config.get("watchlogs", "smtplog", fallback=None)
    if not args.httplog:
        args.httplog = config.get("watchlogs", "httplog", fallback=None)

    for whitelist_type in whitelists:
        if whitelist_type in config.keys():
            for item in config[whitelist_type]:
                whitelists[whitelist_type][config.get(whitelist_type, item)] = item

    return args, whitelists


def main():
    args = parser.parse_args()
    args, whitelists = reconcileINI(args)

    if not os.path.isdir('logs'):
        try:
            os.mkdir('logs')
        except Exception as e:
            sys.stderr.write("unable to create logs directory: {}\n".format(e))
            sys.exit(1)

    try:
        logging.config.fileConfig(args.logging_config_path)
    except Exception as e:
        sys.stderr.write("unable to parse logging configuration: {}\n".format(e))
        sys.exit(1)

    #if args.eventlog:
        #try:
            #f = open(args.eventlog, "a")
            #f.close()
        #except:
            #print("Unable to open event log " + args.eventlog + " for writing.")
            #sys.exit(-1)
        #logging.basicConfig(filename=args.eventlog, format=args.logformatline, level=logging.DEBUG)
    #else:
        #logging.basicConfig(format=args.logformatline, level=logging.DEBUG)

    if not os.path.isfile(args.filename):
        logging.critical("Cannot find: " + args.filename)
        sys.exit(-1)
    if not args.connlog and not args.smtplog and not args.httplog:
        logging.critical("No watch filters (connlog or smtplog) are set.")
        sys.exit(-1)
    filename = os.path.split(args.filename)[1]
    logprocess = None
    if args.connlog:
        if fnmatch.fnmatch(filename, args.connlog):
            logprocess = ConnLog(args.dbtype, args.database, whitelist_src_ips=whitelists["conn_src_whitelist_ips"],
                                 whitelist_dest_ips=whitelists["conn_dest_whitelist_ips"],
                                 whitelist_dest_ports=whitelists["conn_dest_whitelist_ports"])
    if args.smtplog:
        if fnmatch.fnmatch(filename, args.smtplog):
            logprocess = SMTPLog(args.dbtype, args.database, whitelist_source=whitelists["smtp_whitelist_source"],
                                 whitelist_destination=whitelists["smtp_whitelist_destination"])
    if args.httplog:
        if fnmatch.fnmatch(filename, args.httplog):
            logprocess = HTTPLog(args.dbtype, args.database)
    if not logprocess:
        logging.critical("Unable to find a pattern match for " + filename)
        sys.exit(-1)
    runtime, numrecords, benchmark = logprocess.start(args.filename)
    logging.info("Finished processing " + repr(numrecords) + " records in " + repr(runtime) + " seconds at " + repr(
        benchmark) + " records per second.")
    if args.remove:
        try:
            os.remove(args.filename)
        except:
            logging.critical("Unable to remove file: " + args.filename)


if __name__ == "__main__":
    main()
    
