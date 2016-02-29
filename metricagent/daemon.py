import os.path
import time
import logging
from logging.handlers import TimedRotatingFileHandler

SLEEP_DURATION_IN_SECONDS = 10
METRIC_AGENT_LOG = "metric-agent-log"
'''
This class reads service logs usinga file parser and sends it to a metrics aggregator using a metrics aggregator.
'''
class MetricAgentDaemon:
    def __init__(self, file_parser, metricsHandler, service_log_path, metric_agent_path):
        self.__service_log_path = service_log_path
        self.__metric_agent_path = metric_agent_path
        self.__original_inode_number = None 
        self.__service_log_file = None
        self.__metricsHandler = metricsHandler
        self.__file_parser = file_parser
        # Set Logging of Service Log Handler
        self.__metric_agent_logger = logging.getLogger(METRIC_AGENT_LOG)
        handler = TimedRotatingFileHandler(self.__metric_agent_path, 'h', 1, 100)
        self.__metric_agent_logger.addHandler(handler)
        self.__metric_agent_logger.setLevel(logging.DEBUG)
        
    def get_inode_number(self, filename):
        return os.stat(filename).st_ino

    def refresh_service_log_file(self):
        # if the service file is not present, wait for this file to be present to parse metrics
        while (not os.path.isfile(self.__service_log_path)):
            print self.__service_log_path + "does not exist. Waiting for " +SLEEP_DURATION_IN_SECONDS+"seconds" 
            time.sleep(SLEEP_DURATION_IN_SECONDS)

        # fetch the inode number of the service file and check whether this matches with the inode number
        # present in the metrics agent file. If that is the same we have been reading the same file.
        # But if the inode number is changed then the service log file is rotated at the end of the hour
        # and we need read the file is again. 
        new_inode_number = self.get_inode_number(self.__service_log_path)
        if (new_inode_number != self.__original_inode_number):
            if self.__service_log_file is not None:
                self.__service_log_file.close()
            
            self.__service_log_file = open(self.__service_log_path, 'r')
            self.__original_inode_number = new_inode_number
            print "updating_inode_number"+str(new_inode_number)+":"+str(self.__original_inode_number)+":"
            # Think: Do we need to do seek here. Dont think so
        else:
            return self.__service_log_file

    def get_offset(self):
        offset = 0
        if not os.path.isfile(self.__metric_agent_path):
            # TODO: If this does not exist see if the latest file 
            print "No metric agent file present in location "+ self.__metric_agent_path
            return offset
        metric_agent_file = open(self.__metric_agent_path, 'r')
        # read the metrics agent log file and see if there is any offset present. Then we need to read from the offset.
        try:
            while 1:
                line = metric_agent_file.readline()
                if not line:
                    break
                arr = line.strip().split(',')
                inode = arr[0]
                # Since inode is string and original_inode_number is numeric we need to do this transformation
                if inode != str(self.__original_inode_number):
                    continue
                offset = int(arr[1])
                print "Current inode number ",str(self.__original_inode_number),"matched with the current service log file. Hence we will be honoring the offset:",offset
                
        except Exception as e:
            print e
            raise e
        finally:
            metric_agent_file.close()
        return offset
         
    def accumulate_metrics(self):
        try:
            self.refresh_service_log_file()
            # For now do the offset checking only at the start.
            # set the new offset if an offset is found.
            offset = self.get_offset()
            if (offset > 0):
                self.__service_log_file.seek(offset)
            while(1):
                line = self.__service_log_file.readline()
                line = line.strip()
                
                if (line is None or line == ""):
                    print "Nothing to read sleeping for ",(SLEEP_DURATION_IN_SECONDS),"seconds"
                    time.sleep(SLEEP_DURATION_IN_SECONDS)
                    self.refresh_service_log_file()
                else:
                    if not line.startswith("======="):
                        raise Exception("Invalid format file started:"+line)
                    service_log = self.__file_parser.parse(self.__service_log_file)
                    print "reporting service log tags", service_log.display()
                    self.__metricsHandler.send(service_log)
                    self.report_to_metric_agent_log(service_log)
                    # send this to jiostatsd
                
        finally:
            #if not self.__service_log_file.closed:
            #    self.__service_log_file.close()
            print "finally block"
            
    def report_to_metric_agent_log(self, service_log):
        self.__metric_agent_logger.info(str(self.__original_inode_number)+","+str(self.__service_log_file.tell())+","+str(service_log.timestamp))
        pass
    
        




        



        
        




