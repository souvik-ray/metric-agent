'''
Main class to start the metric agent daemon

'''
import sys
import os
from optparse import OptionParser
from daemon import MetricAgentDaemon
from metricshandler import StatsdMetricsHandler
from fileparser import ServiceLogParser


def main(argv=None):
        
        parser = OptionParser()
        parser.add_option("-s", "--statsdserverendpoint", dest="statsd_server_endpoint", help="Ip address of the server")
        parser.add_option("-p", "--service_log_path", dest="service_log_path", help="Path of the Service Log")
        parser.add_option("-m", "--metric_agent_log_directory", dest="metric_agent_log_directory", help="Log of the Metric Agent[default: %default]", metavar="FILE")
        parser.set_defaults(metric_agent_log_directory="/var/log/metric-agent")
        (opts, args) = parser.parse_args(argv)
        host = opts.statsd_server_endpoint
        service_log_path = opts.service_log_path
        metric_agent_log_directory = opts.metric_agent_log_directory
        
        metric_agent_path = str(metric_agent_log_directory) + "/metric-agent-log" + service_log_path.replace('/', '-')
        while (1):
            if os.path.exists(metric_agent_log_directory):
                break
            else:
                print "Metric agent directory do not exist. Creating"
                try:
                    os.mkdir(metric_agent_log_directory)
                except OSError:
                    print "Cant' create folder. Try creating the following folder manually -", metric_agent_log_directory
        
                
        file_parser = ServiceLogParser()
        metrics_handler = StatsdMetricsHandler(host)
        metric_agent_daemon = MetricAgentDaemon(file_parser, metrics_handler, service_log_path, metric_agent_path)
        metric_agent_daemon.accumulate_metrics()
    
    
if __name__ == "__main__":
    sys.exit(main())
