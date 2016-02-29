from jiostatsd import StatsdClient

class MetricsHandler:
    # TODO Add ABS Meta
    def send(self, service_log):
        pass

class MetricType:
    # TODO Add ABS Meta
    GAUGE="g"
    COUNTER="c"
    TIMING="t"
    
'''
Handler to send metrics through a statsd server
'''
class StatsdMetricsHandler(MetricsHandler):
    
    def __init__(self, host='localhost', port=8125):
        self.__statsd_client = StatsdClient(host, port)
        
    def send(self, service_log):
        # At this point of time we are not sure whether counter or gauge should be used. 
        # Counter potentially will show incorrect data if the same dataset is fed to database twice
        # Gauges on the other hand will allow idempotency. 
        # We need to make sure both Gauges and Counter flush is set to true
        
        # Send Latency
        # This needs to be refactored in a proper way. It certainly looks hacky right now!
        metric_key = self.prepare_metric_key("time", service_log)
        self.send_metrics( metric_key, MetricType.TIMING, service_log.time.replace(" ms", ""))
        for key, value in service_log.timing.iteritems():
            metric_key = self.prepare_metric_key(key, service_log)
            self.send_metrics(metric_key, MetricType.TIMING, value.replace(" ms", ""))
        for key, value in service_log.counter.iteritems():
            metric_key = self.prepare_metric_key(key, service_log)
            self.send_metrics(metric_key, MetricType.GAUGE, value)
            #self.send_metrics(metric_key, MetricType.COUNTER, value)
            
    def send_metrics(self, key, metric_type, value):
        print "Sending the following metrics", key, metric_type, value
        if (MetricType.COUNTER == metric_type):
            self.__statsd_client.increment(key, value)
        if (MetricType.GAUGE == metric_type):
            self.__statsd_client.gauge(key, value)
        if (MetricType.TIMING == metric_type):
            self.__statsd_client.timing(key, value)
        
    def prepare_metric_key(self, series, service_log):
        print "series", series
        return series+self.prepare_tag_values(service_log)+self.prepare_timestamp_tag(service_log)
    def prepare_timestamp_tag(self, service_log):
        val = service_log.timestamp.split(".")
        return ","+"timestamp="+val[0]
    def prepare_tag_values(self, service_log):
        tag_text = ""
        for key, value in service_log.tags.iteritems():
            tag_text += ","+key+"="+value
        return tag_text