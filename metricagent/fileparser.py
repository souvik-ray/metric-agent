from servicelog import ServiceLog

class  FileParser:
    def parse(self, line):
        pass

class ServiceLogParser(FileParser):
    
    def parse(self, service_log_file):
        service_log = ServiceLog()
        while (1):
            line=service_log_file.readline()   
            if not line:
                return None
            line = line.strip()
            # All the changes have been accumulated
            if line == "EOE":
                return service_log

            if line.startswith("Timing"):
                attributes = line.replace("Timing=","").split(",")
                for attribute in attributes:
                    attribute=attribute.strip()
                    if (attribute != ""):
                        key, value = self.get_key_value(attribute)
                        service_log.timing[key] = value
            if line.startswith("Counter"):
                #key, value = self.get_key_value(line)
                #self.timing[key] = value
                attributes = line.replace("Counters=","").split(",")
                for attribute in attributes:
                    attribute=attribute.strip()
                    if (attribute != ""):
                        key, value = self.get_key_value(attribute)
                        service_log.counter[key] = value
                pass
            if line.startswith("Time"):
                service_log.time = self.get_value(line)
            if line.startswith("StartTime"):
                service_log.timestamp = self.get_value(line)
            if line.startswith("ProgramName"):
                service_log.tags["service"]=self.get_value(line)
            if line.startswith("OperationName"):
                service_log.tags["method"] = self.get_value(line)
            if line.startswith("HostName"):
                service_log.tags["host"] = self.get_value(line)
            if line.startswith("MarketplaceId"):
                service_log.tags["marketplace"] = self.get_value(line)
                                 
            # Do not include tags
            if line.startswith("EndTime"):
                continue
            if line.startswith("ResponseCode"):
                continue
            if line.startswith("PathInfo"):
                continue
            if line.startswith("PID"):
                continue
            if line.startswith("TenantId"):
                continue
            if line.startswith("RequestID"):
                continue
            
    def get_value(self, line):
        pair = line.split("=")
        return pair[1]
    def get_key_value(self, line):
        pair = line.split("=")
        return pair[0], pair[1]     
