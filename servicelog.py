class ServiceLog:
    def __init__(self):
        self.timing = {}
        self.counter = {}
        self.time = 0
        self.timestamp = None
        self.tags = {}
        self.id = None
    
    def display(self):
        print self.time
        print self.timestamp
        print self.tags
        print self.counter
        print self.timing
