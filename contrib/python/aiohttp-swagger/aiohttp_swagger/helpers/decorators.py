class swagger_path(object):
    def __init__(self, swagger_file):
        self.swagger_file = swagger_file
    
    def __call__(self, f):
        f.swagger_file = self.swagger_file
        return f
