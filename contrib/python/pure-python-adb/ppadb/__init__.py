__version__ = "0.3.0-dev"

class InstallError(Exception):
    def __init__(self, path, error):
        super(InstallError, self).__init__("{} could not be installed - [{}]".format(path, error))


class ClearError(Exception):
    def __init__(self, package, error):
        super(ClearError, self).__init__("Package {} could not be cleared - [{}]".format(package, error))
