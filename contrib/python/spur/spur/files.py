import os

class FileOperations(object):
    def __init__(self, shell):
        self._shell = shell
        
    def copy_file(self, source, destination=None, dir=None):
        if destination is None and dir is None:
            raise TypeError("Destination required for copy")
            
        if destination is not None:
            self._shell.run(["cp", "-T", source, destination])
        elif dir is not None:
            self._shell.run(["cp", source, "-t", dir])
            
    def write_file(self, path, contents):
        self._shell.run(["mkdir", "-p", os.path.dirname(path)])
        file = self._shell.open(path, "w")
        try:
            file.write(contents)
        finally:
            file.close()
        
