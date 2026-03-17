# -*- coding: utf-8 -*-
import io
import os

from six import text_type


class Source:

    """Handle source object"""

    def __init__(self, url_or_file, type_):
        self.source = url_or_file
        self.type = type_

        if self.type == "file":
            self.checkFiles()

    def isUrl(self):
        """URL type"""

        return "url" == self.type

    def isString(self):
        """String type"""

        return "string" == self.type

    def isFile(self, path=None):
        # dirty hack to check where file is opened with codecs module
        # (because it returns 'instance' type when encoding is specified
        if path:
            return (
                isinstance(path, io.IOBase)
                or path.__class__.__name__ == "StreamReaderWriter"
            )
        return "file" in self.type

    def checkFiles(self):
        if isinstance(self.source, list):
            for path in self.source:
                if not os.path.exists(path):
                    raise OSError("No such file: {}".format(path))
        else:
            if not hasattr(self.source, "read") and not os.path.exists(self.source):
                raise OSError("No such file: {}".format(self.source))

    def isFileObj(self):
        return hasattr(self.source, "read")

    def to_s(self):
        if isinstance(self.source, text_type):
            return self.source
        else:
            return text_type(self.source, "utf-8")
