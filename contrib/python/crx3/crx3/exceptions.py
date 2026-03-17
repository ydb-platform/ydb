# -*- coding: utf-8 -*-

class FileFormatNotSupportedError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return 'FileFormatNotSupportedError: ' + self.message
