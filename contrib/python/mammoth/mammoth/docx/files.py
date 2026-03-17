import os
import contextlib
try:
    from urllib2 import urlopen
except ImportError:
    from urllib.request import urlopen
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class Files(object):
    def __init__(self, base, external_file_access):
        self._base = base
        self._external_file_access = external_file_access

    def open(self, uri):
        if not self._external_file_access:
            raise ExternalFileAccessIsDisabledError(
                "could not open external image '{0}', external file access is disabled".format(uri)
            )

        try:
            if _is_absolute(uri):
                return contextlib.closing(urlopen(uri))
            elif self._base is not None:
                return open(os.path.join(self._base, uri), "rb")
            else:
                raise InvalidFileReferenceError("could not find external image '{0}', fileobj has no name".format(uri))
        except IOError as error:
            message = "could not open external image: '{0}' (document directory: '{1}')\n{2}".format(
                uri, self._base, str(error))
            raise InvalidFileReferenceError(message)


def _is_absolute(url):
    return urlparse(url).scheme != ""


class InvalidFileReferenceError(ValueError):
    pass


class ExternalFileAccessIsDisabledError(InvalidFileReferenceError):
    pass
