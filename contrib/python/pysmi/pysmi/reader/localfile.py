#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#
import os
import sys
import time

from pysmi import debug
from pysmi import error
from pysmi.compat import decode
from pysmi.mibinfo import MibInfo
from pysmi.reader.base import AbstractReader


class FileReader(AbstractReader):
    """Fetch ASN.1 MIB text by name from local file.

    *FileReader* class instance tries to locate ASN.1 MIB files
    by name, fetch and return their contents to caller.
    """

    useIndexFile = True  # optional .index file mapping MIB to file name
    indexFile = ".index"

    def __init__(self, path, recursive=True, ignoreErrors=True):
        """Create an instance of *FileReader* serving a directory.

        Args:
            path (str): directory to search MIB files

        Keyword Args:
            recursive (bool): whether to include subdirectories
            ignoreErrors (bool): ignore filesystem access errors
        """
        self._path = os.path.normpath(path)
        self._recursive = recursive
        self._ignoreErrors = ignoreErrors
        self._indexLoaded = False
        self._mibIndex = None

    def __str__(self):
        """Return string representation of the instance."""
        return f'{self.__class__.__name__}{{"{self._path}"}}'

    def get_subdirs(self, path, recursive=True, ignoreErrors=True):
        if not recursive:
            return [path]

        dirs = [path]

        try:
            subdirs = os.listdir(path)

        except OSError:
            if ignoreErrors:
                return dirs

            else:
                raise error.PySmiError(
                    f"directory {path} access error: {sys.exc_info()[1]}"
                )

        for d in subdirs:
            d = os.path.join(decode(path), decode(d))
            if os.path.isdir(d):
                dirs.extend(self.get_subdirs(d, recursive))

        return dirs

    @staticmethod
    def load_index(indexFile):
        mibIndex = {}
        if os.path.exists(indexFile):
            try:
                f = open(indexFile)
                mibIndex = dict([x.split()[:2] for x in f.readlines()])
                f.close()
                debug.logger & debug.FLAG_READER and debug.logger(
                    f"loaded MIB index map from {indexFile} file, {len(mibIndex)} entries"
                )

            except OSError:
                pass

        return mibIndex

    def get_mib_variants(self, mibname, **options):
        if self.useIndexFile:
            if not self._indexLoaded:
                self._mibIndex = self.load_index(
                    os.path.join(self._path, self.indexFile)
                )
                self._indexLoaded = True

            if mibname in self._mibIndex:
                debug.logger & debug.FLAG_READER and debug.logger(
                    f"found {mibname} in MIB index: {self._mibIndex[mibname]}"
                )
                return [(mibname, self._mibIndex[mibname])]

        return super().get_mib_variants(mibname, **options)

    def get_data(self, mibname, **options):
        debug.logger & debug.FLAG_READER and debug.logger(
            f"looking for MIB {mibname}{' recursively' if self._recursive else ''}"
        )

        for path in self.get_subdirs(self._path, self._recursive, self._ignoreErrors):
            for mibalias, mibfile in self.get_mib_variants(mibname, **options):
                f = os.path.join(decode(path), decode(mibfile))

                debug.logger & debug.FLAG_READER and debug.logger(f"trying MIB {f}")

                if os.path.exists(f) and os.path.isfile(f):
                    try:
                        mtime = os.stat(f)[8]

                        debug.logger & debug.FLAG_READER and debug.logger(
                            f"source MIB {f} mtime is {time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(mtime))}, fetching data..."
                        )

                        fp = open(f, mode="rb")
                        mibData = fp.read(self.maxMibSize)
                        fp.close()

                        if len(mibData) == self.maxMibSize:
                            raise OSError(f"MIB {f} too large")

                        return MibInfo(
                            path=f"file://{f}",
                            file=mibfile,
                            name=mibalias,
                            mtime=mtime,
                        ), decode(mibData)

                    except OSError:
                        debug.logger & debug.FLAG_READER and debug.logger(
                            f"source file {f} open failure: {sys.exc_info()[1]}"
                        )

                        if not self._ignoreErrors:
                            raise error.PySmiError(
                                f"file {f} access error: {sys.exc_info()[1]}"
                            )

                    raise error.PySmiReaderFileNotModifiedError(
                        f"source MIB {f} is older than needed", reader=self
                    )

        raise error.PySmiReaderFileNotFoundError(
            f"source MIB {mibname} not found", reader=self
        )
