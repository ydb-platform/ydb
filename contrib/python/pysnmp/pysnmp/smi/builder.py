#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
import marshal
import os
import struct
import sys
import time
import traceback
from typing import Any
import warnings
from errno import ENOENT
from importlib.machinery import BYTECODE_SUFFIXES, SOURCE_SUFFIXES
from importlib.util import MAGIC_NUMBER as PY_MAGIC_NUMBER

from pysnmp import debug, version as pysnmp_version
from pysnmp.smi import error

if __debug__:
    import runpy


PY_SUFFIXES = SOURCE_SUFFIXES + BYTECODE_SUFFIXES

classTypes = (type,)  # noqa: N816


class __AbstractMibSource:
    def __init__(self, srcName):
        self._srcName = srcName
        self.__inited = None
        debug.logger & debug.FLAG_BLD and debug.logger("trying %s" % self)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._srcName!r})"

    def _unique_names(self, files):
        u = set()

        for f in files:
            if f.startswith("__init__."):
                continue

            u.update(f[: -len(sfx)] for sfx in PY_SUFFIXES if f.endswith(sfx))

        return tuple(u)

    # MibSource API follows

    def full_path(self, f="", sfx=""):
        return self._srcName + (f and (os.sep + f + sfx) or "")

    def init(self):
        if self.__inited is None:
            self.__inited = self._init()
            if self.__inited is self:
                self.__inited = True
        if self.__inited is True:
            return self

        else:
            return self.__inited

    def listdir(self):
        return self._listdir()

    def read(self, f):
        pycTime = pyTime = -1

        for pycSfx in BYTECODE_SUFFIXES:
            try:
                pycData, pycPath = self._get_data(f + pycSfx, "rb")

            except OSError:
                why = sys.exc_info()[1]
                if ENOENT == -1 or why.errno == ENOENT:
                    debug.logger & debug.FLAG_BLD and debug.logger(
                        f"file {f + pycSfx} access error: {why}"
                    )

                else:
                    raise error.MibLoadError(
                        f"MIB file {f + pycSfx} access error: {why}"
                    )

            else:
                if PY_MAGIC_NUMBER == pycData[:4]:
                    pycData = pycData[4:]
                    pycTime = struct.unpack("<L", pycData[:4])[0]
                    pycData = pycData[4:]
                    debug.logger & debug.FLAG_BLD and debug.logger(
                        "file %s mtime %d" % (pycPath, pycTime)
                    )
                    break

                else:
                    debug.logger & debug.FLAG_BLD and debug.logger(
                        "bad magic in %s" % pycPath
                    )

        for pySfx in SOURCE_SUFFIXES:
            try:
                pyTime = self._get_timestamp(f + pySfx)

            except OSError:
                why = sys.exc_info()[1]
                if ENOENT == -1 or why.errno == ENOENT:
                    debug.logger & debug.FLAG_BLD and debug.logger(
                        f"file {f + pySfx} access error: {why}"
                    )

                else:
                    raise error.MibLoadError(
                        f"MIB file {f + pySfx} access error: {why}"
                    )

            else:
                debug.logger & debug.FLAG_BLD and debug.logger(
                    "file %s mtime %d" % (f + pySfx, pyTime)
                )
                break

        if pycTime != -1 and pycTime >= pyTime:
            return marshal.loads(pycData), pycSfx

        if pyTime != -1:
            modData, pyPath = self._get_data(f + pySfx, "r")
            return compile(modData, pyPath, "exec"), pyPath

        raise OSError(ENOENT, "No suitable module found", f)

    # Interfaces for subclasses
    def _init(self):
        raise NotImplementedError()

    def _listdir(self):
        raise NotImplementedError()

    def _get_timestamp(self, f):
        raise NotImplementedError()

    def _get_data(self, f, mode):
        NotImplementedError()


class ZipMibSource(__AbstractMibSource):
    """Zip MIB source."""

    def _init(self):
        try:
            p = __import__(self._srcName, globals(), locals(), ["__init__"])
            if hasattr(p, "__loader__") and hasattr(p.__loader__, "_files"):
                self.__loader = p.__loader__
                self._srcName = self._srcName.replace(".", os.sep)
                return self
            elif hasattr(p, "__file__"):
                # Dir relative to PYTHONPATH
                return DirMibSource(os.path.split(p.__file__)[0]).init()
            else:
                raise error.MibLoadError(f"{p} access error")

        except ImportError:
            # Dir relative to CWD
            return DirMibSource(self._srcName).init()

    @staticmethod
    def _parse_dos_ime(dos_date, dos_time):
        t = (
            ((dos_date >> 9) & 0x7F) + 1980,  # year
            ((dos_date >> 5) & 0x0F),  # month
            dos_date & 0x1F,  # mday
            (dos_time >> 11) & 0x1F,  # hour
            (dos_time >> 5) & 0x3F,  # min
            (dos_time & 0x1F) * 2,  # sec
            -1,  # wday
            -1,  # yday
            -1,
        )  # dst
        return time.mktime(t)

    def _listdir(self):
        value = []
        # noinspection PyProtectedMember
        for f in self.__loader._files.keys():
            d, f = os.path.split(f)
            if d == self._srcName:
                value.append(f)
        return tuple(self._unique_names(value))

    def _get_timestamp(self, f):
        p = os.path.join(self._srcName, f)
        # noinspection PyProtectedMember
        if p in self.__loader._files:
            # noinspection PyProtectedMember
            return self._parse_dos_ime(
                self.__loader._files[p][6], self.__loader._files[p][5]
            )
        else:
            raise OSError(ENOENT, "No such file in ZIP archive", p)

    def _get_data(self, f, mode=None):
        p = os.path.join(self._srcName, f)
        try:
            return self.__loader.get_data(p), p

        except Exception:  # ZIP code seems to return all kinds of errors
            why = sys.exc_info()
            raise OSError(ENOENT, f"File or ZIP archive {p} access error: {why[1]}")


class DirMibSource(__AbstractMibSource):
    """Directory MIB source."""

    def _init(self):
        self._srcName = os.path.normpath(self._srcName)
        return self

    def _listdir(self):
        try:
            return self._unique_names(os.listdir(self._srcName))
        except OSError:
            why = sys.exc_info()
            debug.logger & debug.FLAG_BLD and debug.logger(
                f"listdir() failed for {self._srcName}: {why[1]}"
            )
            return ()

    def _get_timestamp(self, f):
        p = os.path.join(self._srcName, f)
        try:
            return os.stat(p)[8]
        except OSError:
            raise OSError(ENOENT, "No such file: %s" % sys.exc_info()[1], p)

    def _get_data(self, f, mode):
        p = os.path.join(self._srcName, "*")
        try:
            if f in os.listdir(self._srcName):  # make FS case-sensitive
                p = os.path.join(self._srcName, f)
                fp = open(p, mode)
                data = fp.read()
                fp.close()
                root, extension = os.path.splitext(p)
                return data, extension

        except OSError:
            why = sys.exc_info()
            msg = f"File or directory {p} access error: {why[1]}"

        else:
            msg = "No such file or directory: %s" % p

        raise OSError(ENOENT, msg)


class MibBuilder:
    """MIB builder."""

    DEFAULT_CORE_MIBS = os.pathsep.join(
        ("pysnmp.smi.mibs.instances", "pysnmp.smi.mibs")
    )
    DEFAULT_MISC_MIBS = "pysnmp_mibs"

    module_id = "PYSNMP_MODULE_ID"

    loadTexts = False  # noqa: N815

    # MIB modules can use this to select the features they can use
    version = pysnmp_version

    __mib_sources: list["ZipMibSource | DirMibSource"]

    def __init__(self):
        """Create a MIB builder instance."""
        self.lastBuildId = self._autoName = 0
        sources = []
        for ev in "PYSNMP_MIB_PKGS", "PYSNMP_MIB_DIRS", "PYSNMP_MIB_DIR":
            if ev in os.environ:
                for m in os.environ[ev].split(os.pathsep):
                    sources.append(ZipMibSource(m))
        if not sources and self.DEFAULT_MISC_MIBS:
            for m in self.DEFAULT_MISC_MIBS.split(os.pathsep):
                sources.append(ZipMibSource(m))
        for m in self.DEFAULT_CORE_MIBS.split(os.pathsep):
            sources.insert(0, ZipMibSource(m))
        self.mibSymbols = {}
        self.__mib_sources = []
        self.__modSeen = {}
        self.__modPathsSeen = set()
        self.__mibCompiler = None
        self.set_mib_sources(*sources)

    # MIB compiler management

    def get_mib_compiler(self):
        """Return MIB compiler."""
        return self.__mibCompiler

    def set_mib_compiler(self, mibCompiler, destDir):
        """Set MIB compiler."""
        self.add_mib_sources(DirMibSource(destDir))
        self.__mibCompiler = mibCompiler
        return self

    # MIB modules management

    def add_mib_sources(self, *mibSources: "ZipMibSource | DirMibSource"):
        """Add MIB sources to the search path."""
        self.__mib_sources.extend([s.init() for s in mibSources])
        debug.logger & debug.FLAG_BLD and debug.logger(
            f"addMibSources: new MIB sources {self.__mib_sources}"
        )

    def set_mib_sources(self, *mibSources: "ZipMibSource | DirMibSource"):
        """Set MIB sources to the search path."""
        self.__mib_sources = [s.init() for s in mibSources]
        debug.logger & debug.FLAG_BLD and debug.logger(
            f"setMibSources: new MIB sources {self.__mib_sources}"
        )

    def get_mib_sources(self) -> tuple["ZipMibSource | DirMibSource", ...]:
        """Get MIB sources from the search path."""
        return tuple(self.__mib_sources)

    def load_module(self, modName, **userCtx):
        """Load and execute MIB modules as Python code."""
        for mibSource in self.__mib_sources:
            debug.logger & debug.FLAG_BLD and debug.logger(
                f"loadModule: trying {modName} at {mibSource}"
            )
            try:
                codeObj, sfx = mibSource.read(modName)

            except OSError:
                debug.logger & debug.FLAG_BLD and debug.logger(
                    f"loadModule: read {modName} from {mibSource} failed: {sys.exc_info()[1]}"
                )
                continue

            modPath = mibSource.full_path(modName, sfx)

            if modPath in self.__modPathsSeen:
                debug.logger & debug.FLAG_BLD and debug.logger(
                    "loadModule: seen %s" % modPath
                )
                break

            else:
                self.__modPathsSeen.add(modPath)

            debug.logger & debug.FLAG_BLD and debug.logger(
                "loadModule: evaluating %s" % modPath
            )

            g = {"mibBuilder": self, "userCtx": userCtx}

            try:
                if __debug__:
                    runpy.run_path(
                        modPath, g
                    )  # IMPORTANT: enable break points in loaded MIBs
                else:
                    exec(codeObj, g)

            except Exception:
                self.__modPathsSeen.remove(modPath)
                raise error.MibLoadError(
                    f"MIB module '{modPath}' load error: {traceback.format_exception(*sys.exc_info())}"
                )

            self.__modSeen[modName] = modPath

            debug.logger & debug.FLAG_BLD and debug.logger(
                "loadModule: loaded %s" % modPath
            )

            break

        if modName not in self.__modSeen:
            raise error.MibNotFoundError(
                'MIB file "{}" not found in search path ({})'.format(
                    modName and modName + ".py[co]",
                    ", ".join([str(x) for x in self.__mib_sources]),
                )
            )

        return self

    def load_modules(self, *modNames, **userCtx):
        """Load (optionally, compiling) pysnmp MIB modules."""
        # Build a list of available modules
        if not modNames:
            modNames = {}
            for mibSource in self.__mib_sources:
                for modName in mibSource.listdir():
                    modNames[modName] = None
            modNames = list(modNames)

        if not modNames:
            raise error.MibNotFoundError(f"No MIB module to load at {self}")

        for modName in modNames:
            try:
                self.load_module(modName, **userCtx)

            except error.MibNotFoundError:
                if not self.__mibCompiler:
                    raise

                debug.logger & debug.FLAG_BLD and debug.logger(
                    "loadModules: calling MIB compiler for %s" % modName
                )
                status = self.__mibCompiler.compile(modName, genTexts=self.loadTexts)
                errs = "; ".join(
                    [
                        hasattr(x, "error") and str(x.error) or x
                        for x in status.values()
                        if x in ("failed", "missing")
                    ]
                )
                if errs:
                    raise error.MibNotFoundError(
                        f"{modName} compilation error(s): {errs}"
                    )

                # compilation succeeded, MIB might load now
                self.load_module(modName, **userCtx)

        return self

    def unload_modules(self, *modNames):
        """Unload MIB modules."""
        if not modNames:
            modNames = list(self.mibSymbols.keys())
        for modName in modNames:
            if modName not in self.mibSymbols:
                raise error.MibNotFoundError(f"No module {modName} at {self}")
            self.unexport_symbols(modName)
            self.__modPathsSeen.remove(self.__modSeen[modName])
            del self.__modSeen[modName]

            debug.logger & debug.FLAG_BLD and debug.logger(
                "unloadModules: %s" % modName
            )

        return self

    def import_symbols(self, modName, *symNames, **userCtx) -> "tuple[Any, ...]":
        """Import MIB symbols."""
        if not modName:
            raise error.SmiError("importSymbols: empty MIB module name")
        r = ()
        for symName in symNames:
            if modName not in self.mibSymbols:
                self.load_modules(modName, **userCtx)
            if modName not in self.mibSymbols:
                raise error.MibNotFoundError(f"No module {modName} loaded at {self}")
            if symName not in self.mibSymbols[modName]:
                raise error.SmiError(f"No symbol {modName}::{symName} at {self}")
            r = r + (self.mibSymbols[modName][symName],)
        return r

    def export_symbols(self, modName, *anonymousSyms, **namedSyms):
        """Export MIB symbols."""
        if modName not in self.mibSymbols:
            self.mibSymbols[modName] = {}
        mibSymbols = self.mibSymbols[modName]

        for symObj in anonymousSyms:
            debug.logger & debug.FLAG_BLD and debug.logger(
                "export_symbols: anonymous symbol %s::__pysnmp_%ld"
                % (modName, self._autoName)
            )
            mibSymbols["__pysnmp_%ld" % self._autoName] = symObj
            self._autoName += 1
        for symName, symObj in namedSyms.items():
            if symName in mibSymbols:
                raise error.SmiError(f"Symbol {symName} already exported at {modName}")

            if symName != self.module_id and not isinstance(symObj, classTypes):
                label = symObj.getLabel()
                if label:
                    symName = label
                else:
                    symObj.setLabel(symName)

            mibSymbols[symName] = symObj

            debug.logger & debug.FLAG_BLD and debug.logger(
                f"export_symbols: symbol {modName}::{symName}"
            )

        self.lastBuildId += 1

    def unexport_symbols(self, modName, *symNames):
        """Unexport MIB symbols."""
        if modName not in self.mibSymbols:
            raise error.SmiError(f"No module {modName} at {self}")
        mibSymbols = self.mibSymbols[modName]
        if not symNames:
            symNames = list(mibSymbols.keys())
        for symName in symNames:
            if symName not in mibSymbols:
                raise error.SmiError(f"No symbol {modName}::{symName} at {self}")
            del mibSymbols[symName]

            debug.logger & debug.FLAG_BLD and debug.logger(
                f"unexport_symbols: symbol {modName}::{symName}"
            )

        if not self.mibSymbols[modName]:
            del self.mibSymbols[modName]

        self.lastBuildId += 1

    # Compatibility API
    deprecated_attributes = {
        "importSymbols": "import_symbols",
        "exportSymbols": "export_symbols",
        "unexportSymbols": "unexport_symbols",
        "loadModules": "load_modules",
        "addMibSources": "add_mib_sources",
    }

    def __getattr__(self, attr):
        """Redirect some attrs access to the OID object to behave alike."""
        if new_attr := self.deprecated_attributes.get(attr):
            warnings.warn(
                f"{attr} is deprecated. Please use {new_attr} instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return getattr(self, new_attr)

        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{attr}'"
        )
