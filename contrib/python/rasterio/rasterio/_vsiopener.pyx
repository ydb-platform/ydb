"""Bridge between Python file openers and GDAL VSI.

Based on _filepath.pyx.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
import contextlib
from contextvars import ContextVar
from functools import singledispatch
import logging
import os
from pathlib import Path
import stat
from uuid import uuid4

from libc.string cimport memcpy

from rasterio.errors import OpenerRegistrationError

log = logging.getLogger(__name__)

cdef str VSI_NS_ROOT = "vsiriopener"

# This is global state for the Python filesystem plugin. It currently only
# contains path -> PyOpenerBase (or subclass) instances. This is used by
# the plugin to determine what "files" exist on "disk".
# Currently the only way to "create" a file in the filesystem is to add
# an entry to this dictionary. GDAL will then Open the path later.
_OPENER_REGISTRY = ContextVar("opener_registery")
_OPENER_REGISTRY.set({})
_OPEN_FILE_EXIT_STACKS = ContextVar("open_file_exit_stacks")
_OPEN_FILE_EXIT_STACKS.set({})


# When an opener is registered for a path, this structure captures the
# path and unique registration instance. VSI stat, read_dir, and open
# calls have access to the struct instance.
cdef struct FSData:
    char *path
    char *uuid


cdef int pyopener_stat(
    void *pUserData,
    const char *pszFilename,
    VSIStatBufL *pStatBuf,
    int nFlags
) with gil:
    """Provides POSIX stat data to GDAL from a Python filesystem."""
    cdef FSData *fsdata = <FSData *>pUserData
    path = fsdata.path.decode("utf-8")
    uuid = fsdata.uuid.decode("utf-8")
    key = (Path(path), uuid)
    urlpath = pszFilename.decode("utf-8")

    registry = _OPENER_REGISTRY.get()
    log.debug(
        "Looking up opener in pyopener_stat: urlpath=%r, registry=%r, key=%r",
        urlpath,
        registry,
        key
    )

    try:
        file_opener = registry[key]
    except KeyError as err:
        errmsg = f"Opener not found: {repr(err)}".encode("utf-8")
        CPLError(CE_Failure, <CPLErrorNum>4, <const char *>"%s", <const char *>errmsg)
        return -1

    try:
        if file_opener.isfile(urlpath):
            fmode = stat.S_IFREG
        elif file_opener.isdir(urlpath):
            fmode = stat.S_IFDIR
        else:
            # No such file or directory.
            return -1
        size = file_opener.size(urlpath)
        mtime = file_opener.mtime(urlpath)
    except (FileNotFoundError, KeyError) as err:
        # No such file or directory.
        return -1
    except Exception as err:
        errmsg = f"Opener failed to determine file info: {repr(err)}".encode("utf-8")
        CPLError(CE_Failure, <CPLErrorNum>4, <const char *>"%s", <const char *>errmsg)
        return -1

    pStatBuf.st_size = size
    pStatBuf.st_mode = fmode
    pStatBuf.st_mtime = mtime
    return 0


cdef int pyopener_unlink(
    void *pUserData,
    const char *pszFilename,
) with gil:
    """Unlink a file from a Python filesystem."""
    cdef FSData *fsdata = <FSData *>pUserData
    path = fsdata.path.decode("utf-8")
    uuid = fsdata.uuid.decode("utf-8")
    key = (Path(path), uuid)
    urlpath = pszFilename.decode("utf-8")

    registry = _OPENER_REGISTRY.get()
    log.debug(
        "Looking up opener in pyopener_unlink: urlpath=%r, registry=%r, key=%r",
        urlpath,
        registry,
        key
    )

    try:
        file_opener = registry[key]
    except KeyError as err:
        errmsg = f"Opener not found: {repr(err)}".encode("utf-8")
        CPLError(CE_Failure, <CPLErrorNum>4, <const char *>"%s", <const char *>errmsg)
        return -1

    try:
        file_opener.rm(urlpath)
        return 0
    except (FileNotFoundError, KeyError) as err:
        # No such file or directory.
        return -1
    except Exception as err:
        errmsg = f"Opener failed to determine file info: {err!r}".encode("utf-8")
        CPLError(CE_Failure, <CPLErrorNum>4, <const char *>"%s", <const char *>errmsg)
        return -1


cdef char ** pyopener_read_dir(
    void *pUserData,
    const char *pszDirname,
    int nMaxFiles
) with gil:
    """Provides a directory listing to GDAL from a Python filesystem."""
    cdef FSData *fsdata = <FSData *>pUserData
    path = fsdata.path.decode("utf-8")
    uuid = fsdata.uuid.decode("utf-8")
    key = (Path(path), uuid)
    urlpath = pszDirname.decode("utf-8")

    registry = _OPENER_REGISTRY.get()
    log.debug(
        "Looking up opener in pyopener_read_dir: urlpath=%r, registry=%r, key=%r",
        urlpath,
        registry,
        key
    )

    try:
        file_opener = registry[key]
    except KeyError as err:
        errmsg = f"Opener not found: {repr(err)}".encode("utf-8")
        CPLError(CE_Failure, <CPLErrorNum>4, <const char *>"%s", <const char *>errmsg)
        return NULL

    try:
        # GDAL wants relative file names.
        contents = [Path(item).name for item in file_opener.ls(urlpath)]
    except (FileNotFoundError, KeyError) as err:
        # No such file or directory.
        return NULL
    except Exception as err:
        errmsg = f"Opener failed to determine directory contents: {repr(err)}".encode("utf-8")
        CPLError(CE_Failure, <CPLErrorNum>4, <const char *>"%s", <const char *>errmsg)
        return NULL

    cdef char **name_list = NULL

    for name in contents:
        fname = name.encode("utf-8")
        name_list = CSLAddString(name_list, <char *>fname)

    return name_list


cdef void* pyopener_open(
    void *pUserData,
    const char *pszFilename,
    const char *pszAccess
) with gil:
    """Access files in the virtual filesystem.

    This function is mandatory in the GDAL Filesystem Plugin API.
    GDAL may call this function multiple times per filename and each
    result must be separately seekable.
    """
    cdef FSData *fsdata = <FSData *>pUserData
    path = fsdata.path.decode("utf-8")
    uuid = fsdata.uuid.decode("utf-8")
    key = (Path(path), uuid)
    urlpath = pszFilename.decode("utf-8")

    mode = pszAccess.decode("utf-8")
    if not "b" in mode:
        mode += "b"

    registry = _OPENER_REGISTRY.get()
    log.debug(
        "Looking up opener in pyopener_open: urlpath=%r, registry=%r, key=%r",
        urlpath,
        registry,
        key
    )

    try:
        file_opener = registry[key]
    except KeyError as err:
        errmsg = f"Opener not found: {repr(err)}".encode("utf-8")
        CPLError(CE_Failure, <CPLErrorNum>4, <const char *>"%s", <const char *>errmsg)
        return NULL

    cdef object file_obj

    try:
        file_obj = file_opener.open(urlpath, mode)
    except ValueError as err:
        # ZipFile.open doesn't accept binary modes like "rb" and will
        # raise ValueError if given one. We strip the mode in this case.
        try:
            file_obj = file_opener.open(urlpath, mode.rstrip("b"))
        except Exception as err:
            return NULL
    except Exception as err:
        return NULL

    log.debug("Opened file object: file_obj=%r, mode=%r", file_obj, mode)

    # Before we return, we attempt to enter the file object's context
    # and store an exit callback stack for it.
    stack = contextlib.ExitStack()

    try:
        file_obj = stack.enter_context(file_obj)
    except (AttributeError, TypeError) as err:
        errmsg = f"Opener failed to open file with arguments ({repr(urlpath)}, {repr(mode)}): {repr(err)}".encode("utf-8")
        CPLError(CE_Failure, <CPLErrorNum>4, <const char *>"%s", <const char *>errmsg)
        return NULL
    except FileNotFoundError as err:
        errmsg = "OpenFile didn't resolve".encode("utf-8")
        return NULL
    else:
        exit_stacks = _OPEN_FILE_EXIT_STACKS.get({})
        exit_stacks[file_obj] = stack
        _OPEN_FILE_EXIT_STACKS.set(exit_stacks)
        return <void *>file_obj


cdef int pyopener_eof(void *pFile) with gil:
    cdef object file_obj = <object>pFile
    if file_obj.read(1):
        file_obj.seek(-1, 1)
        return 1
    else:
        return 0


cdef vsi_l_offset pyopener_tell(void *pFile) with gil:
    cdef object file_obj = <object>pFile
    return <vsi_l_offset>file_obj.tell()


cdef int pyopener_seek(void *pFile, vsi_l_offset nOffset, int nWhence) with gil:
    cdef object file_obj = <object>pFile
    # TODO: Add "seekable" check?
    file_obj.seek(nOffset, nWhence)
    return 0


cdef size_t pyopener_read(void *pFile, void *pBuffer, size_t nSize, size_t nCount) with gil:
    cdef object file_obj = <object>pFile
    cdef bytes python_data = file_obj.read(nSize * nCount)
    cdef int num_bytes = len(python_data)
    # NOTE: We have to cast to char* first, otherwise Cython doesn't do the conversion properly
    memcpy(pBuffer, <void*><unsigned char*>python_data, num_bytes)
    return <size_t>(num_bytes / nSize)


cdef int pyopener_read_multi_range(void *pFile, int nRanges, void **ppData, vsi_l_offset *panOffsets, size_t *panSizes) except -1 with gil:
    cdef object file_obj = <object>pFile

    if not hasattr(file_obj, "get_byte_ranges"):
        errmsg = "MultiRangeRead not implemented for Opener".encode("utf-8")
        CPLError(CE_Failure, <CPLErrorNum>1, <const char *>"%s", <const char *>errmsg)
        return -1

    # NOTE: Convert panOffsets and panSizes to Python lists
    cdef list offsets = [int(panOffsets[i]) for i in range(nRanges)]
    cdef list sizes = [int(panSizes[i]) for i in range(nRanges)]

    # NOTE: Call the Python method with the converted arguments
    cdef list python_data = file_obj.get_byte_ranges(offsets, sizes)
    for i in range(nRanges):
        memcpy(ppData[i], <void*><char*>python_data[i], len(python_data[i]))

    return 0

cdef size_t pyopener_write(void *pFile, void *pBuffer, size_t nSize, size_t nCount) with gil:
    if pBuffer == NULL:
        return -1
    cdef object file_obj = <object>pFile
    buffer_len = nSize * nCount
    cdef unsigned char [:] buff_view = <unsigned char[:buffer_len]>pBuffer
    log.debug(
        "Writing data: file_obj=%r, buff_view=%r, buffer_len=%r",
        file_obj,
        buff_view,
        buffer_len
    )
    try:
        num = file_obj.write(buff_view)
    except TypeError:
        num = file_obj.write(str(buff_view))
    return <size_t>(num // nSize)


cdef int pyopener_flush(void *pFile) with gil:
    cdef object file_obj = <object>pFile
    log.debug("Flushing: file_obj=%r", file_obj)
    try:
        file_obj.flush()
        return 0
    except AttributeError:
        return 1


cdef int pyopener_truncate(void *pFile, vsi_l_offset size) with gil:
    cdef object file_obj = <object>pFile
    log.debug("Truncating: file_obj=%r, size=%r", file_obj, size)
    try:
        file_obj.truncate(size)
        return 0
    except AttributeError:
        return 1


cdef int pyopener_close(void *pFile) with gil:
    cdef object file_obj = <object>pFile
    log.debug("Closing: file_obj=%r", file_obj)
    exit_stacks = _OPEN_FILE_EXIT_STACKS.get()
    stack = exit_stacks.pop(file_obj)
    stack.close()
    _OPEN_FILE_EXIT_STACKS.set(exit_stacks)
    return 0


@contextlib.contextmanager
def _opener_registration(urlpath, obj):
    cdef char **registered_prefixes = NULL
    cdef int prefix_index = 0
    cdef VSIFilesystemPluginCallbacksStruct *callbacks_struct = NULL
    cdef FSData fsdata
    cdef char *path_c = NULL
    cdef char *uuid_c = NULL

    # To resolve issue 1406 we add the opener or filesystem id to the
    # registry key.
    kpath = Path(urlpath).parent
    kid = uuid4().hex
    key = (kpath, kid)

    path_b = kpath.as_posix().encode("utf-8")
    path_c = path_b
    uuid_b = kid.encode("utf-8")
    uuid_c = uuid_b

    fsdata = FSData(path_c, uuid_c)

    namespace = f"{VSI_NS_ROOT}_{kid}"
    cdef bytes prefix_bytes = f"/{namespace}/".encode("utf-8")

    opener = to_pyopener(obj)

    # Before returning we do a quick check that the opener will
    # plausibly function.
    try:
        _ = opener.size("test")
    except (AttributeError, TypeError, ValueError) as err:
        raise OpenerRegistrationError(f"Opener is invalid.") from err
    except Exception:
        # We expect the path to not resolve.
        pass

    registry = _OPENER_REGISTRY.get({})

    if key in registry:
        if registry[key] != opener:
            raise OpenerRegistrationError(f"Opener already registered for urlpath.")
        else:
            try:
                yield f"/{namespace}/{urlpath}"
            finally:
                registry = _OPENER_REGISTRY.get()
                _ = registry.pop(key, None)
                _OPENER_REGISTRY.set(registry)

    else:
        # Install handler.
        registered_prefixes = VSIGetFileSystemsPrefixes()
        prefix_index = CSLFindString(<CSLConstList>registered_prefixes, prefix_bytes)
        CSLDestroy(registered_prefixes)

        if prefix_index < 0:
            log.debug("Installing Python opener handler plugin: prefix_bytes=%r", prefix_bytes)
            callbacks_struct = VSIAllocFilesystemPluginCallbacksStruct()
            callbacks_struct.open = <VSIFilesystemPluginOpenCallback>pyopener_open
            callbacks_struct.eof = <VSIFilesystemPluginEofCallback>pyopener_eof
            callbacks_struct.tell = <VSIFilesystemPluginTellCallback>pyopener_tell
            callbacks_struct.seek = <VSIFilesystemPluginSeekCallback>pyopener_seek
            callbacks_struct.read = <VSIFilesystemPluginReadCallback>pyopener_read
            callbacks_struct.write = <VSIFilesystemPluginWriteCallback>pyopener_write
            callbacks_struct.flush = <VSIFilesystemPluginFlushCallback>pyopener_flush
            callbacks_struct.truncate = <VSIFilesystemPluginTruncateCallback>pyopener_truncate
            callbacks_struct.close = <VSIFilesystemPluginCloseCallback>pyopener_close
            callbacks_struct.read_dir = <VSIFilesystemPluginReadDirCallback>pyopener_read_dir
            callbacks_struct.stat = <VSIFilesystemPluginStatCallback>pyopener_stat
            callbacks_struct.unlink = <VSIFilesystemPluginUnlinkCallback>pyopener_unlink

            if isinstance(opener, MultiByteRangeResourceContainer):
                callbacks_struct.read_multi_range = <VSIFilesystemPluginReadMultiRangeCallback>pyopener_read_multi_range

            callbacks_struct.pUserData = &fsdata

            retval = VSIInstallPluginHandler(prefix_bytes, callbacks_struct)
            VSIFreeFilesystemPluginCallbacksStruct(callbacks_struct)

        registry[key] = opener
        _OPENER_REGISTRY.set(registry)

        try:
            yield f"/{namespace}/{urlpath}"
        finally:
            registry = _OPENER_REGISTRY.get()
            _ = registry.pop(key, None)
            _OPENER_REGISTRY.set(registry)

            IF (CTE_GDAL_MAJOR_VERSION, CTE_GDAL_MINOR_VERSION) >= (3, 9):
                retval = VSIRemovePluginHandler(prefix_bytes)


class FileContainer(ABC):
    """An object that can report on and open Python files."""
    @abstractmethod
    def open(self, path: str, mode: str = "r", **kwds):
        """Get a Python file object for a resource.

        Parameters
        ----------
        path : str
            The identifier/locator for a resource within a filesystem.
        mode : str
            Opening mode.
        kwds : dict
            Opener specific options. Encoding, etc.

        Returns
        -------
        obj
            A Python 'file' object with methods read/write, seek, tell,
            etc.
        """
        pass

    @abstractmethod
    def isfile(self, path: str) -> bool:
        """Test if the resource is a 'file', a sequence of bytes.

        Parameters
        ----------
        path : str
            The identifier/locator for a resource within a filesystem.

        Returns
        -------
        bool
        """
        pass

    @abstractmethod
    def isdir(self, path: str) -> bool:
        """Test if the resource is a 'directory', a container.

        Parameters
        ----------
        path : str
            The identifier/locator for a resource within a filesystem.

        Returns
        -------
        bool
        """
        pass

    @abstractmethod
    def ls(self, path: str) -> list[str]:
        """Get a 'directory' listing.

        Parameters
        ----------
        path : str
            The identifier/locator for a directory within a filesystem.

        Returns
        -------
        list of str
            List of 'path' paths relative to the directory.
        """
        pass

    @abstractmethod
    def mtime(self, path: str) -> int:
        """Get the mtime of a resource..

        Parameters
        ----------
        path : str
            The identifier/locator for a directory within a filesystem.

        Returns
        -------
        int
            Modification timestamp in seconds.
        """
        pass

    @abstractmethod
    def rm(self, path: str) -> None:
        """Remove a resource.

        Parameters
        ----------
        path : str
            The identifier/locator for a resource within a filesystem.

        Returns
        -------
        None
        """
        pass

    @abstractmethod
    def size(self, path: str) -> int:
        """Get the size, in bytes, of a resource..

        Parameters
        ----------
        path : str
            The identifier/locator for a resource within a filesystem.

        Returns
        -------
        int
        """
        pass


class MultiByteRangeResource(ABC):
    """An object that provides VSIFilesystemPluginReadMultiRangeCallback."""
    @abstractmethod
    def get_byte_ranges(self, offsets: list[int], sizes: list[int]) -> list[bytes]:
        """Get a sequence of bytes specified by a sequence of ranges."""
        pass


class MultiByteRangeResourceContainer(FileContainer):
    """An object that can open a MultiByteRangeResource."""
    @abstractmethod
    def open(self, path: str, **kwds) ->  MultiByteRangeResource:
        """Open the resource at the given path."""
        pass


class _FileContainer(FileContainer):
    """Adapts a Python file object to the opener interface."""
    def __init__(self, obj):
        self._obj = obj
    def open(self, path, mode="r", **kwds):
        return self._obj(path, mode=mode, **kwds)
    def isfile(self, path):
        return True
    def isdir(self, path):
        return False
    def ls(self, path):
        return []
    def mtime(self, path):
        return 0
    def rm(self, path):
        pass
    def size(self, path):
        with self._obj(path) as f:
            f.seek(0, os.SEEK_END)
            return f.tell()


class _FilesystemContainer(FileContainer):
    """Adapts an fsspec filesystem object to the opener interface."""
    def __init__(self, obj):
        self._obj = obj
    def open(self, path, mode="r", **kwds):
        return self._obj.open(path, mode=mode, **kwds)
    def isfile(self, path):
        return self._obj.isfile(path)
    def isdir(self, path):
        return self._obj.isdir(path)
    def ls(self, path):
        # return value of ls() varies between file and zip fsspec filesystems.
        return [item if isinstance(item, str) else item["filename"] for item in self._obj.ls(path)]
    def mtime(self, path):
        try:
            mtime = int(self._obj.modified(path).timestamp())
        except NotImplementedError:
            mtime = 0
        return mtime
    def rm(self, path):
        return self._obj.rm(path)
    def size(self, path):
        return self._obj.size(path)


class _AltFilesystemContainer(_FilesystemContainer):
    """Adapts a tiledb virtual filesystem object to the opener interface."""
    def isfile(self, path):
        return self._obj.is_file(path)
    def isdir(self, path):
        return self._obj.is_dir(path)
    def mtime(self, path):
        return 0
    def rm(self, path):
        self._obj.remove_file(path)
    def size(self, path):
        return self._obj.file_size(path)


@singledispatch
def to_pyopener(obj):
    """Adapt an object to the Pyopener interface."""
    if hasattr(obj, "file_size"):
        return _AltFilesystemContainer(obj)
    else:
        return _FilesystemContainer(obj)


@to_pyopener.register(FileContainer)
def _(obj):
    return obj


@to_pyopener.register(Callable)
def _(obj):
    return _FileContainer(obj)
