# cython: language_level=3, boundscheck=False
# distutils: language = c++
"""Bridge between Python file-like objects and GDAL VSI.

The functionality provided in this interface is made possible thanks to GDAL's
Plugin infrastructure. You can find more information about this below and in
GDAL's documentation here:

https://gdal.org/api/cpl.html#structVSIFilesystemPluginCallbacksStruct

.. note::

    Parts of GDAL's plugin interface use C++ features/definitions. For
    that reason this module must be compiled as C++.

The high-level idea of the plugin interface is to define a series of callbacks
for the operations GDAL may need to perform. There are two types of operations:
filesystem and file. The filesystem operations cover things like opening a file,
making directories, renaming files, etc. The file operations involve things like
reading from the file, seeking to a specific position, and getting the current
position in the file.

Filesystem Handling
*******************

This plugin currently only defines the "open" callback. The other features are
either not needed or have usable default implementations.

The entire filesystem's state is stored in a global dictionary mapping
in-memory GDAL filenames to :class:`~rasterio._filepath.FilePathBase` objects.

File Handling
*************

This plugin implements the bare minimum for reading from an open file-like
object. It does this by mapping GDAL's function calls (ex. read, seek) to
the corresponding method call on the file-like object.

"""

include "gdal.pxi"

import logging
from uuid import uuid4

from libc.string cimport memcpy

log = logging.getLogger(__name__)


# NOTE: This has to be defined outside of gdal.pxi or other C extensions will
# try to compile C++ only code included in this header.
cdef extern from "cpl_vsi_virtual.h":
    cdef cppclass VSIFileManager:
        @staticmethod
        void* GetHandler(const char*)


# Prefix for all in-memory paths used by GDAL's VSI system
# Except for errors and log messages this shouldn't really be seen by the user
cdef str FILESYSTEM_PREFIX = "/vsipythonfilelike/"
cdef bytes FILESYSTEM_PREFIX_BYTES = FILESYSTEM_PREFIX.encode("ascii")
# This is global state for the Python filesystem plugin. It currently only
# contains path -> FilePathBase (or subclass) instances. This is used by
# the plugin to determine what "files" exist on "disk".
# Currently the only way to "create" a file in the filesystem is to add
# an entry to this dictionary. GDAL will then Open the path later.
cdef _FILESYSTEM_INFO = {}
cdef _OPEN_FILE_OBJS = set()

cdef int install_filepath_plugin(VSIFilesystemPluginCallbacksStruct *callbacks_struct):
    """Install handlers for python file-like objects if it isn't already installed."""
    cdef char **registered_prefixes = VSIGetFileSystemsPrefixes()
    cdef int prefix_index = CSLFindString(registered_prefixes, FILESYSTEM_PREFIX_BYTES)
    CSLDestroy(registered_prefixes)

    if prefix_index < 0:
        callbacks_struct = VSIAllocFilesystemPluginCallbacksStruct()
        callbacks_struct.open = <VSIFilesystemPluginOpenCallback>filepath_open
        callbacks_struct.tell = <VSIFilesystemPluginTellCallback>filepath_tell
        callbacks_struct.seek = <VSIFilesystemPluginSeekCallback>filepath_seek
        callbacks_struct.read = <VSIFilesystemPluginReadCallback>filepath_read
        callbacks_struct.close = <VSIFilesystemPluginCloseCallback>filepath_close
        callbacks_struct.pUserData = <void*>_FILESYSTEM_INFO
        log.debug("Installing FilePath filesystem handler plugin...")
        retval = VSIInstallPluginHandler(FILESYSTEM_PREFIX_BYTES, callbacks_struct)
        VSIFreeFilesystemPluginCallbacksStruct(callbacks_struct)
        return retval
    else:
        return 0


cdef void uninstall_filepath_plugin(VSIFilesystemPluginCallbacksStruct *callbacks_struct):
    if callbacks_struct is not NULL:
        callbacks_struct.pUserData = NULL
        VSIFreeFilesystemPluginCallbacksStruct(callbacks_struct)
    callbacks_struct = NULL


## Filesystem Functions

def clone_file_obj(fobj):
    """Clone a filelike object.

    Supports BytesIO, MemoryFile, fsspec files, and Python file objects.

    """
    if hasattr(fobj, "fs"):
        new_fobj = fobj.fs.open(fobj.path, fobj.mode)
    elif hasattr(fobj, "getbuffer"):
        new_fobj = fobj.__class__(fobj.getbuffer())
    else:
        new_fobj = open(fobj.name, fobj.mode)

    return new_fobj


cdef void* filepath_open(void *pUserData, const char *pszFilename, const char *pszAccess) with gil:
    """Access files in the virtual filesystem.

    This function is mandatory in the GDAL Filesystem Plugin API.

    This function returns clones of the file wrappers stored in
    _FILESYSTEM_INFO. GDAL may call this function multiple times per
    filename and each result must be separately seekable.

    """
    cdef object file_obj

    if pszAccess != b"r" and pszAccess != b"rb":
        log.error("FilePath is currently a read-only interface.")
        return NULL

    if pUserData is NULL:
        log.error("FilePath filesystem accessed with uninitialized filesystem info.")
        return NULL
    cdef dict filesystem_info = <object>pUserData

    try:
        file_obj = clone_file_obj(filesystem_info[pszFilename])
    except KeyError:
        log.info("Object not found in virtual filesystem: filename=%r", pszFilename)
        return NULL

    # Open file wrappers are kept in this set and removed when closed.
    _OPEN_FILE_OBJS.add(file_obj)

    return <void *>file_obj

## File functions

cdef vsi_l_offset filepath_tell(void *pFile) with gil:
    cdef object file_obj = <object>pFile
    cdef long pos = file_obj.tell()
    return <vsi_l_offset>pos


cdef int filepath_seek(void *pFile, vsi_l_offset nOffset, int nWhence) except -1 with gil:
    cdef object file_obj = <object>pFile
    # TODO: Add "seekable" check?
    file_obj.seek(nOffset, nWhence)
    return 0


cdef size_t filepath_read(void *pFile, void *pBuffer, size_t nSize, size_t nCount) with gil:
    cdef object file_obj = <object>pFile
    cdef bytes python_data = file_obj.read(nSize * nCount)
    cdef int num_bytes = len(python_data)
    # NOTE: We have to cast to char* first, otherwise Cython doesn't do the conversion properly
    memcpy(pBuffer, <void*><char*>python_data, num_bytes)
    return <size_t>(num_bytes / nSize)


cdef int filepath_close(void *pFile) except -1 with gil:
    cdef object file_obj = <object>pFile
    _OPEN_FILE_OBJS.remove(file_obj)
    return 0


cdef class FilePathBase:
    """Base for a BytesIO-like class backed by a Python file-like object."""

    def __init__(self, filelike_obj, dirname=None, filename=None):
        """A file in an in-memory filesystem.

        Parameters
        ----------
        filelike_obj : file-like objects
            A file opened in binary mode
        filename : str
            An optional filename used internally by GDAL. If not provided then
            a unique one will be generated.

        """
        if isinstance(filelike_obj, (bytes, str)) or not hasattr(filelike_obj, "read"):
            raise TypeError("FilePath expects file-like objects only.")

        # Make an in-memory directory specific to this dataset to help organize
        # auxiliary files.
        self._dirname = dirname or str(uuid4())

        # GDAL's SRTMHGT driver requires the filename to be "correct" (match
        # the bounds being written).
        self._filename = filename or self._dirname

        self.name = "{0}{1}/{2}".format(FILESYSTEM_PREFIX, self._dirname, self._filename)

        self._path = self.name.encode('utf-8')
        self._filepath_path = self._path[len(FILESYSTEM_PREFIX):]
        self._file_obj = filelike_obj
        self.mode = "r"
        _FILESYSTEM_INFO[self._filepath_path] = self._file_obj
        self.closed = False

    def exists(self):
        """Test if the in-memory file exists.

        Returns
        -------
        bool
            True if the in-memory file exists.

        """
        cdef VSIStatBufL st_buf
        return VSIStatL(self._path, &st_buf) == 0

    def __len__(self):
        """Length of the file's buffer in number of bytes.

        Returns
        -------
        int
        """
        try:
            return len(self._file_obj)
        except (TypeError, AttributeError):
            pass

        try:
            return self._file_obj.size
        except AttributeError:
            raise RuntimeError("Could not determine length for provided "
                               "file-like object.")

    def close(self):
        """Mark the file as closed.

        This does not actually attempt to close the file; that is left up
        to the user.

        """
        if self.closed:
            return

        _ = _FILESYSTEM_INFO.pop(self._filepath_path)
        self.closed = True
