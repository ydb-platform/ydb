import warnings

__version__ = "1.6.4"

try:
    from ._fallocate import fallocate as _fallocate
    def fallocate(fd, offset, len, mode=0):
        """
        fallocate(fd, offset, len, [mode=0])

        Calls fallocate() on the file object or file descriptor. This allows
        the caller to directly manipulate the allocated disk space for the file
        referred to by fd for the byte range starting at offset and continuing
        for len bytes.

        mode is only available in Linux.

        It should always be 0 unless one of the following possible flags are
        specified:
            FALLOC_FL_KEEP_SIZE     - do not grow file, default is extend size
            FALLOC_FL_PUNCH_HOLE    - punches a hole in file, de-allocates range
            FALLOC_FL_COLLAPSE_SIZE - remove a range of a file without leaving a hole
        """
        if hasattr(fd, 'fileno'):
            fd = fd.fileno()
        return _fallocate(fd, mode, offset, len)
    fallocate.__doc__ = _fallocate.__doc__
except ImportError:
    def fallocate(fd, offset, len, mode=0):
        """ fallocate(2) or OSX equivalent was not found on this system"""
        warnings.warn("fallocate(2) or OSX equivalent was not found on this system")
try:
    from ._fallocate import FALLOC_FL_KEEP_SIZE, FALLOC_FL_PUNCH_HOLE, FALLOC_FL_COLLAPSE_SIZE
except ImportError:
    pass

try:
    from ._fallocate import posix_fallocate as _posix_fallocate
    from ._fallocate import posix_fadvise as _posix_fadvise
    from ._fallocate import POSIX_FADV_NORMAL, POSIX_FADV_SEQUENTIAL, POSIX_FADV_RANDOM, POSIX_FADV_NOREUSE, POSIX_FADV_WILLNEED, POSIX_FADV_DONTNEED
    def posix_fallocate(fd, offset, len):
        if hasattr(fd, 'fileno'):
            fd = fd.fileno()
        return _posix_fallocate(fd, offset, len)
    posix_fallocate.__doc__ = _posix_fallocate.__doc__

    def posix_fadvise(fd, offset, len, advise):
        if hasattr(fd, 'fileno'):
            fd = fd.fileno()
        return _posix_fadvise(fd, offset, len, advise)
    posix_fadvise.__doc__ = _posix_fadvise.__doc__
except ImportError:
    def posix_fallocate(fd, offset, len):
        """ posix_fallocate(3) was not found on this system """
        warnings.warn("posix_fallocate(3) was not found on this system")

    def posix_fadvise(fd, offset, len, advise):
        """ posix_advise(3) was not found on this system"""
        warnings.warn("posix_advise(3) was not found on this system")
