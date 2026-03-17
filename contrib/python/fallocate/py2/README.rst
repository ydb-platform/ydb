=========
fallocate
=========

.. image:: https://travis-ci.org/trbs/fallocate.svg?branch=master
    :target: https://travis-ci.org/trbs/fallocate

fallocate exposes an interface to fallocate(2), posix_fallocate(3) and
posix_fadvise(3).

Under Mac OS X the fallocate() method will use the apple equivalent of
fallocate(2). Note that this might not be exactly the same.

When using the wrapper functions around the fallocate(2) call the arguments
given to the function are slightly different then the c call.

This module has the arguments like:

::

  fallocate(fd, offset, length, mode=0)

While in C the function looks like:

::

  fallocate(fd, mode, offset, length)

The main reason for this is that the mode argument tends not to be used much and
thus having the default as a keyword argument is much easier then having to
specify 0 everytime.

Usage
=====

Funcation: fallocate(fd, offset, len, [mode=0])

Calls fallocate() on the file object or file descriptor. This allows
the caller to directly manipulate the allocated disk space for the file
referred to by fd for the byte range starting at offset and continuing
for len bytes.

mode is only available in Linux.

It should always be 0 unless one of the following possible flags are
specified

::

    FALLOC_FL_KEEP_SIZE     - do not grow file, default is extend size
    FALLOC_FL_PUNCH_HOLE    - punches a hole in file, de-allocates range
    FALLOC_FL_COLLAPSE_SIZE - remove a range of a file without leaving a hole

*Note*: `FALLOC_FL_COLLAPSE_SIZE` was introduced in Linux kernel v3.15 and is
only available on certain filesystems (e.g. ext4, xfs, etc). In order to get
access to it, you must build and run on a kernel and filesystem that both
support it.

Example:

::

    # preallocate using fallocate a 1kb file
    with open("/tmp/test.file", "w+b") as f:
        fallocate(f, 0, 1024)
