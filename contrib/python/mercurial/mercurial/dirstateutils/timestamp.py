# Copyright Mercurial Contributors
#
# This software may be used and distributed according to the terms of the
# GNU General Public License version 2 or any later version.

from __future__ import annotations

import functools
import os
import stat
import time

from .. import error


rangemask = 0x7FFFFFFF


@functools.total_ordering
class timestamp(tuple):
    """
    A Unix timestamp with optional nanoseconds precision,
    modulo 2**31 seconds.

    A 3-tuple containing:

    `truncated_seconds`: seconds since the Unix epoch,
    truncated to its lower 31 bits

    `subsecond_nanoseconds`: number of nanoseconds since `truncated_seconds`.
    When this is zero, the sub-second precision is considered unknown.

    `second_ambiguous`: whether this timestamp is still "reliable"
    (see `reliable_mtime_of`) if we drop its sub-second component.
    """

    def __new__(cls, value):
        truncated_seconds, subsec_nanos, second_ambiguous = value
        value = (truncated_seconds & rangemask, subsec_nanos, second_ambiguous)
        return super().__new__(cls, value)

    def __eq__(self, other):
        raise error.ProgrammingError(
            'timestamp should never be compared directly'
        )

    def __gt__(self, other):
        raise error.ProgrammingError(
            'timestamp should never be compared directly'
        )


def get_fs_now(vfs) -> timestamp | None:
    """return a timestamp for "now" in the current vfs

    This will raise an exception if no temporary files could be created.
    """
    tmpfd, tmpname = vfs.mkstemp()
    try:
        return mtime_of(os.fstat(tmpfd))
    finally:
        os.close(tmpfd)
        vfs.unlink(tmpname)


def zero() -> timestamp:
    """
    Returns the `timestamp` at the Unix epoch.
    """
    return tuple.__new__(timestamp, (0, 0))


def mtime_of(stat_result: os.stat_result) -> timestamp:
    """
    Takes an `os.stat_result`-like object and returns a `timestamp` object
    for its modification time.
    """
    try:
        # TODO: add this attribute to `osutil.stat` objects,
        # see `mercurial/cext/osutil.c`.
        #
        # This attribute is also not available on Python 2.
        nanos = stat_result.st_mtime_ns
    except AttributeError:
        # https://docs.python.org/2/library/os.html#os.stat_float_times
        # "For compatibility with older Python versions,
        #  accessing stat_result as a tuple always returns integers."
        secs = stat_result[stat.ST_MTIME]

        subsec_nanos = 0
    else:
        billion = int(1e9)
        secs = nanos // billion
        subsec_nanos = nanos % billion

    return timestamp((secs, subsec_nanos, False))


def reliable_mtime_of(
    stat_result: os.stat_result, present_mtime: timestamp
) -> timestamp | None:
    """Wrapper for `make_mtime_reliable` for stat objects"""
    file_mtime = mtime_of(stat_result)
    return make_mtime_reliable(file_mtime, present_mtime)


def make_mtime_reliable(
    file_timestamp: timestamp, present_mtime: timestamp
) -> timestamp | None:
    """Same as `mtime_of`, but return `None` or a `Timestamp` with
    `second_ambiguous` set if the date might be ambiguous.

    A modification time is reliable if it is older than "present_time" (or
    sufficiently in the future).

    Otherwise a concurrent modification might happens with the same mtime.
    """
    file_second = file_timestamp[0]
    file_ns = file_timestamp[1]
    boundary_second = present_mtime[0]
    boundary_ns = present_mtime[1]
    # If the mtime of the ambiguous file is younger (or equal) to the starting
    # point of the `status` walk, we cannot garantee that another, racy, write
    # will not happen right after with the same mtime and we cannot cache the
    # information.
    #
    # However if the mtime is far away in the future, this is likely some
    # mismatch between the current clock and previous file system operation. So
    # mtime more than one days in the future are considered fine.
    if boundary_second == file_second:
        if file_ns and boundary_ns:
            if file_ns < boundary_ns:
                return timestamp((file_second, file_ns, True))
        return None
    elif boundary_second < file_second < (3600 * 24 + boundary_second):
        return None
    else:
        return file_timestamp


FS_TICK_WAIT_TIMEOUT = 0.1  # 100 milliseconds


def wait_until_fs_tick(vfs) -> tuple[timestamp, bool] | None:
    """Wait until the next update from the filesystem time by writing in a loop
    a new temporary file inside the working directory and checking if its time
    differs from the first one observed.

    Returns `None` if we are unable to get the filesystem time,
    `(timestamp, True)` if we've timed out waiting for the filesystem clock
    to tick, and `(timestamp, False)` if we've waited successfully.

    On Linux, your average tick is going to be a "jiffy", or 1/HZ.
    HZ is your kernel's tick rate (if it has one configured) and the value
    is the one returned by `grep 'CONFIG_HZ=' /boot/config-$(uname -r)`,
    again assuming a normal setup.

    In my case (Alphare) at the time of writing, I get `CONFIG_HZ=250`,
    which equates to 4ms.
    This might change with a series that could make it to Linux 6.12:
    https://lore.kernel.org/all/20241002-mgtime-v10-8-d1c4717f5284@kernel.org
    """
    start = time.monotonic()

    try:
        old_fs_time = get_fs_now(vfs)
        new_fs_time = get_fs_now(vfs)

        while (
            new_fs_time[0] == old_fs_time[0]
            and new_fs_time[1] == old_fs_time[1]
        ):
            if time.monotonic() - start > FS_TICK_WAIT_TIMEOUT:
                return (old_fs_time, True)
            new_fs_time = get_fs_now(vfs)
    except OSError:
        return None
    else:
        return (new_fs_time, False)
