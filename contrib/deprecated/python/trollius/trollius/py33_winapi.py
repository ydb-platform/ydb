
__all__ = [
    'CloseHandle', 'CreateNamedPipe', 'CreateFile', 'ConnectNamedPipe',
    'NULL',
    'GENERIC_READ', 'GENERIC_WRITE', 'OPEN_EXISTING', 'INFINITE',
    'PIPE_ACCESS_INBOUND',
    'PIPE_ACCESS_DUPLEX', 'PIPE_TYPE_MESSAGE', 'PIPE_READMODE_MESSAGE',
    'PIPE_WAIT', 'PIPE_UNLIMITED_INSTANCES', 'NMPWAIT_WAIT_FOREVER',
    'FILE_FLAG_OVERLAPPED', 'FILE_FLAG_FIRST_PIPE_INSTANCE',
    'WaitForMultipleObjects', 'WaitForSingleObject',
    'WAIT_OBJECT_0', 'ERROR_IO_PENDING',
    ]

try:
    # FIXME: use _overlapped on Python 3.3? see windows_utils.pipe()
    from _winapi import (
        CloseHandle, CreateNamedPipe, CreateFile, ConnectNamedPipe,
        NULL,
        GENERIC_READ, GENERIC_WRITE, OPEN_EXISTING, INFINITE,
        PIPE_ACCESS_INBOUND,
        PIPE_ACCESS_DUPLEX, PIPE_TYPE_MESSAGE, PIPE_READMODE_MESSAGE,
        PIPE_WAIT, PIPE_UNLIMITED_INSTANCES, NMPWAIT_WAIT_FOREVER,
        FILE_FLAG_OVERLAPPED, FILE_FLAG_FIRST_PIPE_INSTANCE,
        WaitForMultipleObjects, WaitForSingleObject,
        WAIT_OBJECT_0, ERROR_IO_PENDING,
    )
except ImportError:
    # Python < 3.3
    from _multiprocessing import win32
    import _subprocess

    from trollius import _overlapped

    CloseHandle = win32.CloseHandle
    CreateNamedPipe = win32.CreateNamedPipe
    CreateFile = win32.CreateFile
    NULL = win32.NULL

    GENERIC_READ = win32.GENERIC_READ
    GENERIC_WRITE = win32.GENERIC_WRITE
    OPEN_EXISTING = win32.OPEN_EXISTING
    INFINITE = win32.INFINITE

    PIPE_ACCESS_INBOUND = win32.PIPE_ACCESS_INBOUND
    PIPE_ACCESS_DUPLEX = win32.PIPE_ACCESS_DUPLEX
    PIPE_READMODE_MESSAGE = win32.PIPE_READMODE_MESSAGE
    PIPE_TYPE_MESSAGE = win32.PIPE_TYPE_MESSAGE
    PIPE_WAIT = win32.PIPE_WAIT
    PIPE_UNLIMITED_INSTANCES = win32.PIPE_UNLIMITED_INSTANCES
    NMPWAIT_WAIT_FOREVER = win32.NMPWAIT_WAIT_FOREVER

    FILE_FLAG_OVERLAPPED = 0x40000000
    FILE_FLAG_FIRST_PIPE_INSTANCE = 0x00080000

    WAIT_OBJECT_0 = _subprocess.WAIT_OBJECT_0
    WaitForSingleObject = _subprocess.WaitForSingleObject
    ERROR_IO_PENDING = _overlapped.ERROR_IO_PENDING

    def ConnectNamedPipe(handle, overlapped):
        ov = _overlapped.Overlapped()
        ov.ConnectNamedPipe(handle)
        return ov

    def WaitForMultipleObjects(events, wait_all, timeout):
        if not wait_all:
            raise NotImplementedError()

        for ev in events:
            res = WaitForSingleObject(ev, timeout)
            if res != WAIT_OBJECT_0:
                err = win32.GetLastError()
                msg = _overlapped.FormatMessage(err)
                raise WindowsError(err, msg)

        return WAIT_OBJECT_0
