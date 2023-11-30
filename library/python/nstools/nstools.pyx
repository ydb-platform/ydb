from cpython.exc cimport PyErr_SetFromErrno

cdef extern from "<sched.h>" nogil:
    int setns(int fd, int mode)
    int unshare(int flags)

    cpdef enum:
        Fs "CLONE_FS"
        # Cgroup "CLONE_NEWCGROUP"
        Ipc "CLONE_NEWIPC"
        Network "CLONE_NEWNET"
        Mount "CLONE_NEWNS"
        Pid "CLONE_NEWPID"
        User "CLONE_NEWUSER"
        Uts "CLONE_NEWUTS"

def unshare_ns(int flags):
    cdef int ret = unshare(flags)
    if ret != 0:
        PyErr_SetFromErrno(OSError)

def move_to_ns(object fileobject, int mode):
    if not isinstance(fileobject, int):
        fileobject = fileobject.fileno()

    cdef int ret = setns(fileobject, mode)
    if ret != 0:
        PyErr_SetFromErrno(OSError)
