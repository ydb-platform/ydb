cdef extern from "<sys/prctl.h>":
    int prctl(int option, unsigned long arg2, unsigned long arg3, unsigned long arg4, unsigned long arg5);


PR_SET_PDEATHSIG = 1
PR_SET_CHILD_SUBREAPER = 36


def set_pdeathsig(signum):
    return prctl(PR_SET_PDEATHSIG, signum, 0, 0, 0)


def set_child_subreaper(val):
    return prctl(PR_SET_CHILD_SUBREAPER, val, 0, 0, 0)
