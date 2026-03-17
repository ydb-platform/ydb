cdef extern from "<systemd/sd-daemon.h>" nogil:
    int sd_notify(int unset_environment, const char *state)
