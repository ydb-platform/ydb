from . cimport sd_daemon


cpdef sd_notify(str line, unset_environment=False):
    """ Send notification to systemd daemon

    :type line: str
    :type: unset_environment: bool
    :return: int
    :raises RuntimeError: When c-call returns zero
    :raises ValueError: Otherwise
    """

    cdef bytes bline = line.encode()
    cdef char* cline = bline
    cdef int unset_env, result
    cdef char cunset_env = 2 if unset_environment else 0

    result = sd_daemon.sd_notify(cunset_env, cline)

    if result > 0:
        return result
    elif result == 0:
        raise RuntimeError("Data could not be sent")
    else:
        raise ValueError("Notification error #%d" % result, result)
