import logging

##
# Create the main logger
#
# Do NOT touch the root logger -- not to break basicConfig() etc
#
log = logging.getLogger('pyroute2')
log.setLevel(0)
log.addHandler(logging.NullHandler())


def debug(*argv, **kwarg):
    return log.debug(*argv, **kwarg)


def info(*argv, **kwarg):
    return log.info(*argv, **kwarg)


def warning(*argv, **kwarg):
    return log.warning(*argv, **kwarg)


def error(*argv, **kwarg):
    return log.error(*argv, **kwarg)


def critical(*argv, **kwarg):
    return log.critical(*argv, **kwarg)
