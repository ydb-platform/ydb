import logging
import os

logger = logging.getLogger("PYREPL_TRACE")

trace_filename = os.environ.get("PYREPL_TRACE")

if trace_filename:
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(trace_filename)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)


def trace(line, *k, **kw):
    if not trace_filename:
        return
    if k or kw:
        line = line.format(*k, **kw)
    logger.debug(line)
