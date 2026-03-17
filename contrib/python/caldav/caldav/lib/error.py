#!/usr/bin/env python
import logging
from collections import defaultdict
from typing import Dict
from typing import Optional

from caldav import __version__

debug_dump_communication = False
try:
    import os

    ## Environmental variables prepended with "PYTHON_CALDAV" are used for debug purposes,
    ## environmental variables prepended with "CALDAV_" are for connection parameters
    debug_dump_communication = os.environ.get("PYTHON_CALDAV_COMMDUMP", False)
    ## one of DEBUG_PDB, DEBUG, DEVELOPMENT, PRODUCTION
    debugmode = os.environ["PYTHON_CALDAV_DEBUGMODE"]
except:
    if "dev" in __version__ or __version__ == "(unknown)":
        debugmode = "DEVELOPMENT"
    else:
        debugmode = "PRODUCTION"

log = logging.getLogger("caldav")
if debugmode.startswith("DEBUG"):
    log.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.WARNING)


def errmsg(r) -> str:
    """Utility for formatting a an error response to an error string"""
    return "%s %s\n\n%s" % (r.status, r.reason, r.raw)


def weirdness(*reasons):
    from caldav.lib.debug import xmlstring

    reason = " : ".join([xmlstring(x) for x in reasons])
    log.warning(f"Deviation from expectations found: {reason}")
    if debugmode == "DEBUG_PDB":
        log.error(f"Dropping into debugger due to {reason}")
        import pdb

        pdb.set_trace()


def assert_(condition: object) -> None:
    try:
        assert condition
    except AssertionError:
        if debugmode == "PRODUCTION":
            log.error(
                "Deviation from expectations found.  %s" % ERR_FRAGMENT, exc_info=True
            )
        elif debugmode == "DEBUG_PDB":
            log.error("Deviation from expectations found.  Dropping into debugger")
            import pdb

            pdb.set_trace()
        else:
            raise


ERR_FRAGMENT: str = "Please consider raising an issue at https://github.com/python-caldav/caldav/issues or reach out to t-caldav@tobixen.no, include this error and the traceback (if any) and tell what server you are using"


class DAVError(Exception):
    url: Optional[str] = None
    reason: str = "no reason"

    def __init__(self, url: Optional[str] = None, reason: Optional[str] = None) -> None:
        if url:
            self.url = url
        if reason:
            self.reason = reason

    def __str__(self) -> str:
        return "%s at '%s', reason %s" % (
            self.__class__.__name__,
            self.url,
            self.reason,
        )


class AuthorizationError(DAVError):
    """
    The client encountered an HTTP 403 error and is passing it on
    to the user. The url property will contain the url in question,
    the reason property will contain the excuse the server sent.
    """

    pass


class PropsetError(DAVError):
    pass


class ProppatchError(DAVError):
    pass


class PropfindError(DAVError):
    pass


class ReportError(DAVError):
    pass


class MkcolError(DAVError):
    pass


class MkcalendarError(DAVError):
    pass


class PutError(DAVError):
    pass


class DeleteError(DAVError):
    pass


class NotFoundError(DAVError):
    pass


class ConsistencyError(DAVError):
    pass


class ResponseError(DAVError):
    pass


exception_by_method: Dict[str, DAVError] = defaultdict(lambda: DAVError)
for method in (
    "delete",
    "put",
    "mkcalendar",
    "mkcol",
    "report",
    "propset",
    "propfind",
    "proppatch",
):
    exception_by_method[method] = locals()[method[0].upper() + method[1:] + "Error"]
