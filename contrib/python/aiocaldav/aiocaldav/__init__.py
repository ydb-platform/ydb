#!/usr/bin/env python

from .davclient import DAVClient
from .objects import *

# possibly a bug in the tBaxter fork of vobject, this one has to be
# imported explicitly to make sure the attribute behaviour gets
# correctly loaded:
from vobject import icalendar
import logging

# Silence notification of no default logging handler
log = logging.getLogger("caldav")


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


log.addHandler(NullHandler())
