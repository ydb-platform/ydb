#!/usr/bin/env python
"""
I got fed up with several thousand lines of code in one and the same file.

This file is by now just a backward compatibility layer.

Logic has been split out:

* DAVObject base class -> davobject.py
* CalendarObjectResource base class -> calendarobjectresource.py
* Event/Todo/Journal/FreeBusy -> calendarobjectresource.py
* Everything else (mostly collection objects) -> collection.py
"""
## For backward compatibility
from .calendarobjectresource import *
from .collection import *
from .davobject import *
