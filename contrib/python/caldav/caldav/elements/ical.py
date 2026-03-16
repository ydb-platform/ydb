#!/usr/bin/env python
from typing import ClassVar

from .base import ValuedBaseElement
from caldav.lib.namespace import ns


# Properties
class CalendarColor(ValuedBaseElement):
    tag: ClassVar[str] = ns("I", "calendar-color")


class CalendarOrder(ValuedBaseElement):
    tag: ClassVar[str] = ns("I", "calendar-order")
