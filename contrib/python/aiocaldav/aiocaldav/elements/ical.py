#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from aiocaldav.lib.namespace import ns
from .base import BaseElement, ValuedBaseElement

# Properties


class CalendarColor(ValuedBaseElement):
    tag = ns("I", "calendar-color")


class CalendarOrder(ValuedBaseElement):
    tag = ns("I", "calendar-order")
