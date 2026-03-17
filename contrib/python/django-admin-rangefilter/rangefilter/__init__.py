# -*- coding: utf-8 -*-

import django

__author__ = "Dmitriy Sokolov"
__version__ = "0.13.5"

if django.VERSION < (3, 2):
    default_app_config = "rangefilter.apps.RangeFilterConfig"


VERSION = __version__
