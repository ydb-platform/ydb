#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys


PY2 = sys.version_info[0] == 2

if PY2:
    text_type = unicode
    string_types = basestring,
else:
    text_type = str
    string_types = str,
