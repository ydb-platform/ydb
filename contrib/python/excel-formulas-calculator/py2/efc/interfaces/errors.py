# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from efc.base.errors import BaseEFCException


class BaseInterfaceError(BaseEFCException):
    code = 400


class NamedRangeNotFound(BaseInterfaceError):
    code = 401
    msg = 'Named range not found'
