# -*- coding: utf-8 -*-
# Copyright: (c) 2021, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import warnings

from spnego._sspi import *

warnings.warn("spnego.sspi is deprecated and will be removed in a future release.", DeprecationWarning)
