# -*- coding: utf-8 -*-
"""ctypes wrapper for IVI-VISA library.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import os

from .highlevel import IVIVisaLibrary

WRAPPER_CLASS = IVIVisaLibrary

WRAP_HANDLER = True

env_value = os.environ.get("PYVISA_WRAP_HANDLER", None)
if env_value is not None:
    WRAP_HANDLER = bool(int(env_value))
