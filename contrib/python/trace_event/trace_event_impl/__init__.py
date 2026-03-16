# Copyright 2011 The Chromium Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.
import sys

# those two imports are needed to monkey-patch 'multiprocessing' module
if sys.version_info >= (3, 4):
    # in Python 3 we also need to monkey-patch 'multiprocessing.context' module
    from . import multiprocessing_context_shim
from . import multiprocessing_shim

from .decorators import *
from .log import *
