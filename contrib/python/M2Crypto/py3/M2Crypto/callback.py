from __future__ import absolute_import

"""Deprecated, use the util module instead.

Copyright (c) 1999-2003 Ng Pheng Siong. All rights reserved."""

import warnings

warnings.warn('Use the util module instead', DeprecationWarning)

from M2Crypto.util import genparam_callback, passphrase_callback
