##############################################################################
#
# Copyright (c) 2023 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################

import importlib
import types
import unittest
from unittest.mock import patch

from zope.i18nmessageid import Message

from zope.schema import _messageid


class MessageIDTests(unittest.TestCase):

    def test_with_zope_i18nmessageid(self):
        # zope.i18nmessageid is installed while running tests, so `_`
        # returns instances of `Message` by default.
        message = _messageid._("test string")
        self.assertIsInstance(message, Message)
        self.assertEqual("zope", message.domain)
        self.assertEqual("test string", message)

    def test_without_zope_i18nmessageid(self):
        # If we pretend that zope.i18nmessageid is not installed, then `_`
        # falls back to returning instances of `str`.
        module = types.ModuleType("zope.i18nmessageid")
        try:
            with patch.dict("sys.modules", {"zope.i18nmessageid": module}):
                importlib.reload(_messageid)
                message = _messageid._("test string")
                self.assertNotIsInstance(message, Message)
                self.assertEqual("test string", message)
        finally:
            importlib.reload(_messageid)
