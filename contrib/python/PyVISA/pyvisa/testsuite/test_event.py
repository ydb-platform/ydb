# -*- coding: utf-8 -*-
"""Test events classes.

This file is part of PyVISA.

:copyright: 2019-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import logging

import pytest

from pyvisa import constants, errors
from pyvisa.events import Event

from . import BaseTestCase


class TestEvent(BaseTestCase):
    """Test Event functionalities."""

    def setup_method(self):
        self.old = Event._event_classes.copy()

    def teardown_method(self):
        Event._event_classes = self.old

    def test_register(self):
        assert Event._event_classes[constants.EventType.clear] is Event

    def test_double_register_event_cls(self, caplog):
        class SubEvent(Event):
            pass

        with caplog.at_level(logging.DEBUG, logger="pyvisa"):
            Event.register(constants.EventType.clear)(SubEvent)

        assert caplog.records

        assert Event._event_classes[constants.EventType.clear] is SubEvent

    def test_register_event_cls_missing_attr(self):
        class SubEvent(Event):
            pass

        with pytest.raises(TypeError):
            Event.register(constants.EventType.exception)(SubEvent)

        assert Event._event_classes[constants.EventType.exception] is not SubEvent

    def test_event_context(self):
        event = Event(None, constants.EventType.clear, 1)
        assert event.context == 1

        event.close()
        with pytest.raises(errors.InvalidSession):
            event.context
