#
# Copyright (C) 2018 Martin Owens
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 3.0 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library.
#
"""
Provide some utilities to tests
"""
from logging import Handler, getLogger, root
from collections import defaultdict

# FLAG: do not report failures from here in tracebacks
# pylint: disable=invalid-name
__unittest = True

class LoggingRecorder(Handler):
    """Record any logger output for testing"""
    def __init__(self, *args, **kwargs):
        self.logs = defaultdict(list)
        super(LoggingRecorder, self).__init__(*args, **kwargs)

    def __getitem__(self, name):
        return self.logs[name.upper()]

    def emit(self, record):
        """Save the log message to the right level"""
        # We have no idea why record's getMessage is prefixed
        msg = str(record.getMessage())
        if msg.startswith('u"'):
            msg = msg[1:]
        if msg and (msg[0] == msg[-1] and msg[0] in '"\''):
            msg = msg[1:-1]
        self[record.levelname].append(msg)
        return True


class LoggingMixin(object):
    """Provide logger capture"""
    log_name = None

    def setUp(self):
        """Make a fresh logger for each test function"""
        super(LoggingMixin, self).setUp()
        named = getLogger(self.log_name)
        for handler in root.handlers[:]:
            root.removeHandler(handler)
        for handler in named.handlers[:]:
            named.removeHandler(handler)
        self.log_handler = LoggingRecorder(level='DEBUG')
        named.addHandler(self.log_handler)

    def tearDown(self):
        """Warn about untested logs"""
        for level in self.log_handler.logs:
            for msg in self.log_handler[level]:
                raise ValueError("Uncaught log: {}: {}\n".format(level, msg))

    def assertLog(self, level, msg):
        """Checks that the logger has emitted the given log"""
        logs = self.log_handler[level]
        self.assertTrue(logs, 'Logger hasn\'t emitted "{}"'.format(msg))
        if len(logs) == 1:
            self.assertEqual(msg, logs[0])
        else:
            self.assertIn(msg, logs)
        logs.remove(msg)

    def assertNoLog(self, level, msg):
        """Checks that the logger has NOT emitted the given log"""
        self.assertNotIn(msg, self.log_handler[level])
