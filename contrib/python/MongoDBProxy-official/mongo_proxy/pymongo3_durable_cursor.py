# -*- coding: utf-8 -*-
"""
Cursor that handles AutoReconnect, NetworkTimeout & NotMasterError problems
when iterating over values and replicate set elections happen.
(node crash or shutdown)

Copyright 2018 IQ Payments Oy

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import time
from pymongo.cursor import Cursor
from pymongo.errors import AutoReconnect
from pymongo.errors import NetworkTimeout
from pymongo.errors import NotMasterError

logger = logging.getLogger(__name__)
MAX_ATTEMPTS = 15
SLEEP_BETWEEN_RETRIES = 3


class TooManyRetries(Exception):
    """When we reach the limit, we raise this exception"""
    pass


class PyMongo3DurableCursor(Cursor):
    """Cursor that on AutoReconnect error waits and spawns a new Cursor,
    keeping track of previous location in iteration.
    """

    def __init__(self, *args, **kwargs):
        """Store original query args & kwargs for reuse if failure"""
        self.retry_args = args
        self.retry_kwargs = dict(kwargs)  # Copy values to keep original "skip"

        self.iterator_count = kwargs.pop('iterator_count', 0)
        self.retry_attempt = kwargs.pop('retry_attempt', 0)
        self.retry_cursor = None

        if self.iterator_count:
            kwargs['skip'] = kwargs.get('skip', 0) + self.iterator_count

        super(PyMongo3DurableCursor, self).__init__(*args, **kwargs)

    def next(self):
        """If Autoreconnect problems, wait and spawn new cursor,
        If new cursor already exists, pass the next() onto it.
        """
        if self.retry_cursor:
            return self.retry_cursor.next()

        try:
            next_item = super(PyMongo3DurableCursor, self).next()
            self.iterator_count += 1
            self.retry_attempt = 0  # Works (again), reset counter
            return next_item
        except (AutoReconnect, NetworkTimeout, NotMasterError) as exception:
            self.retry_attempt += 1
            if self.retry_attempt > MAX_ATTEMPTS:
                raise TooManyRetries('Failed too many times.')
            time.sleep(SLEEP_BETWEEN_RETRIES)
            logger.critical('Caught Exception: {}, spawning new cursor. '
                            'retry: {}/{}'.format(repr(exception),
                                                  self.retry_attempt,
                                                  MAX_ATTEMPTS))
            self.retry_kwargs['retry_attempt'] = self.retry_attempt
            self.retry_kwargs['iterator_count'] = self.iterator_count
            self.retry_cursor = PyMongo3DurableCursor(*self.retry_args,
                                                      **self.retry_kwargs)
            return self.retry_cursor.next()
