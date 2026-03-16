#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import asyncio

from ..compat import *  # noqa

# Hack supporting Python 3.6 asyncio which didn't have 'get_running_loop()'.
# Essentially we want to get away from having users pass in a loop to us.
# Instead we should call 'get_running_loop()' whenever we need
# the currently running loop.
# See: https://aiopg.readthedocs.io/en/stable/run_loop.html#implementation
try:
    from asyncio import get_running_loop
except ImportError:

    def get_running_loop():
        loop = asyncio.get_event_loop()
        if not loop.is_running():
            raise RuntimeError("no running event loop")
        return loop


__all__ = ["get_running_loop"]
