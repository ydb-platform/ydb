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

import sys
from typing import Callable, Tuple, Type, Union

PY2: bool
string_types: Tuple[type, ...]

to_str: Callable[[Union[str, bytes]], str]
to_bytes: Callable[[Union[str, bytes]], bytes]
reraise_exceptions: Tuple[Type[Exception], ...]

if sys.version_info[0] == 2:
    from itertools import imap as map
    from urllib import quote as quote
    from urllib import quote_plus as quote_plus
    from urllib import unquote as unquote
    from urllib import urlencode as urlencode

    from Queue import Queue as Queue
    from urlparse import urlparse as urlparse
else:
    from urllib.parse import quote as quote
    from urllib.parse import quote_plus as quote_plus
    from urllib.parse import unquote as unquote
    from urllib.parse import urlencode as urlencode
    from urllib.parse import urlparse as urlparse

    map = map
    from queue import Queue as Queue
