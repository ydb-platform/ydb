# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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


# type: ignore

# This file exists for the sole reason of making mypy not
# complain about type issues to do with 'aiohttp' and 'yarl'.
# We're in a catch-22 situation:
# - If we use 'type: ignore' on 'import aiohttp' and it's not installed
#   mypy will complain that the annotation is unnecessary.
# - If we don't use 'type: ignore' on 'import aiohttp' and it
#   it's not installed mypy will complain that it can't find
#   type hints for aiohttp.
# So to make mypy happy we move all our 'extra' imports here
# and add a global 'type: ignore' which mypy never complains
# about being unnecessary.

import aiohttp
import aiohttp.client_exceptions as aiohttp_exceptions

# We do this because we don't explicitly require 'yarl'
# within our [async] extra any more.
# See AIOHttpConnection.request() for more information why.
try:
    import yarl
except ImportError:
    yarl = False

__all__ = ["aiohttp", "aiohttp_exceptions", "yarl"]
