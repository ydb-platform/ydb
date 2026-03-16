# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Enter the names of the coroutines to be traced through the environment variable below, separated by commas.
"""

OTEL_PYTHON_ASYNCIO_COROUTINE_NAMES_TO_TRACE = (
    "OTEL_PYTHON_ASYNCIO_COROUTINE_NAMES_TO_TRACE"
)

"""
To determines whether the tracing feature for Future of Asyncio in Python is enabled or not.
"""
OTEL_PYTHON_ASYNCIO_FUTURE_TRACE_ENABLED = (
    "OTEL_PYTHON_ASYNCIO_FUTURE_TRACE_ENABLED"
)

"""
Enter the names of the functions to be traced through the environment variable below, separated by commas.
"""
OTEL_PYTHON_ASYNCIO_TO_THREAD_FUNCTION_NAMES_TO_TRACE = (
    "OTEL_PYTHON_ASYNCIO_TO_THREAD_FUNCTION_NAMES_TO_TRACE"
)
