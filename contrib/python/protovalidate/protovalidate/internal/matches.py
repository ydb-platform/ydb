# Copyright 2023-2025 Buf Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re

import celpy

# Patterns that are supported in Python's re package and not in re2.
# RE2: https://github.com/google/re2/wiki/syntax
invalid_patterns = [
    r"\\[1-9]",  # backreference
    r"\\k<\w+>",  # backreference
    r"\(\?\=",  # lookahead
    r"\(\?\!",  # negative lookahead
    r"\(\?\<\=",  # lookbehind
    r"\(\?\<\!",  # negative lookbehind
    r"\\c[A-Z]",  # control character
    r"\\u[0-9a-fA-F]{4}",  # UTF-16 code-unit
    r"\\0(?!\d)",  # NUL
    r"\[\\b.*\]",  # Backspace eg: [\b]
    r"\\Z",  # End of text (only lowercase z is supported in re2)
]


def matches(text: str, pattern: str) -> bool:
    """Return True if the given pattern matches text. False otherwise.

    CEL uses RE2 syntax which diverges from Python re in various ways. Ideally, we
    would use the google-re2 package, which is an extra dep in celpy, but at press
    time it does not provide a pre-built binary for the latest version of Python (3.13)
    which means those using this version will run into many issues.

    Instead of foisting this issue on users, we instead mimic re2 syntax by failing
    to compile the regex for patterns not compatible with re2.

    Users can choose to override this behavior by providing their own custom matches
    function via the Config.

    Raises:
        celpy.CELEvalError: If pattern contains invalid re2 syntax or if an re.error is raised during matching.
    """
    # Simulate re2 by failing on any patterns not compatible with re2 syntax
    for invalid_pattern in invalid_patterns:
        r = re.search(invalid_pattern, pattern)
        if r is not None:
            msg = f"error evaluating pattern {pattern}, invalid RE2 syntax"
            raise celpy.CELEvalError(msg)

    try:
        m = re.search(pattern, text)
    except re.error as ex:
        msg = "match error"
        raise celpy.CELEvalError(msg, ex.__class__, ex.args) from ex

    return m is not None
