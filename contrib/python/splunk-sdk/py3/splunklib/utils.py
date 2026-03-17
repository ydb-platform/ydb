# Copyright © 2011-2024 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""The **splunklib.utils** File for utility functions."""


def ensure_binary(s, encoding="utf-8", errors="strict"):
    """
    - `str` -> encoded to `bytes`
    - `bytes` -> `bytes`
    """
    if isinstance(s, str):
        return s.encode(encoding, errors)

    if isinstance(s, bytes):
        return s

    raise TypeError(f"not expecting type '{type(s)}'")


def ensure_str(s, encoding="utf-8", errors="strict"):
    """
    - `str` -> `str`
    - `bytes` -> decoded to `str`
    """
    if isinstance(s, bytes):
        return s.decode(encoding, errors)

    if isinstance(s, str):
        return s

    raise TypeError(f"not expecting type '{type(s)}'")


def assertRegex(self, *args, **kwargs):
    return getattr(self, "assertRegex")(*args, **kwargs)
