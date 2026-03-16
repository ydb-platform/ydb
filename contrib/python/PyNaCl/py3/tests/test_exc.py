# Copyright 2013 Donald Stufft and individual contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import pytest

from nacl import exceptions as exc


class CustomError(exc.CryptoError):
    pass


# Type safety: mypy can spot comparisons that will always evaluate to False, and the
# bad argument type. Suppress these: we want to test these are detected at runtime.


def test_exceptions_ensure_with_true_condition():
    exc.ensure(1 == 1, "one equals one")


def test_exceptions_ensure_with_false_condition():
    with pytest.raises(exc.AssertionError):
        exc.ensure(
            1 == 0,  # type: ignore[comparison-overlap]
            "one is not zero",
            raising=exc.AssertionError,
        )


def test_exceptions_ensure_with_unwanted_kwarg():
    with pytest.raises(exc.TypeError):
        exc.ensure(
            1 == 1,
            unexpected="unexpected",  # type: ignore[arg-type]
        )


def test_exceptions_ensure_custom_exception():
    with pytest.raises(CustomError):
        exc.ensure(
            1 == 0,  # type: ignore[comparison-overlap]
            "Raising a CustomError",
            raising=CustomError,
        )
