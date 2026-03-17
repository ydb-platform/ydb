# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from enum import Enum
from typing import Union, Callable, Optional, cast

if False:
    # Work around for MYPY for cyclic import problem
    from libcloud.compute.base import BaseDriver

__all__ = [
    "Type",
    "LibcloudError",
    "MalformedResponseError",
    "ProviderError",
    "InvalidCredsError",
    "InvalidCredsException",
    "LazyList",
]


class Type(str, Enum):
    @classmethod
    def tostring(cls, value):
        # type: (Union[Enum, str]) -> str
        """Return the string representation of the state object attribute
        :param str value: the state object to turn into string
        :return: the uppercase string that represents the state object
        :rtype: str
        """
        value = cast(Enum, value)
        return str(value._value_).upper()

    @classmethod
    def fromstring(cls, value):
        # type: (str) -> Optional[str]
        """Return the state object attribute that matches the string
        :param str value: the string to look up
        :return: the state object attribute that matches the string
        :rtype: str
        """
        return getattr(cls, value.upper(), None)

    """
    NOTE: These methods are here for backward compatibility reasons where
    Type values were simple strings and Type didn't inherit from Enum.
    """

    def __eq__(self, other):
        if isinstance(other, Type):
            return other.value == self.value
        elif isinstance(other, str):
            return self.value == other

        return super().__eq__(other)

    def upper(self):
        return self.value.upper()  # pylint: disable=no-member

    def lower(self):
        return self.value.lower()  # pylint: disable=no-member

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return self.value

    def __hash__(self):
        return hash(self.value)


class LibcloudError(Exception):
    """The base class for other libcloud exceptions"""

    def __init__(self, value, driver=None):
        # type: (str, Optional[BaseDriver]) -> None
        super().__init__(value)
        self.value = value
        self.driver = driver

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<LibcloudError in " + repr(self.driver) + " " + repr(self.value) + ">"


class MalformedResponseError(LibcloudError):
    """Exception for the cases when a provider returns a malformed
    response, e.g. you request JSON and provider returns
    '<h3>something</h3>' due to some error on their side."""

    def __init__(self, value, body=None, driver=None):
        # type: (str, Optional[str], Optional[BaseDriver]) -> None
        self.value = value
        self.driver = driver
        self.body = body

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return (
            "<MalformedResponseException in "
            + repr(self.driver)
            + " "
            + repr(self.value)
            + ">: "
            + repr(self.body)
        )


class ProviderError(LibcloudError):
    """
    Exception used when provider gives back
    error response (HTTP 4xx, 5xx) for a request.

    Specific sub types can be derived for errors like
    HTTP 401 : InvalidCredsError
    HTTP 404 : NodeNotFoundError, ContainerDoesNotExistError
    """

    def __init__(self, value, http_code, driver=None):
        # type: (str, int, Optional[BaseDriver]) -> None
        super().__init__(value=value, driver=driver)
        self.http_code = http_code

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return repr(self.value)


class InvalidCredsError(ProviderError):
    """Exception used when invalid credentials are used on a provider."""

    def __init__(self, value="Invalid credentials with the provider", driver=None):
        # type: (str, Optional[BaseDriver]) -> None
        # NOTE: We don't use http.client constants here since that adds ~20ms
        # import time overhead
        super().__init__(value, http_code=401, driver=driver)


# Deprecated alias of :class:`InvalidCredsError`
InvalidCredsException = InvalidCredsError


class ServiceUnavailableError(ProviderError):
    """Exception used when a provider returns 503 Service Unavailable."""

    def __init__(self, value="Service unavailable at provider", driver=None):
        # type: (str, Optional[BaseDriver]) -> None
        # NOTE: We don't use http.client constants here since that adds ~20ms
        # import time overhead
        super().__init__(value, http_code=503, driver=driver)


class LazyList:
    def __init__(self, get_more, value_dict=None):
        # type: (Callable, Optional[dict]) -> None
        self._data = []  # type: list
        self._last_key = None
        self._exhausted = False
        self._all_loaded = False
        self._get_more = get_more
        self._value_dict = value_dict or {}

    def __iter__(self):
        if not self._all_loaded:
            self._load_all()

        data = self._data
        yield from data

    def __getitem__(self, index):
        if index >= len(self._data) and not self._all_loaded:
            self._load_all()

        return self._data[index]

    def __len__(self):
        self._load_all()
        return len(self._data)

    def __repr__(self):
        self._load_all()
        repr_string = ", ".join([repr(item) for item in self._data])
        repr_string = "[%s]" % (repr_string)
        return repr_string

    def _load_all(self):
        while not self._exhausted:
            newdata, self._last_key, self._exhausted = self._get_more(
                last_key=self._last_key, value_dict=self._value_dict
            )
            self._data.extend(newdata)
        self._all_loaded = True
