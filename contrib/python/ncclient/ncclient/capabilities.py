# Copyright 2009 Shikhar Bhushan
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging


logger = logging.getLogger("ncclient.capabilities")


def _abbreviate(uri):
    if uri.startswith("urn:ietf:params") and ":netconf:" in uri:
        splitted = uri.split(":")
        if ":capability:" in uri:
            if uri.startswith("urn:ietf:params:xml:ns:netconf"):
                name, version = splitted[7], splitted[8]
            else:
                name, version = splitted[5], splitted[6]
            return [ ":" + name, ":" + name + ":" + version ]
        elif ":base:" in uri:
            if uri.startswith("urn:ietf:params:xml:ns:netconf"):
                return [ ":base", ":base" + ":" + splitted[7] ]
            else:
                return [ ":base", ":base" + ":" + splitted[5] ]
    return []

def schemes(url_uri):
    "Given a URI that has a *scheme* query string (i.e. `:url` capability URI), will return a list of supported schemes."
    return url_uri.partition("?scheme=")[2].split(",")

class Capabilities:

    "Represents the set of capabilities available to a NETCONF client or server. It is initialized with a list of capability URI's."

    def __init__(self, capabilities):
        self._dict = {}
        for uri in capabilities:
            self.add(uri)

    def __contains__(self, key):
        try:
            self.__getitem__(key)
        except KeyError:
            return False
        else:
            return True

    def __getitem__(self, key):
        try:
            return self._dict[key]
        except KeyError:
            for capability in self._dict.values():
                if key in capability.get_abbreviations():
                    return capability

        raise KeyError(key)

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict.keys())

    def __repr__(self):
        return repr(self._dict.keys())

    def add(self, uri):
        "Add a capability."
        self._dict[uri] = Capability.from_uri(uri)

    def remove(self, uri):
        "Remove a capability."
        if uri in self._dict:
            del self._dict[uri]


class Capability:

    """Represents a single capability"""

    def __init__(self, namespace_uri, parameters=None):
        self.namespace_uri = namespace_uri
        self.parameters = parameters or {}

    @classmethod
    def from_uri(cls, uri):
        split_uri = uri.split("?")
        namespace_uri = split_uri[0]
        capability = cls(namespace_uri)

        try:
            param_string = split_uri[1]
        except IndexError:
            return capability

        capability.parameters = {
            param.key: param.value
            for param in _parse_parameter_string(param_string, uri)
        }

        return capability

    def __eq__(self, other):
        return (
            self.namespace_uri == other.namespace_uri and
            self.parameters == other.parameters
        )

    def get_abbreviations(self):
        return _abbreviate(self.namespace_uri)


def _parse_parameter_string(string, uri):
    for param_string in string.split("&"):
        try:
            yield _Parameter.from_string(param_string)
        except _InvalidParameter:
            logger.error(
                "Invalid parameter '{param}' in capability URI '{uri}'".format(
                    param=param_string,
                    uri=uri,
                )
            )


class _Parameter:

    """Represents a parameter to a capability"""

    def __init__(self, key, value):
        self.key = key
        self.value = value

    @classmethod
    def from_string(cls, string):
        try:
            key, value = string.split("=")
        except ValueError:
            raise _InvalidParameter

        return cls(key, value)


class _InvalidParameter(Exception):

    pass
