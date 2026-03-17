# Copyright (c) 2016 Canonical Ltd
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import warnings
import os

from pylxd import exceptions
from six.moves.urllib import parse


# Global used to record which warnings have been issues already for unknown
# attributes.
_seen_attribute_warnings = set()


class Operation(object):
    """An LXD operation.

    If the LXD server sends attributes that this version of pylxd is unaware of
    then a warning is printed.  By default the warning is issued ONCE and then
    supressed for every subsequent attempted setting.  The warnings can be
    completely suppressed by setting the environment variable PYLXD_WARNINGS to
    'none', or always displayed by setting the PYLXD_WARNINGS variable to
    'always'.
    """

    __slots__ = [
        '_client',
        'class', 'created_at', 'description', 'err', 'id', 'may_cancel',
        'metadata', 'resources', 'status', 'status_code', 'updated_at']

    @classmethod
    def wait_for_operation(cls, client, operation_id):
        """Get an operation and wait for it to complete."""
        operation = cls.get(client, operation_id)
        operation.wait()
        return cls.get(client, operation.id)

    @classmethod
    def extract_operation_id(cls, s):
        return os.path.split(parse.urlparse(s).path)[-1]

    @classmethod
    def get(cls, client, operation_id):
        """Get an operation."""
        operation_id = cls.extract_operation_id(operation_id)
        response = client.api.operations[operation_id].get()
        return cls(_client=client, **response.json()['metadata'])

    def __init__(self, **kwargs):
        super(Operation, self).__init__()
        for key, value in kwargs.items():
            try:
                setattr(self, key, value)
            except AttributeError:
                # ignore attributes we don't know about -- prevent breakage
                # in the future if new attributes are added.
                global _seen_attribute_warnings
                env = os.environ.get('PYLXD_WARNINGS', '').lower()
                if env != 'always' and key in _seen_attribute_warnings:
                    continue
                _seen_attribute_warnings.add(key)
                if env == 'none':
                    continue
                warnings.warn(
                    'Attempted to set unknown attribute "{}" '
                    'on instance of "{}"'
                    .format(key, self.__class__.__name__))
                pass

    def wait(self):
        """Wait for the operation to complete and return."""
        response = self._client.api.operations[self.id].wait.get()

        try:
            if response.json()['metadata']['status'] == 'Failure':
                raise exceptions.LXDAPIException(response)
        except KeyError:
            # Support for legacy LXD
            pass
