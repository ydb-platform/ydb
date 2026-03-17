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
import os
import warnings

import six

from pylxd import exceptions


MISSING = object()


class Attribute(object):
    """A metadata class for model attributes."""

    def __init__(self, validator=None, readonly=False, optional=False):
        self.validator = validator
        self.readonly = readonly
        self.optional = optional


class Manager(object):
    """A manager declaration.

    This class signals to the model that it will have a Manager
    attribute.
    """


class Parent(object):
    """A parent declaration.

    Child managers must keep a reference to their parent.
    """


class ModelType(type):
    """A Model metaclass.

    This metaclass converts the declarative Attribute style
    to attributes on the model instance itself.
    """

    def __new__(cls, name, bases, attrs):
        if '__slots__' in attrs and name != 'Model':  # pragma: no cover
            raise TypeError('__slots__ should not be specified.')
        attributes = {}
        for_removal = []
        managers = []

        for base in bases:
            if hasattr(base, '__attributes__'):
                attributes.update(base.__attributes__)

        for key, val in attrs.items():
            if type(val) == Attribute:
                attributes[key] = val
                for_removal.append(key)
            if type(val) in (Manager, Parent):
                managers.append(key)
                for_removal.append(key)
        for key in for_removal:
            del attrs[key]

        slots = list(attributes.keys())
        if '__slots__' in attrs:
            slots = slots + attrs['__slots__']
        for base in bases:
            if '__slots__' in dir(base):
                slots = slots + base.__slots__
        if len(managers) > 0:
            slots = slots + managers
        attrs['__slots__'] = slots
        attrs['__attributes__'] = attributes

        return super(ModelType, cls).__new__(cls, name, bases, attrs)


# Global used to record which warnings have been issues already for unknown
# attributes.
_seen_attribute_warnings = set()


@six.add_metaclass(ModelType)
class Model(object):
    """A Base LXD object model.

    Objects fetched from the LXD API have state, which allows
    the objects to be used transactionally, with E-tag support,
    and be smart about I/O.

    The model lifecycle is this: A model's get/create methods will
    return an instance. That instance may or may not be a partial
    instance. If it is a partial instance, `sync` will be called
    and the rest of the object retrieved from the server when
    un-initialized attributes are read. When attributes are modified,
    the instance is marked as dirty. `save` will save the changes
    to the server.

    If the LXD server sends attributes that this version of pylxd is unaware of
    then a warning is printed.  By default the warning is issued ONCE and then
    supressed for every subsequent attempted setting.  The warnings can be
    completely suppressed by setting the environment variable PYLXD_WARNINGS to
    'none', or always displayed by setting the PYLXD_WARNINGS variable to
    'always'.
    """
    NotFound = exceptions.NotFound
    __slots__ = ['client', '__dirty__']

    def __init__(self, client, **kwargs):
        self.__dirty__ = set()
        self.client = client

        for key, val in kwargs.items():
            try:
                setattr(self, key, val)
            except AttributeError:
                global _seen_attribute_warnings
                env = os.environ.get('PYLXD_WARNINGS', '').lower()
                item = "{}.{}".format(self.__class__.__name__, key)
                if env != 'always' and item in _seen_attribute_warnings:
                    continue
                _seen_attribute_warnings.add(item)
                if env == 'none':
                    continue
                warnings.warn(
                    'Attempted to set unknown attribute "{}" '
                    'on instance of "{}"'.format(
                        key, self.__class__.__name__
                    ))
        self.__dirty__.clear()

    def __getattribute__(self, name):
        try:
            return super(Model, self).__getattribute__(name)
        except AttributeError:
            if name in self.__attributes__:
                self.sync()
                return super(Model, self).__getattribute__(name)
            else:
                raise

    def __setattr__(self, name, value):
        if name in self.__attributes__:
            attribute = self.__attributes__[name]

            if attribute.validator is not None:
                if attribute.validator is not type(value):
                    value = attribute.validator(value)
            self.__dirty__.add(name)
        return super(Model, self).__setattr__(name, value)

    def __iter__(self):
        for attr in self.__attributes__.keys():
            yield attr, getattr(self, attr)

    @property
    def dirty(self):
        return len(self.__dirty__) > 0

    def sync(self, rollback=False):
        """Sync from the server.

        When collections of objects are retrieved from the server, they
        are often partial objects. The full object must be retrieved before
        it can modified. This method is called when getattr is called on
        a non-initaliazed object.
        """
        # XXX: rockstar (25 Jun 2016) - This has the potential to step
        # on existing attributes.
        response = self.api.get()
        payload = response.json()['metadata']
        for key, val in payload.items():
            if key not in self.__dirty__ or rollback:
                try:
                    setattr(self, key, val)
                except AttributeError:
                    # We have received an attribute from the server that we
                    # don't support in our model. Ignore this error, it
                    # doesn't hurt us.
                    pass

        # Make sure that *all* supported attributes are set, even those that
        # aren't supported by the server.
        missing_attrs = set(self.__attributes__.keys()) - set(payload.keys())
        for missing_attr in missing_attrs:
            setattr(self, missing_attr, MISSING)
        if rollback:
            self.__dirty__.clear()

    def rollback(self):
        """Reset the object from the server."""
        return self.sync(rollback=True)

    def save(self, wait=False):
        """Save data to the server.

        This method should write the new data to the server via marshalling.
        It should be a no-op when the object is not dirty, to prevent needless
        I/O.
        """
        marshalled = self.marshall()
        response = self.api.put(json=marshalled)

        if response.json()['type'] == 'async' and wait:
            self.client.operations.wait_for_operation(
                response.json()['operation'])
        self.__dirty__.clear()

    def delete(self, wait=False):
        """Delete an object from the server."""
        response = self.api.delete()

        if response.json()['type'] == 'async' and wait:
            self.client.operations.wait_for_operation(
                response.json()['operation'])
        self.client = None

    def marshall(self, skip_readonly=True):
        """Marshall the object in preparation for updating to the server."""
        marshalled = {}
        for key, attr in self.__attributes__.items():
            if attr.readonly and skip_readonly:
                continue
            if (not attr.optional) or (  # pragma: no branch
                    attr.optional and hasattr(self, key)):
                val = getattr(self, key)
                # Don't send back to the server an attribute it doesn't
                # support.
                if val is not MISSING:
                    marshalled[key] = val
        return marshalled

    def put(self, put_object, wait=False):
        """Access the PUT method directly for the object.

        This is to bypass the `save` method, and introduce a slightly saner
        approach of thinking about immuatable objects coming *from* the lXD
        server, and sending back PUTs and PATCHes.

        This method allows arbitrary puts to be attempted on the object (thus
        by passing the API attributes), but syncs the object overwriting any
        changes that may have been made to it.For a raw object return, see
        `raw_put`, which does not modify the object, and returns nothing.

        The `put_object` is the dictionary keys in the json object that is sent
        to the server for the API endpoint for the model.

        :param wait: If wait is True, then wait here until the operation
            completes.
        :type wait: bool
        :param put_object: jsonable dictionary to use as the PUT json object.
        :type put_object: dict
        :raises: :class:`pylxd.exception.LXDAPIException` on error
        """
        self.raw_put(put_object, wait)
        self.sync(rollback=True)

    def raw_put(self, put_object, wait=False):
        """Access the PUT method on the object direct, but with NO sync back.

        This accesses the PUT method for the object, but uses the `put_object`
        param to send as the JSON object.  It does NOT update the object
        afterwards, so it is effectively stale.  This is to allow a PUT when
        the object is no longer needed as it avoids another GET on the object.

        :param wait: If wait is True, then wait here until the operation
            completes.
        :type wait: bool
        :param put_object: jsonable dictionary to use as the PUT json object.
        :type put_object: dict
        :raises: :class:`pylxd.exception.LXDAPIException` on error
        """
        response = self.api.put(json=put_object)

        if response.json()['type'] == 'async' and wait:
            self.client.operations.wait_for_operation(
                response.json()['operation'])

    def patch(self, patch_object, wait=False):
        """Access the PATCH method directly for the object.

        This is to bypass the `save` method, and introduce a slightly saner
        approach of thinking about immuatable objects coming *from* the lXD
        server, and sending back PUTs and PATCHes.

        This method allows arbitrary patches to be attempted on the object
        (thus by passing the API attributes), but syncs the object overwriting
        any changes that may have been made to it.  For a raw object return,
        see `raw_patch`, which does not modify the object, and returns nothing.

        The `patch_object` is the dictionary keys in the json object that is
        sent to the server for the API endpoint for the model.

        :param wait: If wait is True, then wait here until the operation
            completes.
        :type wait: bool
        :param patch_object: jsonable dictionary to use as the PUT json object.
        :type patch_object: dict
        :raises: :class:`pylxd.exception.LXDAPIException` on error
        """
        self.raw_patch(patch_object, wait)
        self.sync(rollback=True)

    def raw_patch(self, patch_object, wait=False):
        """Access the PATCH method on the object direct, but with NO sync back.

        This accesses the PATCH method for the object, but uses the
        `patch_object` param to send as the JSON object.  It does NOT update
        the object afterwards, so it is effectively stale.  This is to allow a
        PATCH when the object is no longer needed as it avoids another GET on
        the object.

        :param wait: If wait is True, then wait here until the operation
            completes.
        :type wait: bool
        :param patch_object: jsonable dictionary to use as the PUT json object.
        :type patch_object: dict
        :raises: :class:`pylxd.exception.LXDAPIException` on error
        """
        response = self.api.patch(json=patch_object)

        if response.json()['type'] == 'async' and wait:
            self.client.operations.wait_for_operation(
                response.json()['operation'])
