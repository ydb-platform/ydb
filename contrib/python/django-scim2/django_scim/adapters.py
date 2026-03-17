"""
Adapters are used to convert the data model described by the SCIM 2.0
specification to a data model that fits the data provided by the application
implementing a SCIM api.

For example, in a Django app, there are User and Group models that do
not have the same attributes/fields that are defined by the SCIM 2.0
specification. The Django User model has both ``first_name`` and ``last_name``
attributes but the SCIM speicifcation requires this same data be sent under
the names ``givenName`` and ``familyName`` respectively.

An adapter is instantiated with a model instance. Eg::

    user = get_user_model().objects.get(id=1)
    scim_user = SCIMUser(user)
    ...

"""
from typing import Optional, Union
from urllib.parse import urljoin

from django import core
from django.urls import reverse
from scim2_filter_parser.attr_paths import AttrPath

from . import constants, exceptions
from .utils import (
    get_base_scim_location_getter,
    get_group_adapter,
    get_group_filter_parser,
    get_user_adapter,
    get_user_filter_parser,
    get_user_model,
)


class SCIMMixin(object):

    ATTR_MAP = {}

    id_field = 'scim_id'  # Modifiable by overriding classes

    def __init__(self, obj, request=None):
        self.obj = obj
        self._request = request

    @property
    def request(self):
        if self._request:
            return self._request

        raise RuntimeError('Adapter is not associated with a request object. '
                           'Set object.request to avoid this error.')

    @request.setter
    def request(self, value):
        self._request = value

    @property
    def id(self):
        return str(getattr(self.obj, self.id_field))

    @property
    def path(self):
        return reverse(self.url_name, kwargs={'uuid': self.id})

    @property
    def location(self):
        return urljoin(get_base_scim_location_getter()(self.request), self.path)

    def to_dict(self):
        """
        Return a ``dict`` conforming to the object's SCIM Schema,
        ready for conversion to a JSON object.
        """
        d = {
            'id': self.id,
            'externalId': self.obj.scim_external_id,
        }

        return d

    def validate_dict(self, d):
        """
        Validate dict from SCIM call.

        Currently this method only validates:
            - the most common attributes
            - attributes against their expected types
        """
        for key, value in d.items():
            expected_type = {
                'active': bool,
            }.get(key)

            if expected_type and not isinstance(value, expected_type):
                raise exceptions.BadRequestError(
                    f'''"{key}" should be of type "{expected_type.__name__}". '''
                    f'''Got type "{type(value).__name__}"'''
                )

    def from_dict(self, d):
        """
        Consume a ``dict`` conforming to the object's SCIM Schema, updating the
        internal object with data from the ``dict``.

        This method is overridden and called by subclass adapters. Please make
        changes there.
        """
        scim_external_id = d.get('externalId')
        self.obj.scim_external_id = scim_external_id or ''

    def save(self):
        self.obj.save()

    def delete(self):
        self.obj.__class__.objects.filter(id=self.id).delete()

    def handle_operations(self, operations):
        """
        The SCIM specification allows for making changes to specific attributes
        of a model. These changes are sent in PATCH requests and are batched into
        operations to be performed on a object. Operations can have an op code
        of 'add', 'remove', or 'replace'. This method iterates through all of the
        operations in ``operations`` and calls the appropriate handler (defined
        on the appropriate adapter) for each.

        Django-scim2 only provides a partial implementation of PATCH call
        handlers. The RFC (https://tools.ietf.org/html/rfc7644#section-3.5.2)
        specifies a number of requirements for a full PATCH implementation.
        This implementation does not meet all of those requirements. For
        example, these are some features that have been left out.

        Add Operations:
            - If the target location does not exist, the attribute and value
              are added.
        Remove Operations:
            - If the target location is a multi-valued attribute and a complex
              filter is specified comparing a "value", the values matched by the
              filter are removed.  If no other values remain after removal of
              the selected values, the multi-valued attribute SHALL be
              considered unassigned.
        Replace Operations:
            - If the target location path specifies an attribute that does not
              exist, the service provider SHALL treat the operation as an "add".
        """
        for operation in operations:
            path = operation.get('path')
            value = operation.get('value')

            paths_and_values = self.parse_path_and_values(path, value)

            for path, value in paths_and_values:
                self.handle_path_and_value(path, value, operation)

    def handle_path_and_value(self,
                              path: AttrPath,
                              value: Union[str, list, dict],
                              operation: dict):
        op_code = operation.get('op').lower()

        if op_code not in constants.VALID_PATCH_OPS:
            raise exceptions.BadRequestError(f'Unknown PATCH op "{op_code}"')

        if op_code == 'remove' and not path:
            msg = '"path" must be specified during "remove" PATCH calls'
            raise exceptions.BadRequestError(msg, scim_type='noTarget')

        validate_method = 'validate_op_' + op_code
        handler = getattr(self, validate_method, self._default_validate_op)
        if handler:
            handler(path, value, operation)

        handle_method = 'handle_' + op_code
        handler = getattr(self, handle_method)
        handler(path, value, operation)

    def _default_validate_op(self,
                             path: Optional[AttrPath],
                             value: Union[str, list, dict],
                             operation: dict):
        """
        Validate the operation.

        Currently this method only validates:
            - the most common attributes
            - simple paths
            - attributes against their expected types
        """
        expected_type = None
        if path and not path.is_complex:
            expected_type = {
                ('active', None, None): bool,
            }.get(path.first_path)

        if expected_type and not isinstance(value, expected_type):
            raise exceptions.BadRequestError(
                f'''"{operation['path']}" should be of type "{expected_type.__name__}". '''
                f'''Got type "{type(value).__name__}"'''
            )

    def parse_path_and_values(self,
                              path: Optional[str],
                              value: Union[str, list, dict]) -> list:
        """
        Return new paths and values given original paths and values.

        This method can be overridden to provide a more usable path and value
        within the associated handle methods.
        """
        paths_and_values = []
        # Convert all path's to AttrPath objects in preparation for
        # use of scim2-filter-parser. Complex paths can path through as the
        # logic to handle them is not in place yet.
        if not path:
            if not isinstance(value, dict):
                raise ValueError('No path and operation value is a non-dict. Can not determine attribute.')

            # If there is no path and value is a dict, we assume that each
            # key in the dict is an attribute path. Let's convert attribute
            # paths to AttrPath objects to have a uniform API.
            for path, value in value.items():
                new_path = self.split_path(path)
                new_value = value
                paths_and_values.append((new_path, new_value))

        else:
            new_path = self.split_path(path)
            new_value = value
            paths_and_values.append((new_path, new_value))

        return paths_and_values

    def split_path(self, path: str) -> AttrPath:
        """
        Convert string path to an AttrPath object if possible.

        An AttrPath can be complex. Eg::

            - "addresses[type eq "work"]"
            - "members[value eq "123"].displayName"
            - "emails[type eq "work" and value co "@example.com"].value"

        It's up to the handlers to reject, ignore, handle requests with
        these types of paths. Handling them is above and beyond what
        the maintainer has time for.
        """
        # AttrPath requires a complete filter query. Thus we tack on
        # ' eq ""' to path to make a complete SCIM query.
        filter_ = path + ' eq ""'
        attr_path = AttrPath(filter_, self.ATTR_MAP)

        if not list(attr_path):
            msg = 'No attribute path found in request'
            raise exceptions.BadRequestError(msg)

        return attr_path

    def handle_add(self,
                   path: AttrPath,
                   value: Union[str, list, dict],
                   operation: dict):
        """
        Handle add operations per:
        https://tools.ietf.org/html/rfc7644#section-3.5.2.1
        """
        raise exceptions.NotImplementedError

    def handle_remove(self,
                      path: AttrPath,
                      value: Union[str, list, dict],
                      operation: dict):
        """
        Handle remove operations per:
        https://tools.ietf.org/html/rfc7644#section-3.5.2.2
        """
        raise exceptions.NotImplementedError

    def handle_replace(self,
                       path: AttrPath,
                       value: Union[str, list, dict],
                       operation: dict):
        """
        Handle replace operations per:
        https://tools.ietf.org/html/rfc7644#section-3.5.2.3
        """
        raise exceptions.NotImplementedError


class SCIMUser(SCIMMixin):
    """
    Adapter for adding SCIM functionality to a Django User object.

    This adapter can be overridden; see the ``USER_ADAPTER`` setting
    for details.
    """
    # not great, could be more decoupled. But \__( )__/ whatevs.
    url_name = 'scim:users'
    resource_type = 'User'

    ATTR_MAP = get_user_filter_parser().attr_map

    @property
    def display_name(self):
        """
        Return the displayName of the user per the SCIM spec.
        """
        if self.obj.first_name and self.obj.last_name:
            return u'{0.first_name} {0.last_name}'.format(self.obj)
        return self.obj.username

    @property
    def name_formatted(self):
        return self.display_name

    @property
    def emails(self):
        """
        Return the email of the user per the SCIM spec.
        """
        return [{'value': self.obj.email, 'primary': True}]

    @property
    def groups(self):
        """
        Return the groups of the user per the SCIM spec.
        """
        group_qs = self.obj.scim_groups.all()
        scim_groups = [get_group_adapter()(g, self.request) for g in group_qs]

        dicts = []
        for group in scim_groups:
            d = {
                'value': group.id,
                '$ref': group.location,
                'display': group.display_name,
            }
            dicts.append(d)

        return dicts

    @property
    def meta(self):
        """
        Return the meta object of the user per the SCIM spec.
        """
        d = {
            'resourceType': self.resource_type,
            'created': self.obj.date_joined.isoformat(),
            'lastModified': self.obj.date_joined.isoformat(),
            'location': self.location,
        }

        return d

    def to_dict(self):
        """
        Return a ``dict`` conforming to the SCIM User Schema,
        ready for conversion to a JSON object.
        """
        d = super().to_dict()
        d.update({
            'schemas': [constants.SchemaURI.USER],
            'userName': self.obj.username,
            'name': {
                'givenName': self.obj.first_name,
                'familyName': self.obj.last_name,
                'formatted': self.name_formatted,
            },
            'displayName': self.display_name,
            'emails': self.emails,
            'active': self.obj.is_active,
            'groups': self.groups,
            'meta': self.meta,
        })

        return d

    def from_dict(self, d):
        """
        Consume a ``dict`` conforming to the SCIM User Schema, updating the
        internal user object with data from the ``dict``.

        Please note, the user object is not saved within this method. To
        persist the changes made by this method, please call ``.save()`` on the
        adapter. Eg::

            scim_user.from_dict(d)
            scim_user.save()
        """
        super().from_dict(d)

        username = d.get('userName')
        self.obj.username = username or ''

        self.obj.scim_username = self.obj.username

        first_name = d.get('name', {}).get('givenName')
        self.obj.first_name = first_name or ''

        last_name = d.get('name', {}).get('familyName')
        self.obj.last_name = last_name or ''

        emails = d.get('emails', [])
        self.parse_emails(emails)

        cleartext_password = d.get('password')
        if cleartext_password:
            self.obj.set_password(cleartext_password)

        active = d.get('active')
        if active is not None:
            self.obj.is_active = active

    @classmethod
    def resource_type_dict(cls, request=None):
        """
        Return a ``dict`` containing ResourceType metadata for the user object.
        """
        id_ = cls.resource_type
        path = reverse('scim:resource-types', kwargs={'uuid': id_})
        location = urljoin(get_base_scim_location_getter()(request), path)
        return {
            'schemas': [constants.SchemaURI.RESOURCE_TYPE],
            'id': id_,
            'name': 'User',
            'endpoint': reverse('scim:users'),
            'description': 'User Account',
            'schema': constants.SchemaURI.USER,
            'meta': {
                'location': location,
                'resourceType': 'ResourceType'
            }
        }

    def parse_emails(self, value: Optional[list]):
        if value:
            email = None
            if isinstance(value, list):
                primary_emails = sorted(
                    (e for e in value if e.get('primary')),
                    key=lambda d: d.get('value')
                )
                secondary_emails = sorted(
                    (e for e in value if not e.get('primary')),
                    key=lambda d: d.get('value')
                )

                emails = primary_emails + secondary_emails
                if emails:
                    email = emails[0].get('value')
                else:
                    raise exceptions.BadRequestError('Invalid email value')

            elif isinstance(value, dict):
                # if value is a dict, let's assume it contains the primary email.
                # OneLogin sends a dict despite the spec:
                #   https://tools.ietf.org/html/rfc7643#section-4.1.2
                #   https://tools.ietf.org/html/rfc7643#section-8.2
                email = (value.get('value') or '').strip()

            self.validate_email(email)

            self.obj.email = email

    @staticmethod
    def validate_email(email):
        try:
            core.validators.EmailValidator()(email)
        except core.exceptions.ValidationError:
            raise exceptions.BadRequestError('Invalid email value')

    def handle_replace(self,
                       path: Optional[AttrPath],
                       value: Union[str, list, dict],
                       operation: dict):
        """
        Handle the replace operations.
        """
        if not isinstance(value, dict):
            # Restructure for use in loop below.
            value = {path: value}

        if not isinstance(value, dict):
            raise exceptions.NotImplementedError(
                f'PATCH replace operation with value type of '
                f'{type(value)} is not implemented'
            )

        for path, value in (value or {}).items():
            if path.first_path in self.ATTR_MAP:
                setattr(self.obj, self.ATTR_MAP.get(path.first_path), value)

            elif path.first_path == ('emails', None, None):
                self.parse_emails(value)

            else:
                raise exceptions.NotImplementedError('Not Implemented')

        self.save()


class SCIMGroup(SCIMMixin):
    """
    Adapter for adding SCIM functionality to a Django Group object.

    This adapter can be overridden; see the ``GROUP_ADAPTER``
    setting for details.
    """
    # not great, could be more decoupled. But \__( )__/ whatevs.
    url_name = 'scim:groups'
    resource_type = 'Group'

    ATTR_MAP = get_group_filter_parser().attr_map

    @property
    def display_name(self):
        """
        Return the displayName of the group per the SCIM spec.
        """
        return self.obj.name

    @property
    def members(self):
        """
        Return a list of user dicts (ready for serialization) for the members
        of the group.

        :rtype: list
        """
        users = self.obj.user_set.all()
        scim_users = [get_user_adapter()(user, self.request) for user in users]

        dicts = []
        for user in scim_users:
            d = {
                'value': user.id,
                '$ref': user.location,
                'display': user.display_name,
            }
            dicts.append(d)

        return dicts

    @property
    def meta(self):
        """
        Return the meta object of the group per the SCIM spec.
        """
        d = {
            'resourceType': self.resource_type,
            'location': self.location,
        }

        return d

    def to_dict(self):
        """
        Return a ``dict`` conforming to the SCIM Group Schema,
        ready for conversion to a JSON object.
        """
        d = super().to_dict()
        d.update({
            'schemas': [constants.SchemaURI.GROUP],
            'displayName': self.display_name,
            'members': self.members,
            'meta': self.meta,
        })
        return d

    def from_dict(self, d):
        """
        Consume a ``dict`` conforming to the SCIM Group Schema, updating the
        internal group object with data from the ``dict``.

        Please note, the group object is not saved within this method. To
        persist the changes made by this method, please call ``.save()`` on the
        adapter. Eg::

            scim_group.from_dict(d)
            scim_group.save()
        """
        super().from_dict(d)

        name = d.get('displayName')
        self.obj.name = name or ''

    @classmethod
    def resource_type_dict(cls, request=None):
        """
        Return a ``dict`` containing ResourceType metadata for the group object.
        """
        id_ = cls.resource_type
        path = reverse('scim:resource-types', kwargs={'uuid': id_})
        location = urljoin(get_base_scim_location_getter()(request), path)
        return {
            'schemas': [constants.SchemaURI.RESOURCE_TYPE],
            'id': id_,
            'name': 'Group',
            'endpoint': reverse('scim:groups'),
            'description': 'Group',
            'schema': constants.SchemaURI.GROUP,
            'meta': {
                'location': location,
                'resourceType': 'ResourceType'
            }
        }

    def handle_add(self, path, value, operation):
        """
        Handle add operations.
        """
        if path.first_path == ('members', None, None):
            members = value or []
            ids = [int(member.get('value')) for member in members]
            users = get_user_model().objects.filter(id__in=ids)

            if len(ids) != users.count():
                raise exceptions.BadRequestError('Can not add a non-existent user to group')

            for user in users:
                self.obj.user_set.add(user)

        else:
            raise exceptions.NotImplementedError

    def handle_remove(self, path, value, operation):
        """
        Handle remove operations.
        """
        if path.first_path == ('members', None, None):
            members = value or []
            ids = [int(member.get('value')) for member in members]
            users = get_user_model().objects.filter(id__in=ids)

            if len(ids) != users.count():
                raise exceptions.BadRequestError('Can not remove a non-existent user from group')

            for user in users:
                self.obj.user_set.remove(user)

        else:
            raise exceptions.NotImplementedError

    def handle_replace(self, path, value, operation):
        """
        Handle the replace operations.
        """
        if path.first_path == ('name', None, None):
            name = value[0].get('value')
            self.obj.name = name
            self.save()

        else:
            raise exceptions.NotImplementedError
