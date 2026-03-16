#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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
#
import json
import logging
import urllib
from collections import defaultdict
from threading import Lock

from requests import (Session,
                      utils)

from .error import SchemaRegistryError


# TODO: consider adding `six` dependency or employing a compat file
# Python 2.7 is officially EOL so compatibility issue will be come more the norm.
# We need a better way to handle these issues.
# Six is one possibility but the compat file pattern used by requests
# is also quite nice.
#
# six: https://pypi.org/project/six/
# compat file : https://github.com/psf/requests/blob/master/requests/compat.py
try:
    string_type = basestring  # noqa

    def _urlencode(value):
        return urllib.quote(value, safe='')
except NameError:
    string_type = str

    def _urlencode(value):
        return urllib.parse.quote(value, safe='')

log = logging.getLogger(__name__)
VALID_AUTH_PROVIDERS = ['URL', 'USER_INFO']


class _RestClient(object):
    """
    HTTP client for Confluent Schema Registry.

    See SchemaRegistryClient for configuration details.

    Args:
        conf (dict): Dictionary containing _RestClient configuration
    """

    def __init__(self, conf):
        self.session = Session()

        # copy dict to avoid mutating the original
        conf_copy = conf.copy()

        base_url = conf_copy.pop('url', None)
        if base_url is None:
            raise ValueError("Missing required configuration property url")
        if not isinstance(base_url, string_type):
            raise TypeError("url must be an instance of str, not "
                            + str(type(base_url)))
        if not base_url.startswith('http'):
            raise ValueError("Invalid url {}".format(base_url))
        self.base_url = base_url.rstrip('/')

        # The following configs map Requests Session class properties.
        # See the API docs for specifics.
        # https://requests.readthedocs.io/en/master/api/#request-sessions
        ca = conf_copy.pop('ssl.ca.location', None)
        if ca is not None:
            self.session.verify = ca

        key = conf_copy.pop('ssl.key.location', None)
        cert = conf_copy.pop('ssl.certificate.location', None)

        if cert is not None and key is not None:
            self.session.cert = (cert, key)

        if cert is not None and key is None:
            self.session.cert = cert

        if key is not None and cert is None:
            raise ValueError("ssl.certificate.location required when"
                             " configuring ssl.key.location")

        userinfo = utils.get_auth_from_url(base_url)
        if 'basic.auth.user.info' in conf_copy:
            if userinfo != ('', ''):
                raise ValueError("basic.auth.user.info configured with"
                                 " userinfo credentials in the URL."
                                 " Remove userinfo credentials from the url or"
                                 " remove basic.auth.user.info from the"
                                 " configuration")

            userinfo = tuple(conf_copy.pop('basic.auth.user.info', '').split(':', 1))

            if len(userinfo) != 2:
                raise ValueError("basic.auth.user.info must be in the form"
                                 " of {username}:{password}")

        self.session.auth = userinfo if userinfo != ('', '') else None

        # Any leftover keys are unknown to _RestClient
        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

    def _close(self):
        self.session.close()

    def get(self, url, query=None):
        return self.send_request(url, method='GET', query=query)

    def post(self, url, body, **kwargs):
        return self.send_request(url, method='POST', body=body)

    def delete(self, url):
        return self.send_request(url, method='DELETE')

    def put(self, url, body=None):
        return self.send_request(url, method='PUT', body=body)

    def send_request(self, url, method, body=None, query=None):
        """
        Sends HTTP request to the SchemaRegistry.

        All unsuccessful attempts will raise a SchemaRegistryError with the
        response contents. In most cases this will be accompanied with a
        Schema Registry supplied error code.

        In the event the response is malformed an error_code of -1 will be used.

        Args:
            url (str): Request path

            method (str): HTTP method

            body (str): Request content

            query (dict): Query params to attach to the URL

        Returns:
            dict: Schema Registry response content.
        """

        headers = {'Accept': "application/vnd.schemaregistry.v1+json,"
                             " application/vnd.schemaregistry+json,"
                             " application/json"}

        if body is not None:
            body = json.dumps(body)
            headers = {'Content-Length': str(len(body)),
                       'Content-Type': "application/vnd.schemaregistry.v1+json"}

        response = self.session.request(
            method, url="/".join([self.base_url, url]),
            headers=headers, data=body, params=query)

        try:
            if 200 <= response.status_code <= 299:
                return response.json()
            raise SchemaRegistryError(response.status_code,
                                      response.json().get('error_code'),
                                      response.json().get('message'))
        # Schema Registry may return malformed output when it hits unexpected errors
        except (ValueError, KeyError, AttributeError):
            raise SchemaRegistryError(response.status_code,
                                      -1,
                                      "Unknown Schema Registry Error: "
                                      + str(response.content))


class _SchemaCache(object):
    """
    Thread-safe cache for use with the Schema Registry Client.

    This cache may be used to retrieve schema ids, schemas or to check
    known subject membership.
    """

    def __init__(self):
        self.lock = Lock()
        self.schema_id_index = {}
        self.schema_index = {}
        self.subject_schemas = defaultdict(set)

    def set(self, schema_id, schema, subject_name=None):
        """
        Add a Schema identified by schema_id to the cache.

        Args:
            schema_id (int): Schema's registration id

            schema (Schema): Schema instance

            subject_name(str): Optional, subject schema is registered under
        """

        with self.lock:
            self.schema_id_index[schema_id] = schema
            self.schema_index[schema] = schema_id
            if subject_name is not None:
                self.subject_schemas[subject_name].add(schema)

    def remove_by_subject(self, subject_name):
        """
        Remove a Schema from the cache.

        Args:
            subject_name (str): Subject name the schema is registered under.
        """

        with self.lock:
            if subject_name in self.subject_schemas:
                for schema in self.subject_schemas[subject_name]:
                    schema_id = self.schema_index.get(schema, None)
                    if schema_id is not None:
                        self.schema_id_index.pop(schema_id, None)
                    self.schema_index.pop(Schema, None)

                del self.subject_schemas[subject_name]

    def get_schema(self, schema_id):
        """
        Get the schema instance associated with schema_id from the cache.

        Args:
            schema_id (int): Id used to identify a schema

        Returns:
            Schema: The schema if known; else None
        """

        with self.lock:
            return self.schema_id_index.get(schema_id, None)

    def get_schema_id_by_subject(self, subject, schema):
        """
        Get the schema_id associated with this schema registered under subject.

        Args:
            subject (str): The subject this schema is associated with

            schema (Schema): The schema associated with this schema_id

        Returns:
            int: Schema ID if known; else None
        """

        with self.lock:
            if schema in self.subject_schemas[subject]:
                return self.schema_index.get(schema, None)


class _RegisteredSchemaCache(object):
    """
    Thread-safe cache for use with the Schema Registry Client.

    This cache may be used to retrieve registered schemas based on subject_name/version/schema
    - Get registered schema based on subject name + version
    - Get registered schema based on subject name + schema
    """

    def __init__(self):
        self.lock = Lock()
        self.schema_version_index = defaultdict(dict)
        self.schema_index = defaultdict(dict)

    def set(self, subject_name, schema, version, registered_schema):
        """
        Add a Schema identified by schema_id to the cache.

        Args:
            subject_name (str): The subject name this registered schema is associated with

            schema (Schema): The schema this registered schema is associated with

            version (int): The version this registered schema is associated with

            registered_schema (RegisteredSchema): The registered schema instance
        """

        with self.lock:
            if schema is not None:
                self.schema_index[subject_name][schema] = registered_schema
            elif version is not None:
                self.schema_version_index[subject_name][version] = registered_schema

    def get_registered_schema_by_version(self, subject_name, version):
        """
        Get the registered schema instance associated with version from the cache.

        Args:
            subject_name (str): The subject name this registered schema is associated with

            version (int): The version this registered schema is associated with

        Returns:
            RegisteredSchema: The registered schema if known; else None
        """

        with self.lock:
            return self.schema_version_index.get(subject_name, {}).get(version, None)

    def get_registered_schema_by_schema(self, subject_name, schema):
        """
        Get the registered schema instance associated with schema from the cache.

        Args:
            subject_name (str): The subject name this registered schema is associated with

            schema (Schema): The schema this registered schema is associated with

        Returns:
            RegisteredSchema: The registered schema if known; else None
        """

        with self.lock:
            return self.schema_index.get(subject_name, {}).get(schema, None)


class SchemaRegistryClient(object):
    """
    A Confluent Schema Registry client.

    Configuration properties (* indicates a required field):

    +------------------------------+------+-------------------------------------------------+
    | Property name                | type | Description                                     |
    +==============================+======+=================================================+
    | ``url`` *                    | str  | Schema Registry URL.                            |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Path to CA certificate file used                |
    | ``ssl.ca.location``          | str  | to verify the Schema Registry's                 |
    |                              |      | private key.                                    |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Path to client's private key                    |
    |                              |      | (PEM) used for authentication.                  |
    | ``ssl.key.location``         | str  |                                                 |
    |                              |      | ``ssl.certificate.location`` must also be set.  |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Path to client's public key (PEM) used for      |
    |                              |      | authentication.                                 |
    | ``ssl.certificate.location`` | str  |                                                 |
    |                              |      | May be set without ssl.key.location if the      |
    |                              |      | private key is stored within the PEM as well.   |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Client HTTP credentials in the form of          |
    |                              |      | ``username:password``.                          |
    | ``basic.auth.user.info``     | str  |                                                 |
    |                              |      | By default userinfo is extracted from           |
    |                              |      | the URL if present.                             |
    +------------------------------+------+-------------------------------------------------+

    Args:
        conf (dict): Schema Registry client configuration.

    See Also:
        `Confluent Schema Registry documentation <http://confluent.io/docs/current/schema-registry/docs/intro.html>`_
    """  # noqa: E501

    def __init__(self, conf):
        self._rest_client = _RestClient(conf)
        self._cache = _SchemaCache()
        self._metadata_cache = _RegisteredSchemaCache()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._rest_client is not None:
            self._rest_client._close()

    def register_schema(self, subject_name, schema, normalize_schemas=False):
        """
        Registers a schema under ``subject_name``.

        Args:
            subject_name (str): subject to register a schema under

            schema (Schema): Schema instance to register

        Returns:
            int: Schema id

        Raises:
            SchemaRegistryError: if Schema violates this subject's
                Compatibility policy or is otherwise invalid.

        See Also:
            `POST Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        schema_id = self._cache.get_schema_id_by_subject(subject_name, schema)
        if schema_id is not None:
            return schema_id

        request = {'schema': schema.schema_str}

        # CP 5.5 adds new fields (for JSON and Protobuf).
        if len(schema.references) > 0 or schema.schema_type != 'AVRO':
            request['schemaType'] = schema.schema_type
            request['references'] = [{'name': ref.name,
                                      'subject': ref.subject,
                                      'version': ref.version}
                                     for ref in schema.references]

        response = self._rest_client.post(
            'subjects/{}/versions?normalize={}'.format(_urlencode(subject_name), normalize_schemas),
            body=request)

        schema_id = response['id']
        self._cache.set(schema_id, schema, subject_name)

        return schema_id

    def get_schema(self, schema_id):
        """
        Fetches the schema associated with ``schema_id`` from the
        Schema Registry. The result is cached so subsequent attempts will not
        require an additional round-trip to the Schema Registry.

        Args:
            schema_id (int): Schema id

        Returns:
            Schema: Schema instance identified by the ``schema_id``

        Raises:
            SchemaRegistryError: If schema can't be found.

        See Also:
         `GET Schema API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id>`_
        """  # noqa: E501

        schema = self._cache.get_schema(schema_id)
        if schema is not None:
            return schema

        response = self._rest_client.get('schemas/ids/{}'.format(schema_id))
        schema = Schema(schema_str=response['schema'],
                        schema_type=response.get('schemaType', 'AVRO'))

        schema.references = [
            SchemaReference(name=ref['name'], subject=ref['subject'], version=ref['version'])
            for ref in response.get('references', [])
        ]

        self._cache.set(schema_id, schema)

        return schema

    def lookup_schema(self, subject_name, schema, normalize_schemas=False):
        """
        Returns ``schema`` registration information for ``subject``.

        Args:
            subject_name (str): Subject name the schema is registered under

            schema (Schema): Schema instance.

        Returns:
            RegisteredSchema: Subject registration information for this schema.

        Raises:
            SchemaRegistryError: If schema or subject can't be found

        See Also:
            `POST Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        registered_schema = self._metadata_cache.get_registered_schema_by_schema(subject_name, schema)
        if registered_schema is not None:
            return registered_schema

        request = {'schema': schema.schema_str}

        # CP 5.5 adds new fields (for JSON and Protobuf).
        if len(schema.references) > 0 or schema.schema_type != 'AVRO':
            request['schemaType'] = schema.schema_type
            request['references'] = [{'name': ref.name,
                                      'subject': ref.subject,
                                      'version': ref.version}
                                     for ref in schema.references]

        response = self._rest_client.post('subjects/{}?normalize={}'
                                          .format(_urlencode(subject_name), normalize_schemas),
                                          body=request)

        schema_type = response.get('schemaType', 'AVRO')

        registered_schema = RegisteredSchema(
            schema_id=response['id'],
            schema=Schema(
                response['schema'],
                schema_type,
                [
                    SchemaReference(
                        name=ref['name'],
                        subject=ref['subject'],
                        version=ref['version']
                    ) for ref in response.get('references', [])
                ]
            ),
            subject=response['subject'],
            version=response['version']
        )
        self._metadata_cache.set(subject_name, schema, None, registered_schema)

        return registered_schema

    def get_subjects(self):
        """
        List all subjects registered with the Schema Registry

        Returns:
            list(str): Registered subject names

        Raises:
            SchemaRegistryError: if subjects can't be found

        See Also:
            `GET subjects API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        return self._rest_client.get('subjects')

    def delete_subject(self, subject_name, permanent=False):
        """
        Deletes the specified subject and its associated compatibility level if
        registered. It is recommended to use this API only when a topic needs
        to be recycled or in development environments.

        Args:
            subject_name (str): subject name
            permanent (bool): True for a hard delete, False (default) for a soft delete

        Returns:
            list(int): Versions deleted under this subject

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `DELETE Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)>`_
        """  # noqa: E501

        list = self._rest_client.delete('subjects/{}'
                                        .format(_urlencode(subject_name)))

        if permanent:
            self._rest_client.delete('subjects/{}?permanent=true'
                                     .format(_urlencode(subject_name)))

        self._cache.remove_by_subject(subject_name)

        return list

    def get_latest_version(self, subject_name):
        """
        Retrieves latest registered version for subject

        Args:
            subject_name (str): Subject name.

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.

        See Also:
            `GET Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        response = self._rest_client.get('subjects/{}/versions/{}'
                                         .format(_urlencode(subject_name),
                                                 'latest'))

        schema_type = response.get('schemaType', 'AVRO')
        return RegisteredSchema(schema_id=response['id'],
                                schema=Schema(response['schema'],
                                              schema_type,
                                              [
                                                  SchemaReference(name=ref['name'],
                                                                  subject=ref['subject'],
                                                                  version=ref['version'])
                                                  for ref in response.get('references', [])
                                              ]),
                                subject=response['subject'],
                                version=response['version'])

    def get_version(self, subject_name, version):
        """
        Retrieves a specific schema registered under ``subject_name``.

        Args:
            subject_name (str): Subject name.

            version (int): version number. Defaults to latest version.

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.

        See Also:
            `GET Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        registered_schema = self._metadata_cache.get_registered_schema_by_version(subject_name, version)
        if registered_schema is not None:
            return registered_schema

        response = self._rest_client.get('subjects/{}/versions/{}'
                                         .format(_urlencode(subject_name),
                                                 version))

        schema_type = response.get('schemaType', 'AVRO')
        registered_schema = RegisteredSchema(
            schema_id=response['id'],
            schema=Schema(
                response['schema'],
                schema_type,
                [
                    SchemaReference(
                        name=ref['name'],
                        subject=ref['subject'],
                        version=ref['version']
                    ) for ref in response.get('references', [])
                ]
            ),
            subject=response['subject'],
            version=response['version']
        )
        self._metadata_cache.set(subject_name, None, version, registered_schema)

        return registered_schema

    def get_versions(self, subject_name):
        """
        Get a list of all versions registered with this subject.

        Args:
            subject_name (str): Subject name.

        Returns:
            list(int): Registered versions

        Raises:
            SchemaRegistryError: If subject can't be found

        See Also:
            `GET Subject Versions API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        return self._rest_client.get('subjects/{}/versions'.format(_urlencode(subject_name)))

    def delete_version(self, subject_name, version):
        """
        Deletes a specific version registered to ``subject_name``.

        Args:
            subject_name (str) Subject name

            version (int): Version number

        Returns:
            int: Version number which was deleted

        Raises:
            SchemaRegistryError: if the subject or version cannot be found.

        See Also:
            `Delete Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        response = self._rest_client.delete('subjects/{}/versions/{}'.
                                            format(_urlencode(subject_name),
                                                   version))
        return response

    def set_compatibility(self, subject_name=None, level=None):
        """
        Update global or subject level compatibility level.

        Args:
            level (str): Compatibility level. See API reference for a list of
                valid values.

            subject_name (str, optional): Subject to update. Sets compatibility
                level policy if not set.

        Returns:
            str: The newly configured compatibility level.

        Raises:
            SchemaRegistryError: If the compatibility level is invalid.

        See Also:
            `PUT Subject Compatibility API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#put--config-(string-%20subject)>`_
        """  # noqa: E501

        if level is None:
            raise ValueError("level must be set")

        if subject_name is None:
            return self._rest_client.put('config',
                                         body={'compatibility': level.upper()})

        return self._rest_client.put('config/{}'
                                     .format(_urlencode(subject_name)),
                                     body={'compatibility': level.upper()})

    def get_compatibility(self, subject_name=None):
        """
        Get the current compatibility level.

        Args:
            subject_name (str, optional): Subject name. Returns global policy
                if left unset.

        Returns:
            str: Compatibility level for the subject if set, otherwise the global compatibility level.

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `GET Subject Compatibility API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--config-(string-%20subject)>`_
        """  # noqa: E501

        if subject_name is not None:
            url = 'config/{}'.format(_urlencode(subject_name))
        else:
            url = 'config'

        result = self._rest_client.get(url)
        return result['compatibilityLevel']

    def test_compatibility(self, subject_name, schema, version="latest"):
        """Test the compatibility of a candidate schema for a given subject and version

        Args:
            subject_name (str): Subject name the schema is registered under

            schema (Schema): Schema instance.

            version (int or str, optional): Version number, or the string "latest". Defaults to "latest".

        Returns:
            bool: True if the schema is compatible with the specified version

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `POST Test Compatibility API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--compatibility-subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        request = {"schema": schema.schema_str}
        if schema.schema_type != "AVRO":
            request['schemaType'] = schema.schema_type

        if schema.references:
            request['references'] = [
                {'name': ref.name, 'subject': ref.subject, 'version': ref.version}
                for ref in schema.references
            ]

        response = self._rest_client.post(
            'compatibility/subjects/{}/versions/{}'.format(subject_name, version), body=request
        )

        return response['is_compatible']


class Schema(object):
    """
    An unregistered Schema.

    Args:
        schema_str (str): String representation of the schema.

        schema_type (str): The schema type: AVRO, PROTOBUF or JSON.

        references ([SchemaReference]): SchemaReferences used in this schema.
    """
    __slots__ = ['schema_str', 'schema_type', 'references', '_hash']

    def __init__(self, schema_str, schema_type, references=[]):
        super(Schema, self).__init__()

        self.schema_str = schema_str
        self.schema_type = schema_type
        self.references = references
        self._hash = hash(schema_str)

    def __eq__(self, other):
        return all([self.schema_str == other.schema_str,
                    self.schema_type == other.schema_type])

    def __hash__(self):
        return self._hash


class RegisteredSchema(object):
    """
    Schema registration information.

    Represents a  Schema registered with a subject. Use this class when you need
    a specific version of a subject such as forming a SchemaReference.

    Args:
        schema_id (int): Registered Schema id

        schema (Schema): Registered Schema

        subject (str): Subject this schema is registered under

        version (int): Version of this subject this schema is registered to
    """

    def __init__(self, schema_id, schema, subject, version):
        self.schema_id = schema_id
        self.schema = schema
        self.subject = subject
        self.version = version


class SchemaReference(object):
    """
    Reference to a Schema registered with the Schema Registry.

    As of Confluent Platform 5.5 Schema's may now hold references to other
    registered schemas. As long as there is a references to a schema it may not
    be deleted.

    Args:
        name (str): Schema name

        subject (str): Subject this Schema is registered with

        version (int): This Schema's version
    """

    def __init__(self, name, subject, version):
        super(SchemaReference, self).__init__()
        self.name = name
        self.subject = subject
        self.version = version
