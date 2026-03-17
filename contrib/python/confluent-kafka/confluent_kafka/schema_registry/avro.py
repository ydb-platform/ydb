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

from io import BytesIO
from json import loads
from struct import pack, unpack

from fastavro import (parse_schema,
                      schemaless_reader,
                      schemaless_writer)

from . import (_MAGIC_BYTE,
               Schema,
               topic_subject_name_strategy)
from confluent_kafka.serialization import (Deserializer,
                                           SerializationError,
                                           Serializer)


class _ContextStringIO(BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


def _schema_loads(schema_str):
    """
    Instantiate a Schema instance from a declaration string.

    Args:
        schema_str (str): Avro Schema declaration.

    .. _Schema declaration:
        https://avro.apache.org/docs/current/spec.html#schemas

    Returns:
        Schema: A Schema instance.
    """

    schema_str = schema_str.strip()

    # canonical form primitive declarations are not supported
    if schema_str[0] != "{" and schema_str[0] != "[":
        schema_str = '{"type":' + schema_str + '}'

    return Schema(schema_str, schema_type='AVRO')


def _resolve_named_schema(schema, schema_registry_client, named_schemas=None):
    """
    Resolves named schemas referenced by the provided schema recursively.
    :param schema: Schema to resolve named schemas for.
    :param schema_registry_client: SchemaRegistryClient to use for retrieval.
    :param named_schemas: Dict of named schemas resolved recursively.
    :return: named_schemas dict.
    """
    if named_schemas is None:
        named_schemas = {}
    if schema.references is not None:
        for ref in schema.references:
            referenced_schema = schema_registry_client.get_version(ref.subject, ref.version)
            _resolve_named_schema(referenced_schema.schema, schema_registry_client, named_schemas)
            parse_schema(loads(referenced_schema.schema.schema_str), named_schemas=named_schemas)
    return named_schemas


class AvroSerializer(Serializer):
    """
    Serializer that outputs Avro binary encoded data with Confluent Schema Registry framing.

    Configuration properties:

    +---------------------------+----------+--------------------------------------------------+
    | Property Name             | Type     | Description                                      |
    +===========================+==========+==================================================+
    |                           |          | If True, automatically register the configured   |
    | ``auto.register.schemas`` | bool     | schema with Confluent Schema Registry if it has  |
    |                           |          | not previously been associated with the relevant |
    |                           |          | subject (determined via subject.name.strategy).  |
    |                           |          |                                                  |
    |                           |          | Defaults to True.                                |
    +---------------------------+----------+--------------------------------------------------+
    |                           |          | Whether to normalize schemas, which will         |
    | ``normalize.schemas``     | bool     | transform schemas to have a consistent format,   |
    |                           |          | including ordering properties and references.    |
    +---------------------------+----------+--------------------------------------------------+
    |                           |          | Whether to use the latest subject version for    |
    | ``use.latest.version``    | bool     | serialization.                                   |
    |                           |          |                                                  |
    |                           |          | WARNING: There is no check that the latest       |
    |                           |          | schema is backwards compatible with the object   |
    |                           |          | being serialized.                                |
    |                           |          |                                                  |
    |                           |          | Defaults to False.                               |
    +---------------------------+----------+--------------------------------------------------+
    |                           |          | Callable(SerializationContext, str) -> str       |
    |                           |          |                                                  |
    | ``subject.name.strategy`` | callable | Defines how Schema Registry subject names are    |
    |                           |          | constructed. Standard naming strategies are      |
    |                           |          | defined in the confluent_kafka.schema_registry   |
    |                           |          | namespace.                                       |
    |                           |          |                                                  |
    |                           |          | Defaults to topic_subject_name_strategy.         |
    +---------------------------+----------+--------------------------------------------------+

    Schemas are registered against subject names in Confluent Schema Registry that
    define a scope in which the schemas can be evolved. By default, the subject name
    is formed by concatenating the topic name with the message field (key or value)
    separated by a hyphen.

    i.e. {topic name}-{message field}

    Alternative naming strategies may be configured with the property
    ``subject.name.strategy``.

    Supported subject name strategies:

    +--------------------------------------+------------------------------+
    | Subject Name Strategy                | Output Format                |
    +======================================+==============================+
    | topic_subject_name_strategy(default) | {topic name}-{message field} |
    +--------------------------------------+------------------------------+
    | topic_record_subject_name_strategy   | {topic name}-{record name}   |
    +--------------------------------------+------------------------------+
    | record_subject_name_strategy         | {record name}                |
    +--------------------------------------+------------------------------+

    See `Subject name strategy <https://docs.confluent.io/current/schema-registry/serializer-formatter.html#subject-name-strategy>`_ for additional details.

    Note:
        Prior to serialization, all values must first be converted to
        a dict instance. This may handled manually prior to calling
        :py:func:`Producer.produce()` or by registering a `to_dict`
        callable with AvroSerializer.

        See ``avro_producer.py`` in the examples directory for example usage.

    Note:
       Tuple notation can be used to determine which branch of an ambiguous union to take.

       See `fastavro notation <https://fastavro.readthedocs.io/en/latest/writer.html#using-the-tuple-notation-to-specify-which-branch-of-a-union-to-take>`_

    Args:
        schema_registry_client (SchemaRegistryClient): Schema Registry client instance.

        schema_str (str or Schema):
            Avro `Schema Declaration. <https://avro.apache.org/docs/current/spec.html#schemas>`_
            Accepts either a string or a :py:class:`Schema` instance. Note that string
            definitions cannot reference other schemas. For referencing other schemas,
            use a :py:class:`Schema` instance.

        to_dict (callable, optional): Callable(object, SerializationContext) -> dict. Converts object to a dict.

        conf (dict): AvroSerializer configuration.
    """  # noqa: E501
    __slots__ = ['_hash', '_auto_register', '_normalize_schemas', '_use_latest_version',
                 '_known_subjects', '_parsed_schema',
                 '_registry', '_schema', '_schema_id', '_schema_name',
                 '_subject_name_func', '_to_dict', '_named_schemas']

    _default_conf = {'auto.register.schemas': True,
                     'normalize.schemas': False,
                     'use.latest.version': False,
                     'subject.name.strategy': topic_subject_name_strategy}

    def __init__(self, schema_registry_client, schema_str, to_dict=None, conf=None):
        if isinstance(schema_str, str):
            schema = _schema_loads(schema_str)
        elif isinstance(schema_str, Schema):
            schema = schema_str
        else:
            raise TypeError('You must pass either schema string or schema object')

        self._registry = schema_registry_client
        self._schema_id = None
        self._known_subjects = set()

        if to_dict is not None and not callable(to_dict):
            raise ValueError("to_dict must be callable with the signature "
                             "to_dict(object, SerializationContext)->dict")

        self._to_dict = to_dict

        conf_copy = self._default_conf.copy()
        if conf is not None:
            conf_copy.update(conf)

        self._auto_register = conf_copy.pop('auto.register.schemas')
        if not isinstance(self._auto_register, bool):
            raise ValueError("auto.register.schemas must be a boolean value")

        self._normalize_schemas = conf_copy.pop('normalize.schemas')
        if not isinstance(self._normalize_schemas, bool):
            raise ValueError("normalize.schemas must be a boolean value")

        self._use_latest_version = conf_copy.pop('use.latest.version')
        if not isinstance(self._use_latest_version, bool):
            raise ValueError("use.latest.version must be a boolean value")
        if self._use_latest_version and self._auto_register:
            raise ValueError("cannot enable both use.latest.version and auto.register.schemas")

        self._subject_name_func = conf_copy.pop('subject.name.strategy')
        if not callable(self._subject_name_func):
            raise ValueError("subject.name.strategy must be callable")

        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}"
                             .format(", ".join(conf_copy.keys())))

        schema_dict = loads(schema.schema_str)
        self._named_schemas = _resolve_named_schema(schema, schema_registry_client)
        parsed_schema = parse_schema(schema_dict, named_schemas=self._named_schemas)

        if isinstance(parsed_schema, list):
            # if parsed_schema is a list, we have an Avro union and there
            # is no valid schema name. This is fine because the only use of
            # schema_name is for supplying the subject name to the registry
            # and union types should use topic_subject_name_strategy, which
            # just discards the schema name anyway
            schema_name = None
        else:
            # The Avro spec states primitives have a name equal to their type
            # i.e. {"type": "string"} has a name of string.
            # This function does not comply.
            # https://github.com/fastavro/fastavro/issues/415
            schema_name = parsed_schema.get("name", schema_dict["type"])

        self._schema = schema
        self._schema_name = schema_name
        self._parsed_schema = parsed_schema

    def __call__(self, obj, ctx):
        """
        Serializes an object to Avro binary format, prepending it with Confluent
        Schema Registry framing.

        Args:
            obj (object): The object instance to serialize.

            ctx (SerializationContext): Metadata pertaining to the serialization operation.

        Raises:
            SerializerError: If any error occurs serializing obj.
            SchemaRegistryError: If there was an error registering the schema with
                                 Schema Registry, or auto.register.schemas is
                                 false and the schema was not registered.

        Returns:
            bytes: Confluent Schema Registry encoded Avro bytes
        """

        if obj is None:
            return None

        subject = self._subject_name_func(ctx, self._schema_name)

        if subject not in self._known_subjects:
            if self._use_latest_version:
                latest_schema = self._registry.get_latest_version(subject)
                self._schema_id = latest_schema.schema_id

            else:
                # Check to ensure this schema has been registered under subject_name.
                if self._auto_register:
                    # The schema name will always be the same. We can't however register
                    # a schema without a subject so we set the schema_id here to handle
                    # the initial registration.
                    self._schema_id = self._registry.register_schema(subject,
                                                                     self._schema,
                                                                     self._normalize_schemas)
                else:
                    registered_schema = self._registry.lookup_schema(subject,
                                                                     self._schema,
                                                                     self._normalize_schemas)
                    self._schema_id = registered_schema.schema_id
            self._known_subjects.add(subject)

        if self._to_dict is not None:
            value = self._to_dict(obj, ctx)
        else:
            value = obj

        with _ContextStringIO() as fo:
            # Write the magic byte and schema ID in network byte order (big endian)
            fo.write(pack('>bI', _MAGIC_BYTE, self._schema_id))
            # write the record to the rest of the buffer
            schemaless_writer(fo, self._parsed_schema, value)

            return fo.getvalue()


class AvroDeserializer(Deserializer):
    """
    Deserializer for Avro binary encoded data with Confluent Schema Registry
    framing.

    Note:
        By default, Avro complex types are returned as dicts. This behavior can
        be overriden by registering a callable ``from_dict`` with the deserializer to
        convert the dicts to the desired type.

        See ``avro_consumer.py`` in the examples directory in the examples
        directory for example usage.

    Args:
        schema_registry_client (SchemaRegistryClient): Confluent Schema Registry
            client instance.

        schema_str (str, Schema, optional): Avro reader schema declaration Accepts
            either a string or a :py:class:`Schema` instance. If not provided, the
            writer schema will be used as the reader schema. Note that string
            definitions cannot reference other schemas. For referencing other schemas,
            use a :py:class:`Schema` instance.

        from_dict (callable, optional): Callable(dict, SerializationContext) -> object.
            Converts a dict to an instance of some object.

        return_record_name (bool): If True, when reading a union of records, the result will
                                   be a tuple where the first value is the name of the record and the second value is
                                   the record itself.  Defaults to False.

    See Also:
        `Apache Avro Schema Declaration <https://avro.apache.org/docs/current/spec.html#schemas>`_

        `Apache Avro Schema Resolution <https://avro.apache.org/docs/1.8.2/spec.html#Schema+Resolution>`_
    """

    __slots__ = ['_reader_schema', '_registry', '_from_dict', '_writer_schemas', '_return_record_name', '_schema',
                 '_named_schemas']

    def __init__(self, schema_registry_client, schema_str=None, from_dict=None, return_record_name=False):
        schema = None
        if schema_str is not None:
            if isinstance(schema_str, str):
                schema = _schema_loads(schema_str)
            elif isinstance(schema_str, Schema):
                schema = schema_str
            else:
                raise TypeError('You must pass either schema string or schema object')

        self._schema = schema
        self._registry = schema_registry_client
        self._writer_schemas = {}

        if schema:
            schema_dict = loads(self._schema.schema_str)
            self._named_schemas = _resolve_named_schema(self._schema, schema_registry_client)
            self._reader_schema = parse_schema(schema_dict,
                                               named_schemas=self._named_schemas)
        else:
            self._named_schemas = None
            self._reader_schema = None

        if from_dict is not None and not callable(from_dict):
            raise ValueError("from_dict must be callable with the signature "
                             "from_dict(SerializationContext, dict) -> object")
        self._from_dict = from_dict

        self._return_record_name = return_record_name
        if not isinstance(self._return_record_name, bool):
            raise ValueError("return_record_name must be a boolean value")

    def __call__(self, data, ctx):
        """
        Deserialize Avro binary encoded data with Confluent Schema Registry framing to
        a dict, or object instance according to from_dict, if specified.

        Arguments:
            data (bytes): bytes

            ctx (SerializationContext): Metadata relevant to the serialization
                operation.

        Raises:
            SerializerError: if an error occurs parsing data.

        Returns:
            object: If data is None, then None. Else, a dict, or object instance according
                    to from_dict, if specified.
        """  # noqa: E501

        if data is None:
            return None

        if len(data) <= 5:
            raise SerializationError("Expecting data framing of length 6 bytes or "
                                     "more but total data size is {} bytes. This "
                                     "message was not produced with a Confluent "
                                     "Schema Registry serializer".format(len(data)))

        with _ContextStringIO(data) as payload:
            magic, schema_id = unpack('>bI', payload.read(5))
            if magic != _MAGIC_BYTE:
                raise SerializationError("Unexpected magic byte {}. This message "
                                         "was not produced with a Confluent "
                                         "Schema Registry serializer".format(magic))

            writer_schema = self._writer_schemas.get(schema_id, None)

            if writer_schema is None:
                registered_schema = self._registry.get_schema(schema_id)
                self._named_schemas = _resolve_named_schema(registered_schema, self._registry)
                prepared_schema = _schema_loads(registered_schema.schema_str)
                writer_schema = parse_schema(loads(
                    prepared_schema.schema_str), named_schemas=self._named_schemas)
                self._writer_schemas[schema_id] = writer_schema

            obj_dict = schemaless_reader(payload,
                                         writer_schema,
                                         self._reader_schema,
                                         self._return_record_name)

            if self._from_dict is not None:
                return self._from_dict(obj_dict, ctx)

            return obj_dict
