#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
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


#
# derived from https://github.com/verisign/python-confluent-schemaregistry.git
#
import io
import json
import logging
import struct
import sys
import traceback

import avro
import avro.io

from confluent_kafka.avro import ClientError
from confluent_kafka.avro.serializer import (SerializerError,
                                             KeySerializerError,
                                             ValueSerializerError)

log = logging.getLogger(__name__)

MAGIC_BYTE = 0

HAS_FAST = False
try:
    from fastavro import schemaless_reader, schemaless_writer
    from fastavro.schema import parse_schema

    HAS_FAST = True
except ImportError:
    pass


class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False


class MessageSerializer(object):
    """
    A helper class that can serialize and deserialize messages
    that need to be encoded or decoded using the schema registry.

    All encode_* methods return a buffer that can be sent to kafka.
    All decode_* methods expect a buffer received from kafka.
    """

    def __init__(self, registry_client, reader_key_schema=None, reader_value_schema=None):
        self.registry_client = registry_client
        self.id_to_decoder_func = {}
        self.id_to_writers = {}
        self.reader_key_schema = reader_key_schema
        self.reader_value_schema = reader_value_schema

    # Encoder support
    def _get_encoder_func(self, writer_schema):
        if HAS_FAST:
            schema = json.loads(str(writer_schema))
            parsed_schema = parse_schema(schema)
            return lambda record, fp: schemaless_writer(fp, parsed_schema, record)
        writer = avro.io.DatumWriter(writer_schema)
        return lambda record, fp: writer.write(record, avro.io.BinaryEncoder(fp))

    def encode_record_with_schema(self, topic, schema, record, is_key=False):
        """
        Given a parsed avro schema, encode a record for the given topic.  The
        record is expected to be a dictionary.

        The schema is registered with the subject of 'topic-value'
        :param str topic: Topic name
        :param schema schema: Avro Schema
        :param dict record: An object to serialize
        :param bool is_key: If the record is a key
        :returns: Encoded record with schema ID as bytes
        :rtype: bytes
        """
        serialize_err = KeySerializerError if is_key else ValueSerializerError

        subject_suffix = ('-key' if is_key else '-value')
        # get the latest schema for the subject
        subject = topic + subject_suffix
        if self.registry_client.auto_register_schemas:
            # register it
            schema_id = self.registry_client.register(subject, schema)
        else:
            schema_id = self.registry_client.check_registration(subject, schema)
        if not schema_id:
            message = "Unable to retrieve schema id for subject %s" % (subject)
            raise serialize_err(message)

        # cache writer
        if schema_id not in self.id_to_writers:
            self.id_to_writers[schema_id] = self._get_encoder_func(schema)

        return self.encode_record_with_schema_id(schema_id, record, is_key=is_key)

    def encode_record_with_schema_id(self, schema_id, record, is_key=False):
        """
        Encode a record with a given schema id.  The record must
        be a python dictionary.
        :param int schema_id: integer ID
        :param dict record: An object to serialize
        :param bool is_key: If the record is a key
        :returns: decoder function
        :rtype: func
        """
        serialize_err = KeySerializerError if is_key else ValueSerializerError

        # use slow avro
        if schema_id not in self.id_to_writers:
            # get the writer + schema

            try:
                schema = self.registry_client.get_by_id(schema_id)
                if not schema:
                    raise serialize_err("Schema does not exist")
                self.id_to_writers[schema_id] = self._get_encoder_func(schema)
            except ClientError:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                raise serialize_err(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))

        # get the writer
        writer = self.id_to_writers[schema_id]
        with ContextStringIO() as outf:
            # Write the magic byte and schema ID in network byte order (big endian)
            outf.write(struct.pack('>bI', MAGIC_BYTE, schema_id))

            # write the record to the rest of the buffer
            writer(record, outf)

            return outf.getvalue()

    # Decoder support
    def _get_decoder_func(self, schema_id, payload, is_key=False):
        if schema_id in self.id_to_decoder_func:
            return self.id_to_decoder_func[schema_id]

        # fetch writer schema from schema reg
        try:
            writer_schema_obj = self.registry_client.get_by_id(schema_id)
        except ClientError as e:
            raise SerializerError("unable to fetch schema with id %d: %s" % (schema_id, str(e)))

        if writer_schema_obj is None:
            raise SerializerError("unable to fetch schema with id %d" % (schema_id))

        curr_pos = payload.tell()

        reader_schema_obj = self.reader_key_schema if is_key else self.reader_value_schema

        if HAS_FAST:
            # try to use fast avro
            try:
                fast_avro_writer_schema = parse_schema(json.loads(str(writer_schema_obj)))
                if reader_schema_obj is not None:
                    fast_avro_reader_schema = parse_schema(json.loads(str(reader_schema_obj)))
                else:
                    fast_avro_reader_schema = None
                schemaless_reader(payload, fast_avro_writer_schema)

                # If we reach this point, this means we have fastavro and it can
                # do this deserialization. Rewind since this method just determines
                # the reader function and we need to deserialize again along the
                # normal path.
                payload.seek(curr_pos)

                self.id_to_decoder_func[schema_id] = lambda p: schemaless_reader(
                    p, fast_avro_writer_schema, fast_avro_reader_schema)
                return self.id_to_decoder_func[schema_id]
            except Exception:
                log.warning("Fast avro failed for schema with id %d, falling thru to standard avro" % (schema_id))

        # here means we should just delegate to slow avro
        # rewind
        payload.seek(curr_pos)
        # Avro DatumReader py2/py3 inconsistency, hence no param keywords
        # should be revisited later
        # https://github.com/apache/avro/blob/master/lang/py3/avro/io.py#L459
        # https://github.com/apache/avro/blob/master/lang/py/src/avro/io.py#L423
        # def __init__(self, writers_schema=None, readers_schema=None)
        # def __init__(self, writer_schema=None, reader_schema=None)
        avro_reader = avro.io.DatumReader(writer_schema_obj, reader_schema_obj)

        def decoder(p):
            bin_decoder = avro.io.BinaryDecoder(p)
            return avro_reader.read(bin_decoder)

        self.id_to_decoder_func[schema_id] = decoder
        return self.id_to_decoder_func[schema_id]

    def decode_message(self, message, is_key=False):
        """
        Decode a message from kafka that has been encoded for use with
        the schema registry.
        :param str|bytes or None message: message key or value to be decoded
        :returns: Decoded message contents.
        :rtype dict:
        """

        if message is None:
            return None

        if len(message) <= 5:
            raise SerializerError("message is too small to decode")

        with ContextStringIO(message) as payload:
            magic, schema_id = struct.unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise SerializerError("message does not start with magic byte")
            decoder_func = self._get_decoder_func(schema_id, payload, is_key)
            return decoder_func(payload)
