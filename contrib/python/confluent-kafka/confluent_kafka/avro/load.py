#!/usr/bin/env python
#
# Copyright 2017 Confluent Inc.
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


from confluent_kafka.avro.error import ClientError


def loads(schema_str):
    """ Parse a schema given a schema string """
    try:
        return schema.parse(schema_str)
    except SchemaParseException as e:
        raise ClientError("Schema parse failed: %s" % (str(e)))


def load(fp):
    """ Parse a schema from a file path """
    with open(fp) as f:
        return loads(f.read())


# avro.schema.RecordSchema and avro.schema.PrimitiveSchema classes are not hashable. Hence defining them explicitly as
# a quick fix
def _hash_func(self):
    return hash(str(self))


try:
    from avro import schema

    try:
        # avro >= 1.11.0
        from avro.errors import SchemaParseException
    except ImportError:
        # avro < 1.11.0
        from avro.schema import SchemaParseException

    schema.RecordSchema.__hash__ = _hash_func
    schema.PrimitiveSchema.__hash__ = _hash_func
    schema.UnionSchema.__hash__ = _hash_func

except ImportError:
    schema = None
