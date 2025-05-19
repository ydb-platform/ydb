#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import random
import string

import six
from google.protobuf import text_format, json_format
from google.protobuf.pyext._message import FieldDescriptor

from library.python import resource
from ydb.tools.cfg import types


def random_string(length):
    choices = string.ascii_lowercase + string.digits
    return ''.join([random.choice(choices) for _ in range(length)])


def uuid():
    return '-'.join([random_string(length) for length in (8, 4, 4, 4, 12)])


def create_if_does_not_exists(output_dir):
    try:
        os.mkdir(output_dir)
    except OSError:
        pass


def message_to_string(message):
    return text_format.MessageToString(message, double_format='.15g')


def write_to_file(file_path, value):
    create_if_does_not_exists(os.path.dirname(file_path))

    value = types.py3_ensure_str(value)

    with open(file_path, 'w') as writer:
        writer.write(value)
        writer.write('\n')


def write_proto_to_file(file_path, proto):
    write_to_file(file_path, message_to_string(proto))


def read_message_from_string(value, message):
    text_format.Parse(value, message)
    return message


def get_resource_prefix(*args):
    return os.path.join('resources', *args)


def get_resource_data(*args):
    path = get_resource_prefix(*args)
    data = resource.find(path)
    if data is not None:
        return data
    raise RuntimeError('Cannot find resource %s' % (path,))


def read_from_resource(message, *args):
    for key, value in resource.iteritems(get_resource_prefix(*args)):
        return read_message_from_string(value, message)
    raise RuntimeError()


def get_resources_list(prefix=''):
    result = []
    for key in resource.iterkeys(prefix):
        result.append(key.replace(prefix, ''))
    return result


def capitalize_name(initial_name):
    return ''.join(map(lambda name_part: name_part.capitalize(), initial_name.split('_')))


def to_lower(s):
    return s.lower()


def get_canonical_name(name):
    chars = set(string.punctuation)
    return to_lower("".join(filter(lambda x: x not in chars, name)))


def apply_config_changes(target, changes, fix_names=None):
    fix_names = {} if fix_names is None else fix_names

    in_proto_field_names = {}
    in_proto_field_descriptor = {}
    for field in target.DESCRIPTOR.fields:
        in_proto_field_names[get_canonical_name(field.name)] = field.name
        in_proto_field_descriptor[get_canonical_name(field.name)] = field

    for change_name, change_value in changes.items():
        try:
            fixed_change_name = capitalize_name(change_name)
            fixed_change_name = fix_names.get(fixed_change_name)
            canonical_change_name = get_canonical_name(change_name)

            if fixed_change_name is None:
                fixed_change_name = in_proto_field_names.get(get_canonical_name(change_name))

            assert fixed_change_name is not None, "Cannot find suitable field %s, proto descriptor: %s" % (
                change_name,
                target.DESCRIPTOR.full_name,
            )

            if isinstance(change_value, list):
                for item in change_value:
                    if isinstance(item, dict):
                        item_proto = getattr(target, fixed_change_name).add()
                        apply_config_changes(item_proto, item, fix_names)
                    else:
                        getattr(target, fixed_change_name).append(item)
            elif isinstance(change_value, dict):
                apply_config_changes(getattr(target, fixed_change_name), change_value, fix_names)
            elif in_proto_field_descriptor.get(canonical_change_name) is not None:
                descriptor = in_proto_field_descriptor.get(canonical_change_name)
                if descriptor.enum_type:
                    if isinstance(change_value, six.integer_types):
                        value_desc = descriptor.enum_type.values_by_number[change_value]
                    else:
                        value_desc = descriptor.enum_type.values_by_name[change_value]
                    setattr(target, fixed_change_name, value_desc.number)
                else:
                    field_descriptor = target.DESCRIPTOR.fields_by_name[fixed_change_name]
                    if isinstance(change_value, str) and field_descriptor.type == FieldDescriptor.TYPE_BYTES:
                        change_value = types.py3_ensure_bytes(change_value)
                    setattr(target, fixed_change_name, change_value)
            else:
                setattr(target, fixed_change_name, change_value)
        except Exception as e:
            raise RuntimeError(
                "Cannot apply config change: change_name: %s, change_value %s. Reason: %s"
                % (
                    change_name,
                    str(change_value),
                    str(e),
                )
            )

    return target


def random_int(low, high, *seed):
    random.seed(''.join(map(str, seed)))
    return random.randint(low, high)


def wrap_parse_dict(dictionary, proto):
    def get_camel_case_string(snake_str):
        components = snake_str.split('_')
        camelCased = ''.join(x.capitalize() for x in components)
        abbreviations = {
            'Uuid': 'UUID',
            'Pdisk': 'PDisk',
            'Vdisk': 'VDisk',
            'NtoSelect': 'NToSelect',
            'Ssid': 'SSId',
            'Sids': 'SIDs',
            'GroupId': 'GroupID',
            'NodeId': 'NodeID',
            'DiskId': 'DiskID',
            'SlotId': 'SlotID',
        }
        for k, v in abbreviations.items():
            camelCased = camelCased.replace(k, v)
        return camelCased

    def convert_keys(data):
        if isinstance(data, dict):
            return {get_camel_case_string(k): convert_keys(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [convert_keys(item) for item in data]
        else:
            return data

    json_format.ParseDict(convert_keys(dictionary), proto)


def need_generate_bs_config(template_bs_config):
    # We need to generate blob_storage_config if template file does not contain static group:
    # blob_storage_config.service_set.groups
    if template_bs_config is None:

        return True

    return template_bs_config.get("service_set", {}).get("groups") is None
