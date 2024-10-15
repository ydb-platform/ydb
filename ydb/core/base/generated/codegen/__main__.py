import os
import sys
from ydb.core.protos.feature_flags_pb2 import TFeatureFlags, RequireRestart

from jinja2 import Environment, FileSystemLoader
from google.protobuf.descriptor import FieldDescriptor


class Slot(object):
    def __init__(self, name, index):
        self.name = name
        self.index = index
        self.fields = []
        self.default_value = 0
        self.runtime_flags_mask = 0


class Field(object):
    def __init__(self, name, slot, has_mask, value_mask, full_mask, default_value, is_runtime):
        self.name = name
        self.slot = slot
        self.has_mask = has_mask
        self.value_mask = value_mask
        self.full_mask = full_mask
        self.default_value = default_value
        self.is_runtime = is_runtime


class EnvironmentByDir(dict):
    def __missing__(self, template_dir):
        env = Environment(loader=FileSystemLoader(template_dir))
        self[template_dir] = env
        return env


def main():
    slots = []
    fields = []
    current_bits = 64
    for field in TFeatureFlags.DESCRIPTOR.fields:
        if field.type == FieldDescriptor.TYPE_BOOL:
            if current_bits + 2 > 64:
                index = len(slots)
                slots.append(Slot(name=f'slot{index}', index=index))
                current_bits = 0
            shift = current_bits
            current_bits += 2
            slot = slots[-1]
            has_mask = 1 << shift
            value_mask = 2 << shift
            full_mask = 3 << shift
            default_value = 0
            if field.default_value:
                default_value = 2 << shift
            is_runtime = not field.GetOptions().Extensions[RequireRestart]
            fields.append(Field(
                name=field.name,
                slot=slot,
                has_mask=has_mask,
                value_mask=value_mask,
                full_mask=full_mask,
                default_value=default_value,
                is_runtime=is_runtime,
            ))
            slot.fields.append(fields[-1])
            slot.default_value |= default_value
            if is_runtime:
                slot.runtime_flags_mask |= full_mask

    template_files = sys.argv[1::2]
    output_files = sys.argv[2::2]
    env_by_dir = EnvironmentByDir()
    for (template_file, output_file) in zip(template_files, output_files):
        (template_dir, template_name) = os.path.split(template_file)
        env = env_by_dir[template_dir]
        template = env.get_template(template_name)
        result = template.render(generator=__file__, slots=slots, fields=fields)
        with open(output_file, 'w') as f:
            f.write(result)
        print(f'Generated {output_file} from {template_name}')


if __name__ == '__main__':
    main()
