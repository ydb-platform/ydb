#!/usr/bin/env python3

from typing import Any, List, Type, Dict
from itertools import groupby
from templates import NON_REPEATED, REPEATED, STRUCT_WIDE

import os
import re

SPEC_FILE = 'spec.raw'
GENERATED_STRUCTS_FILE = 'generated.raw.h'
CLOSING_BRACKET = '};\n'
OP = '{'
CL = '}'
ENUM = 'enum'

class TreeNode:
    def __init__(self, value: Any):
        self.value = value

class Struct(TreeNode):
    @staticmethod
    def generate_sizeof(fields):
        sizes_unrepeated = [f'sizeof({field_info["field_type"]})' for field_info in fields if not field_info["field_repeated"]]
        sizes_repeated = [f'sizeof({field_info["field_type"]}) * {field_info["field_name"]}.size()' for field_info in fields if field_info["field_repeated"]]
        return ' + '.join(sizes_unrepeated + sizes_repeated)

    @staticmethod
    def generate_debug_string(fields):
        return "DEBUG_STRING"

    @staticmethod
    def generate_serialize(fields):
        res = ""

        sizeof_without_repeated_fields = Struct.generate_sizeof([field for field in fields if not field['field_repeated']])
        res += f"serializeHelper(this, {sizeof_without_repeated_fields});\n"

        for field in fields:
            if not field['field_repeated']:
                continue
            res += f"\t\tserializeHelper({field['field_name']}.data(), {Struct.generate_sizeof([field])});\n"

        print("\n\n", "generate_serialize:\n", res)
        return res


    def fields(self):
        fields = list()
        for field in self.value[1:-1]:
            regex = re.match(r'^\s*(\w*)\s(\w*)( = (\w*))?\W\s(<\w*>)?$', field)
            field_info = {
                'field_type'    : regex.group(1),
                'field_name'    : regex.group(2),
                'field_default' : f" = {regex.group(4)}" if regex.group(4) else "",
                'field_repeated': regex.group(5) is not None,
                }
            fields.append(field_info)

        return fields

    def process(self) -> str:
        entity = self.value[0][:-3].split()
        if entity[0] == ENUM:
            return "".join(self.value) + "\n"

        fields = self.fields()

        res = self.value[0].strip('\n')
        for field_info in fields:
            res += (REPEATED if field_info['field_repeated'] else NON_REPEATED).format(**field_info)
        res += STRUCT_WIDE.format(type_name=entity[1],
                                  sizeof=self.generate_sizeof(fields),
                                  debug_string=self.generate_debug_string(fields),
                                  serialize_code=self.generate_serialize(fields),
                                  )

        return res

class AST:
    def __init__(self, items):
        self.items: List[Struct] = items

    @classmethod
    def from_file(cls, file_path: str):
        with open(file_path) as spec:
            lines = spec.readlines()
        return cls([Struct(list(g)) for k, g in groupby(lines, key=lambda x: x != '\n') if k])

def main():
    ast = AST.from_file(SPEC_FILE)

    try:
        os.remove(GENERATED_STRUCTS_FILE)
    except:
        pass

    for item in ast.items:
        struct: str = item.process()
        with open(GENERATED_STRUCTS_FILE, mode='a') as res:
            res.write(struct)

    return 0

if __name__ == '__main__':
    main()