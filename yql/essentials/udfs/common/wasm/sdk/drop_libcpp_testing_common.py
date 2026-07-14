#!/usr/bin/env python3
import os
import sys
import json

SDK_LIBS = [
    "library/cpp/json/common/libcpp-json-common.a",
    "library/cpp/json/fast_sax/libcpp-json-fast_sax.a",
    "tools/enum_parser/enum_serialization_runtime/libtools-enum_parser-enum_serialization_runtime.a",
    "library/cpp/json/writer/libcpp-json-writer.a",
    "library/cpp/string_utils/relaxed_escaper/libcpp-string_utils-relaxed_escaper.a",
    "library/cpp/json/liblibrary-cpp-json.a",
    "library/cpp/testing/common/libcpp-testing-common.a",
]


def fix_cmd_ytql_wasm_sdk(cmd):
    for flag in cmd:
        if flag not in SDK_LIBS:
            yield flag


if __name__ == "__main__":
    sys.stdout.write(json.dumps(list(fix_cmd_ytql_wasm_sdk(sys.argv[1:]))))
