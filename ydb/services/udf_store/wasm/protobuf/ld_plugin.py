#!/usr/bin/env python3

import sys
import json


PROTOBUF_LIBS = [
    "contrib/libs/protobuf/libcontrib-libs-protobuf.a",
    "contrib/libs/zlib/libcontrib-libs-zlib.a",
    "contrib/restricted/abseil-cpp-tstring/libcontrib-restricted-abseil-cpp-tstring.a",
    "contrib/restricted/google/utf8_range/libcontrib-restricted-google-utf8_range.a",
    "contrib/restricted/abseil-cpp/libcontrib-restricted-abseil-cpp.a",
]


def fix_cmd_wasm_protobuf(cmd):
    for flag in cmd:
        if flag not in PROTOBUF_LIBS:
            yield flag


if __name__ == "__main__":
    sys.stdout.write(json.dumps(list(fix_cmd_wasm_protobuf(sys.argv[1:]))))
