#!/usr/bin/env python3

import sys
import json


SDK_LIBS = [
    "contrib/restricted/emscripten/system/lib/c/libsystem-lib-c.a",
    "contrib/restricted/emscripten/system/lib/dlmalloc/libsystem-lib-dlmalloc.a",
    "contrib/restricted/emscripten/system/lib/standalonewasm/libsystem-lib-standalonewasm.a",
    "contrib/libs/cxxsupp/libcxx/liblibs-cxxsupp-libcxx.a",
    "contrib/libs/cxxsupp/libcxxabi/liblibs-cxxsupp-libcxxabi.a",
    "contrib/libs/libunwind/libcontrib-libs-libunwind.a",
    "util/libyutil.a",
]


def fix_cmd_wasm_sdk(cmd):
    for flag in cmd:
        if flag not in SDK_LIBS:
            yield flag


if __name__ == "__main__":
    sys.stdout.write(json.dumps(list(fix_cmd_wasm_sdk(sys.argv[1:]))))
