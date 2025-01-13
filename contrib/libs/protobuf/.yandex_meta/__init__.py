import os
import os.path

from devtools.yamaker import fileutil
from devtools.yamaker import pathutil
from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def make_full_path(src):
    return os.path.join("src/google/protobuf", src)


RUNTIME_EXCESS_SOURCES = [
    "src/google/protobuf/compiler/importer.cc",
    "src/google/protobuf/compiler/parser.cc",
]

RUNTIME_YANDEX_SPECIFIC_SOURCES = [
    "src/google/protobuf/json_util.cc",
    "src/google/protobuf/messagext.cc",
]

# This stubs are deprecated, will removed after protobuf update
DEPRECATED_STUBS = [
    "src/google/protobuf/stubs/hash.h",
    "src/google/protobuf/stubs/stl_util.h",
    "src/google/protobuf/stubs/stringpiece.cc",
    "src/google/protobuf/stubs/stringpiece.h",
    "src/google/protobuf/stubs/strutil.cc",
    "src/google/protobuf/stubs/strutil.h",
    "src/google/protobuf/stubs/status.cc",
    "src/google/protobuf/stubs/status.h",
    "src/google/protobuf/stubs/substitute.cc",
    "src/google/protobuf/stubs/substitute.h",
    "src/google/protobuf/stubs/map_util.h",
    "src/google/protobuf/stubs/structurally_valid.cc",
    "src/google/protobuf/util/json_util.h",
    "src/google/protobuf/json/old_json.cc",
    "src/google/protobuf/json_util.cc",
    "src/google/protobuf/json_util.h",
]

DEPRECATED_SRC = [x for x in DEPRECATED_STUBS if x.endswith(".cc")]

# Set of proto files coming with original google protobuf (excluding descriptor.proto, see below)
# WARN: upon changing this file, make sure to check protobuf_std counterpart.
RUNTIME_PROTO_FILES = [
    "src/google/protobuf/any.proto",
    "src/google/protobuf/api.proto",
    "src/google/protobuf/descriptor.proto",
    "src/google/protobuf/duration.proto",
    "src/google/protobuf/empty.proto",
    "src/google/protobuf/field_mask.proto",
    "src/google/protobuf/source_context.proto",
    "src/google/protobuf/struct.proto",
    "src/google/protobuf/timestamp.proto",
    "src/google/protobuf/type.proto",
    "src/google/protobuf/wrappers.proto",
]

PROTOC_PROTO_FILES = [
    "src/google/protobuf/compiler/plugin.proto",
]


LIBPROTOC_DIR = "contrib/libs/protoc"
PYTHON_DIR = "contrib/python/protobuf/py3"

POSSIBLE_STD_STRING_USAGE_PATTERNS = [
    # It can be referenced to as `::std::string` or `std::string`
    r"::std::string",
    r"\bstd::string\b",
]


def post_build(self):
    # Replace std::string with TProtoStringType.
    for pattern in POSSIBLE_STD_STRING_USAGE_PATTERNS:
        fileutil.re_sub_dir(
            self.dstdir,
            pattern,
            "TProtoStringType",
            # Only apply replacements to C++ code
            test=pathutil.is_preprocessable,
        )


def post_install(self):
    with self.yamakes["."] as libprotobuf:
        libprotobuf.PROVIDES = ["protobuf"]

        libprotobuf.SRCS.update(RUNTIME_YANDEX_SPECIFIC_SOURCES)
        libprotobuf.SRCS.update(DEPRECATED_SRC)  # This files should be removed after protobuf update
        libprotobuf.NO_UTIL = False

        # Work around fixed_address_empty_string initialization on macOS.
        gmu = "src/google/protobuf/generated_message_util.cc"
        libprotobuf.SRCS.remove(gmu)
        libprotobuf.SRCS.add(ArcPath(gmu, GLOBAL=True))

        # These sources are parts of protoc, they should not be linked into runtime
        for src in RUNTIME_EXCESS_SOURCES:
            libprotobuf.SRCS.remove(src)

        libprotobuf.after(
            "CFLAGS",
            Switch(
                OS_ANDROID=Linkable(
                    # Link with system android log library
                    # Android logging is used in stubs/common.cc
                    EXTRALIBS=["log"]
                )
            ),
        )

        libprotobuf.after("SRCS", Linkable(FILES=RUNTIME_PROTO_FILES))
        libprotobuf.after(
            "ORIGINAL_SOURCE",
            """IF (OPENSOURCE_REPLACE_PROTOBUF AND EXPORT_CMAKE)

    OPENSOURCE_EXPORT_REPLACEMENT(
        CMAKE Protobuf
        CMAKE_TARGET protobuf::libprotobuf protobuf::libprotoc
        CONAN protobuf/${OPENSOURCE_REPLACE_PROTOBUF}
        CONAN_ADDITIONAL_SEMS
            "&& conan_require_tool" protobuf/${OPENSOURCE_REPLACE_PROTOBUF} "&& conan-tool_requires" protobuf/${OPENSOURCE_REPLACE_PROTOBUF}
            "&& conan_import \\"bin, protoc* -> ./bin\\" && conan-imports 'bin, protoc* -> ./bin' && vanilla_protobuf"
    )

ELSE()

    ADDINCL(
        GLOBAL contrib/libs/protobuf/src
        GLOBAL FOR proto contrib/libs/protobuf/src
    )

ENDIF()
""",
        )

        libprotobuf.ADDINCL = ["contrib/libs/protobuf/third_party/utf8_range"]

        libprotobuf.SRCS.add("third_party/utf8_range/utf8_validity.cc")

        libprotobuf.RECURSE = ["builtin_proto"]

        libprotobuf.PEERDIR.add("library/cpp/sanitizer/include")

        # Dont use full y_absl library
        # fmt: off
        libprotobuf.PEERDIR = set([
            lib for lib in libprotobuf.PEERDIR
            if 'abseil-cpp-tstring' not in lib
        ])
        # fmt: on
        libprotobuf.PEERDIR.add("contrib/restricted/abseil-cpp-tstring/y_absl/status")
        libprotobuf.PEERDIR.add("contrib/restricted/abseil-cpp-tstring/y_absl/log")

    del self.yamakes["src/google/protobuf/compiler"]
    # merging src/google/protobuf/compiler/protoc library and
    # src/google/protobuf/compiler binary into top-level binary
    with self.yamakes.pop("src/google/protobuf/compiler/protoc") as libprotoc:
        libprotoc.VERSION = self.version
        libprotoc.ORIGINAL_SOURCE = self.source_url
        libprotoc.PROVIDES = ["protoc"]
        libprotoc.after("LICENSE", "LICENSE_TEXTS(.yandex_meta/licenses.list.txt)\n")
        libprotoc.after(
            "ORIGINAL_SOURCE",
            """IF (OPENSOURCE_REPLACE_PROTOBUF AND EXPORT_CMAKE)

    OPENSOURCE_EXPORT_REPLACEMENT(
        CMAKE Protobuf
        CMAKE_TARGET protobuf::libprotobuf protobuf::libprotoc
        CONAN protobuf/${OPENSOURCE_REPLACE_PROTOBUF}
        CONAN_ADDITIONAL_SEMS
            "&& conan_require_tool" protobuf/${OPENSOURCE_REPLACE_PROTOBUF} "&& conan-tool_requires" protobuf/${OPENSOURCE_REPLACE_PROTOBUF}
            "&& conan_import \\"bin, protoc* -> ./bin\\" && conan-imports 'bin, protoc* -> ./bin' && vanilla_protobuf"
    )

ELSE()

    ADDINCL(
        GLOBAL contrib/libs/protoc/src
    )

ENDIF()
""",
        )

        libprotoc.ADDINCL = ["contrib/libs/protobuf/third_party/utf8_range"]

        libprotoc.SRCS = {os.path.join("src/google/protobuf/compiler", src) for src in libprotoc.SRCS}
        # Moving a couple of sources from runtime library to compiler (where they actually belong)
        libprotoc.SRCS.update(RUNTIME_EXCESS_SOURCES)

        libprotoc.SRCDIR = None

        # Unbundle libprotobuf.la which confuses yamaker by being linked statically
        libprotoc.PEERDIR = {self.arcdir}

        libprotoc_abs_dir = os.path.join(self.ctx.arc, LIBPROTOC_DIR)

        fileutil.copy(
            os.path.join(self.dstdir, "src/google/protobuf/compiler"),
            os.path.join(libprotoc_abs_dir, "src/google/protobuf"),
            replace=True,
            move=True,
        )

        fileutil.copy(
            [os.path.join(self.dstdir, "LICENSE")],
            libprotoc_abs_dir,
            replace=True,
            move=False,
        )

        for lang in ["csharp", "objectivec"]:
            for root, _, files in os.walk(os.path.join(libprotoc_abs_dir, "src/google/protobuf/compiler", lang)):
                for file in files:
                    if file.endswith(".h"):
                        with open(os.path.join(root, lang + "_" + file), "w") as f:
                            f.write(f'#include "{file}"\n')
                            f.write('#include "names.h"')

        # generate temporal proxy for ydb
        with open(os.path.join(libprotoc_abs_dir, "src/google/protobuf/compiler/cpp/cpp_helpers.h"), "w") as f:
            f.write('#include "helpers.h"')

        with open(f"{libprotoc_abs_dir}/ya.make", "wt") as ymake:
            ymake.write(str(libprotoc))

        with open(os.path.join(self.ctx.arc, self.arcdir, "src/google/protobuf/util/json_util.h"), "w") as f:
            f.write('#define USE_DEPRECATED_NAMESPACE 1\n#include "google/protobuf/json/json.h"')


protobuf = CMakeNinjaNixProject(
    owners=["g:cpp-committee", "g:cpp-contrib"],
    arcdir="contrib/libs/protobuf",
    nixattr="protobuf",
    license_analysis_extra_dirs=[
        LIBPROTOC_DIR,
    ],
    install_targets=[
        # Do not install protobuf lite, as nobody needs it
        "protobuf",
        # Specifying protoc will install both libprotoc and protoc executable
        "protoc",
    ],
    unbundle_from={"abseil-cpp": "third_party/abseil-cpp"},
    put={"protobuf": "."},
    disable_includes=[
        "sys/isa_defs.h",
    ],
    keep_paths=[
        # ya.make generation for legacy PACKAGE at protobuf/python/ya.make is not configure by yamaker.
        "python/ya.make",
        # built-in protobufs need to be exposed via PROTO_LIBRARY.
        # Needed at least for Python and for complete descriptors generation
        "builtin_proto",
        # yandex-specific files. Should be moved out of the project, if possible
        "src/google/protobuf/messagext.*",
        *DEPRECATED_STUBS,
    ],
    copy_sources=(
        RUNTIME_PROTO_FILES
        + PROTOC_PROTO_FILES
        + [
            # java_names.h is required by contrib/libs/grpc-java
            "src/google/protobuf/compiler/java/java_names.h",
        ]
    ),
    post_build=post_build,
    post_install=post_install,
)
