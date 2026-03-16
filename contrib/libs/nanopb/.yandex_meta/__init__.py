from devtools.yamaker.project import CMakeNinjaNixProject

nanopb = CMakeNinjaNixProject(
    owners=["g:quasar-sys", "g:cpp-contrib"],
    nixattr="nanopb",
    arcdir="contrib/libs/nanopb",
    build_subdir="build",
    disable_includes=["avr/pgmspace.h", "PB_SYSTEM_HEADER"],
    addincl_global={".": ["."]},
    copy_sources=[
        "pb.h",
        "pb_common.c",
        "pb_common.h",
        "pb_decode.c",
        "pb_decode.h",
        "pb_encode.c",
        "pb_encode.h",
        "generator/nanopb_generator.py",
        "generator/proto/__init__.py",
        "generator/proto/_utils.py",
        "generator/proto/nanopb.proto",
    ],
    keep_paths=[
        "generator/**/ya.make",
        "generator/protoc-gen-nanopb.py",
    ],
)
