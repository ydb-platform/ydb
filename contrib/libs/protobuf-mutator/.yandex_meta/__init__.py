from devtools.yamaker import fileutil
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    fileutil.re_sub_dir(
        self.dstdir,
        r"\bstd::string\b",
        "TProtoStringType",
    )


protobuf_mutator = CMakeNinjaNixProject(
    nixattr="protobuf_mutator",
    arcdir="contrib/libs/protobuf-mutator",
    owners=["g:cpp-contrib"],
    addincl_global={".": ["."]},
    put_with={"protobuf-mutator": ["protobuf-mutator-libfuzzer"]},
    post_install=post_install,
)
