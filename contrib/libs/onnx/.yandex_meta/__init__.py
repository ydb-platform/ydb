import os
import shutil

from devtools.yamaker import fileutil, onnx, pathutil
from devtools.yamaker.modules import GLOBAL, py_srcs
from devtools.yamaker.project import CMakeNinjaNixProject


def post_build(self):
    # Remove the generated .pb.h/.pb.cc files and copy the original .proto's to a separate PROTO_LIBRARY().
    dst = f"{self.dstdir}/onnx"
    for proto in onnx.ORIG_PROTO_NAMES:
        os.remove(f"{dst}/{proto}.pb.h")
        os.remove(f"{dst}/{proto}.pb.cc")
        sanitized_proto = onnx.sanitize_proto_name(proto)
        shutil.copyfile(f"{self.builddir}/onnx/{proto}.proto", f"{dst}/{sanitized_proto}.proto")
        py_wrapper = onnx.get_py_wrapper(proto)
        shutil.copyfile(f"{self.builddir}/onnx/{py_wrapper}", f"{dst}/{py_wrapper}")

    onnx.sanitize_proto_names(self)

    # The upstream way of importing submodules fails in Arcadia Python with errors that look like this:
    #    [...]
    #    ---> 23 import onnx.onnx_cpp2py_export.checker as c_checker
    #    [...]
    #    ModuleNotFoundError: No module named 'onnx.onnx_cpp2py_export.checker'; 'onnx.onnx_cpp2py_export' is not a package
    fileutil.re_sub_dir(
        dst,
        r"import onnx\.onnx_cpp2py_export\.(\w+) as ",
        r"from onnx.onnx_cpp2py_export import \1 as ",
        test=pathutil.is_py_src,
    )

    with open(f"{self.dstdir}/onnx/version.py", "w") as f:
        f.write(f'version = "{self.version}"')


def post_install(self):
    self.yamakes["proto"] = self.module(
        module="PROTO_LIBRARY",
        PROTO_NAMESPACE=GLOBAL("contrib/libs/onnx"),
        PY_NAMESPACE=".",
        SRCDIR=["contrib/libs/onnx"],
        # fmt: off
        SRCS={
            f"onnx/{onnx.sanitize_proto_name(proto)}.proto"
            for proto in onnx.ORIG_PROTO_NAMES
        },
        # fmt: on
        # Go protobufs fail to build with the following error:
        #    https://nda.ya.ru/t/VI_pND9b58B8YN
        # It's highly unlikely anyone is going to use ONNX from Go, so we can safely remove the Go tag.
        EXCLUDE_TAGS=["GO_PROTO"],
    )

    self.yamakes["python"].to_py_library(
        PY_REGISTER=["onnx.onnx_cpp2py_export"],
        PY_SRCS=py_srcs(f"{self.dstdir}/onnx", ns="onnx"),
    )
    with self.yamakes["python"] as m:
        m.PEERDIR |= {
            "contrib/libs/onnx/proto",
            # For numpy_helper.py.
            "contrib/python/numpy",
            "contrib/python/typing-extensions",
        }

    with self.yamakes["."] as m:
        # ONNX defines a bunch of -DONNX_XXX=YYY flags, which should be set when any library includes headers from `contrib/libs/onnx`.
        # Mark them as GLOBAL.
        m.CFLAGS = [GLOBAL(flag) for flag in m.CFLAGS]
        m.NO_UTIL = False
        m.PEERDIR.add("contrib/libs/onnx/proto")
        m.RECURSE.add("proto")
    onnx.use_any_protobuf(self)


onnx_project = CMakeNinjaNixProject(
    nixattr="onnx",
    arcdir="contrib/libs/onnx",
    owners=["g:cpp-contrib", "g:matrixnet"],
    flags=[
        "-DONNX_BUILD_PYTHON=ON",
        "-DCMAKE_INSTALL_LIBDIR=lib",
        "-DONNX_GEN_PB_TYPE_STUBS=OFF",
    ],
    # Build only the ONNX core and pybind11 bindings for Python.
    build_targets=["onnx", "onnx_cpp2py_export"],
    # Don't install onnx_proto, which is a dependency of onnx.
    install_targets=["onnx", "onnx_cpp2py_export.cpython-311-x86_64-linux-gnu"],
    put={
        "onnx": ".",
        "onnx_cpp2py_export.cpython-311-x86_64-linux-gnu": "python",
    },
    # We resort to manually copying and building ONNX protobufs:
    # https://st.yandex-team.ru/CONTRIB-2463#6206252cb7c956262cc663ed
    ignore_commands=["protoc"],
    post_build=post_build,
    post_install=post_install,
    copy_sources=[
        # Intentionally include only some of the Python files from `onnx/`.
        # This ignores tests, tools, and ONNX backend wrappers (which we don't use).
        "onnx/*.py",
        "onnx/**/*.pyi",
        "onnx/defs/__init__.py",
        "onnx/py.typed",
    ],
    copy_sources_except=[
        "onnx/test/__init__.pyi",
    ],
    copy_top_sources_except=[
        "CODEOWNERS",
        "CODE_OF_CONDUCT.md",
        "RELEASE-MANAGEMENT.md",
        "VERSION_NUMBER",
    ],
    # We define ONNX_ML. These includes are supposed to be used only when ONNX_ML is undefined,.
    disable_includes=[
        "onnx/onnx.pb.h",
        "onnx/onnx-operators.pb.h",
    ],
)
