import os
import shutil

from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.modules import py_srcs, Linkable, Switch
from devtools.yamaker.project import NixProject
from devtools.yamaker import python


def post_install(self):
    python.extract_dist_info(self)

    # pypi archive with grpcio contains cpp core and python files
    # Python files in this archive are located in src/python/grpcio
    # In order to exclude additional PEERDIR usage in Arcadia
    # let's move src/python/grpcio content directly to contrib/python/grpcio/py3
    python_source_location = os.path.join(self.dstdir, "src/python/grpcio")
    grpcio_files = os.listdir(python_source_location)

    # move all files to root
    for elem in grpcio_files:
        shutil.move(os.path.join(python_source_location, elem), self.dstdir)

    # remove unnecessary folder src
    shutil.rmtree(os.path.join(self.dstdir, "src"))

    # Collect python files but remove files for pypi build and
    # yamaker related files
    with self.yamakes["."] as pb:
        py_src_common = py_srcs(
            self.dstdir,
            remove=[
                ".yandex_meta/__init__.py",  # yamaker settings
                "grpc_core_dependencies.py",
                "support.py",
                "commands.py",
                "_spawn_patch.py",
                "_parallel_compile_patch.py",
                "grpc_version.py",
            ],
        )

        pb.to_py_library(
            module="PY3_LIBRARY",
            PY_SRCS=py_src_common,
            PEERDIR=["contrib/libs/grpc", "contrib/python/six"],
            ADDINCL=[
                "${ARCADIA_BUILD_ROOT}/contrib/libs/grpc",
                "contrib/libs/grpc",
                "contrib/libs/grpc/include",
                ArcPath("contrib/python/grpcio/py3", FOR="cython"),
            ],
        )

        # Function pointer is not sanitized for ubsan.
        # This decision has been done in original grpc repo.
        # see https://github.com/grpc/grpc/blob/v1.45.0/tools/bazel.rc#L103
        pb.after(
            "ADDINCL",
            Switch({"SANITIZER_TYPE == undefined": Linkable(CXXFLAGS=["-fno-sanitize=function"])}),
        )


grpcio = NixProject(
    owners=["g:python-contrib"],
    projects=["python/grpcio"],
    arcdir="contrib/python/grpcio/py3",
    nixattr=python.make_nixattr("grpcio"),
    ignore_targets=[
        "cygrpc.cpython-310-x86_64-linux-gnu",
    ],
    copy_sources=[
        "src/python/grpcio/**/*.py",
        "src/python/grpcio/**/*.pxi",
        "src/python/grpcio/**/*.pxd",
        "src/python/grpcio/**/*.pyx",
    ],
    post_install=post_install,
)
