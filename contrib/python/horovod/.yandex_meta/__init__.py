import os
import re
import shutil

from devtools.yamaker.arcfs import ArcBuildPath
from devtools.yamaker.fileutil import files
from devtools.yamaker.modules import (
    GLOBAL,
    Library,
    Linkable,
    Module,
    Switch,
    Words,
    py_srcs,
)
from devtools.yamaker.pathutil import is_source
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    def a(s):
        return f"{self.arcdir}/{s}"

    def d(s):
        return f"{self.dstdir}/{s}"

    # Regenerate from message.fbs.
    os.unlink(d("horovod/common/wire/message_generated.h"))
    # Remove unimportable packages.
    for p in ("keras", "mxnet", "spark", "torch"):
        shutil.rmtree(d("horovod/" + p))
    no_ops = re.compile("(cuda|ddl|mlsl|nccl)_operations")

    def common_source(p):
        return p.endswith(".fbs") or is_source(p) and not no_ops.search(p)

    def cuda_source(p):
        return is_source(p) and re.search("(cuda|nccl)_operations", p)

    cuda_cflags = ["-DHAVE_CUDA", "-DHAVE_NCCL", "-DHOROVOD_GPU_ALLREDUCE='N'"]
    self.yamakes["horovod/common"] = self.module(
        Library,
        PEERDIR=[
            "contrib/restricted/boost/atomic",
            "contrib/restricted/boost/lockfree",
            "contrib/libs/eigen",
            "contrib/libs/openmpi/ompi",
        ],
        ADDINCL=[
            "contrib/libs/eigen",
            "contrib/libs/nvidia/nccl/include",
            ArcBuildPath(a("horovod/common")),
            a("third_party/lbfgs/include"),
        ],
        CFLAGS=["-DEIGEN_MPL2_ONLY", "-DOMPI_SKIP_MPICXX"],
        NO_COMPILER_WARNINGS=True,
        SRCS=files(d("horovod/common"), rel=True, test=common_source),
        RECURSE=["syms"],
        after=dict(
            CFLAGS=[
                Switch(
                    HAVE_CUDA=Linkable(
                        PEERDIR=["contrib/libs/nvidia/nccl"],
                        CFLAGS=cuda_cflags,
                        SRCS=files(d("horovod/common"), rel=True, test=cuda_source),
                    )
                )
            ]
        ),
    )
    self.yamakes["horovod/tensorflow"] = self.module(
        Library,
        PEERDIR=["contrib/deprecated/tf/minimal", a("horovod/common")],
        NO_COMPILER_WARNINGS=True,
        SRCS=[GLOBAL("mpi_ops.cc")],
        RECURSE=["gen_mpi_ops"],
        after=dict(CFLAGS=[Switch(HAVE_CUDA=Linkable(CFLAGS=cuda_cflags))]),
    )
    self.yamakes["."] = self.module(
        Module,
        module="PY3_LIBRARY",
        PEERDIR=[
            "contrib/deprecated/tf/python",
            a("horovod/common/syms"),
            a("horovod/tensorflow/gen_mpi_ops"),
            "contrib/python/cloudpickle",
            "contrib/python/psutil",
            "contrib/python/setuptools",
        ],
        NO_LINT=True,
        PY_SRCS=py_srcs(self.dstdir),
        RESOURCE_FILES=[
            Words("PREFIX", f"{self.arcdir}/"),
            ".dist-info/METADATA",
            ".dist-info/top_level.txt",
        ],
        RECURSE=["horovod/common", "horovod/tensorflow"],
    )


horovod = NixSourceProject(
    projects=["horovod"],
    owners=["g:python-contrib"],
    arcdir="contrib/python/horovod",
    nixattr="horovod",
    copy_sources=["horovod/", "third_party/lbfgs/"],
    disable_includes=["ddl.hpp", "mlsl.hpp"],
    keep_paths=[".dist-info", "horovod/common/syms", "horovod/tensorflow/gen_mpi_ops"],
    post_install=post_install,
)
