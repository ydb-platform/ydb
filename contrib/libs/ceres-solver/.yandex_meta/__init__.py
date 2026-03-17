from devtools.yamaker.fileutil import re_sub_dir
from devtools.yamaker.project import CMakeNinjaNixProject


def ceres_solver_post_install(self):
    # Fix build with libc++.
    re_sub_dir(
        self.dstdir,
        r"([ (])(adjacent_find|sort|unique|find|random_shuffle|accumulate)\(",
        r"\1std::\2(",
    )


ceres_solver = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/ceres-solver",
    nixattr="ceres-solver",
    copy_sources=[
        "include/",
        "internal/ceres/cuda_*.h",
    ],
    build_install_subdir="tmp/yamaker/ceres-solver/out",
    use_full_libnames=True,
    install_targets=[
        "libceres",
        "libceres_static",
    ],
    put={
        "libceres": ".",
    },
    put_with={
        "libceres": [
            "libceres_static",
        ],
    },
    disable_includes=[
        "cs.h",
        "tbb/",
        "Accelerate.h",
        "SuiteSparseQR.hpp",
        "cholmod.h",
    ],
    post_install=ceres_solver_post_install,
)
