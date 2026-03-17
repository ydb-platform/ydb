from devtools.yamaker.fileutil import re_sub_dir
from devtools.yamaker.modules import GLOBAL, Library
from devtools.yamaker.project import NixSourceProject

CUSTOM_ALIGNMENT_NOTICE = """
# Custom alignment is already used by serialized data stored outside of Arcadia.
# Any attempt to change alignment to default (16 at the time) might lead to unexpected slowdowns.
# It may also lead to segfaults caused by misalignments.
""".strip()


def eigen_post_install(self):
    re_sub_dir(self.dstdir, r"mkl\.h", "contrib/libs/intel/mkl/include/mkl.h")
    re_sub_dir(self.dstdir, r"mkl_lapacke\.h", "contrib/libs/intel/mkl/include/mkl_lapacke.h")
    self.yamakes["."] = self.module(
        Library,
        NO_UTIL=True,
        CFLAGS=[
            GLOBAL("-DEIGEN_MAX_ALIGN_BYTES=32"),
            GLOBAL("-DEIGEN_MPL2_ONLY"),
        ],
    )
    self.yamakes["."].before("CFLAGS", CUSTOM_ALIGNMENT_NOTICE)


eigen = NixSourceProject(
    arcdir="contrib/libs/eigen",
    nixattr="eigen",
    copy_sources=[
        "Eigen/**",
        "unsupported/Eigen/**",
        "unsupported/README.txt",
    ],
    disable_includes=[
        "src/IterativeSolvers/IterationController.h",
        "src/IterativeSolvers/ConstrainedConjGrad.h",
        "Eigen/src/Core/arch/HIP/hcc/math_constants.h",
        "SYCL/",
        "arm_sve.h",
        "hip/*.h",
        "lapacke_config.h",
        "metis.h",
        "src/FFT/ei_fftw_impl.h",
        "src/FFT/ei_imklfft_impl.h",
        "vecintrin.h",
        "EIGEN_*_PLUGIN",
    ],
    post_install=eigen_post_install,
)


eigen.copy_sources_except |= {
    # Do not copy LGPL-2.1 licensed sources
    "unsupported/Eigen/src/IterativeSolvers/ConstrainedConjGrad.h",
    "unsupported/Eigen/src/IterativeSolvers/IterationController.h",
}
