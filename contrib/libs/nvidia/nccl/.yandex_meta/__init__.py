from devtools.yamaker.project import NixProject
import shutil

OPERATIONS = [
    "all_gather",
    "all_reduce",
    "broadcast",
    "reduce",
    "reduce_scatter",
    "sendrecv",
]


def post_install(self):
    # Replace their CUDA_MAJOR.CUDA_MINOR with our CUDA_VERSION.
    with self.yamakes["."] as nccl:
        nccl.CFLAGS = []
        nccl.CFLAGS.append("-DPROFAPI")
        nccl.CFLAGS.append("-DCUDA_VERSION=${CUDA_VERSION}")

    # Translate src/collectives/device/gen_rules.sh.
    with self.yamakes["."] as m:
        m.module = "CUDA_DEVICE_LINK_LIBRARY"

        with open(self.srcdir + "/makefiles/common.mk") as f:
            assert "maxrregcount=96 " in f.read()
        # We need to compile single source (e. g. reduce.cu) multiple times
        # with different CFLAGS.
        m.CUDA_NVCC_FLAGS = [
            "--relocatable-device-code=true",
            "-Xptxas=-maxrregcount=96",
            "-gencode=arch=compute_61,code=sm_61",
            "-gencode=arch=compute_70,code=sm_70",
            "-gencode=arch=compute_80,code=sm_80",
        ]
        m.after(
            "CUDA_NVCC_FLAGS",
            """
            IF (CUDA_VERSION VERSION_GE "11.8")
                CUDA_NVCC_FLAGS(
                    -gencode=arch=compute_90,code=sm_90
                )
            ELSE()
                CUDA_NVCC_FLAGS(
                    -gencode=arch=compute_80,code=compute_80
                )
            ENDIF()
            """,
        )

        srcs = []
        for src in m.SRCS:
            if '/device/' in src and src.endswith('.cu'):
                srcs.append(src)

        # There are two sets of .cu files with the same name,
        # and during compilation under CUDA-11, we encounter an error
        # during linking. Therefore, we need to rename all generated files.
        symmetric = list(filter(lambda name: '/symmetric/' in name, srcs))
        renamed = []
        for src in symmetric:
            s = f'{self.dstdir}/{src}'
            n = f'{self.dstdir}/{src}'.replace('.cu', '_.cu')
            shutil.move(s, n)
            renamed.append(f'{src}'.replace('.cu', '_.cu'))

        m.SRCS -= set(srcs)
        srcs = set(set(srcs) - set(symmetric)) | set(renamed)
        for src in sorted(srcs):
            m.after("SRC", f"SRC({src})")

        nvcc_device_link = f"""
NVCC_DEVICE_LINK(
    {"\n".join(sorted(srcs))}
)
""".strip()
        m.after("SRC", nvcc_device_link)


nvidia_nccl = NixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/nvidia/nccl",
    nixattr="cudaPackages.nccl",
    build_install_subdir="build",
    ignore_commands=["bash", "install", "sed", "generate.py", "python3"],
    disable_includes=[
        "nvml.h",
        "gdrapi.h",
        "infiniband/**",
    ],
    ignore_targets=["nccl", "ncclras"],
    put={"nccl_static": "."},
    post_install=post_install,
)
