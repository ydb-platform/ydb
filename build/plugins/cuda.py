import os

DEFAULT_CUDA_ARCHITECTURES="sm_50"


def oncuda_srcs(unit, *args):
    """
    @usage: CUDA_SRCS(File...)

    A macro for efficient distributed compilation of CUDA code for multiple device architectures.

    For each source .cu file multiple nodes are generated:
    - node per each device architecture producing PTX and CUBIN
    - node merging all PTX and CUBIN files into a single FATBIN blob
    - node producing .cpp with host code
    - node compiling host .cpp with embedded FATBIN blob

    CUDA_ARCHITECTURES variable is used to determine the list of architectures to compile device code for.
    """
    architecture_names = (unit.get("CUDA_ARCHITECTURES") or DEFAULT_CUDA_ARCHITECTURES).split(":")
    architectures = set(name.split('_')[1] for name in architecture_names)
    max_arch = max(architectures)
    arch_list = "${__COMMA__}".join([str(arch * 10) for arch in architectures])

    cflags = [
        f"-D__CUDA_ARCH__={max_arch * 10}",
        f"-D__CUDA_ARCH_LIST__={arch_list}",
        "-D__NV_LEGACY_LAUNCH",
    ]

    for cu in args:
        name, _ = os.path.splitext(cu)
        images = []

        for arch in architectures:
            unit.on_cuda_compile_device([cu, str(arch)])

            images.append(f"{name}.{arch}.ptx")
            images.append(f"{name}.{arch}.cubin")

        unit.on_cuda_fatbin([cu, *images])

        unit.on_cuda_compile_host([cu, str(max_arch)])

        unit.onsrc([f"{name}.cudafe1.cpp", *cflags])
