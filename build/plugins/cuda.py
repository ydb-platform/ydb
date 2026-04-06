import os

DEFAULT_CUDA_ARCHITECTURES="sm_50"


def arch2num(arch):
    if not arch[-1].isdigit():
        arch = arch[:-1]

    return f"{arch}0"


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
    architectures = [name.split('_')[1] for name in architecture_names]

    stub_arch = architectures[-1]
    arch_list = arch2num(stub_arch)

    cflags = [
        "-D__NV_LEGACY_LAUNCH",
        "-Wno-unused-function",
        "-Wno-unused-parameter",
        "-Wno-deprecated-literal-operator",
    ]

    for cu in args:
        name, _ = os.path.splitext(cu)
        images = []

        for arch in architectures:
            unit.on_cuda_compile_device([cu, arch, arch_list])

            images.append(f"{name}.{arch}.ptx")
            images.append(f"{name}.{arch}.cubin")
            images.append(f"{name}.{arch}.module_id")

        unit.on_cuda_fatbin([cu, *images])

        unit.on_cuda_compile_host([cu, stub_arch, arch_list])

        unit.onsrc([f"{name}.cudafe1.cpp", *cflags])
