import os.path as P
import re

from devtools.yamaker.fileutil import re_sub_dir, re_sub_file
from devtools.yamaker.modules import GLOBAL, Library, Linkable, Recursable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject
from devtools.yamaker import python

CUDA_MODULES = [
    "cudaarithm",
    "cudafilters",
    "cudaimgproc",
    "cudalegacy",
    "cudaoptflow",
    "cudawarping",
    "cudev",
]


REGULAR_MODULES = [
    "calib3d",
    "core",
    "features2d",
    "flann",
    "highgui",
    "imgcodecs",
    "imgproc",
    "ml",
    "objdetect",
    "optflow",
    "photo",
    "stitching",
    "superres",
    "video",
    "videoio",
    "ximgproc",
]


PY_BINDING_MODULE = "python/src2"


# depend on heavy CUDA modules when CUDA is enabled
HEAVY_MODULES = [
    "stitching",
    "superres",
]


MODULES_WITHOUT_CUDA_SUPPORT = [
    # segfault when building with CUDA 11.8, build OK with CUDA_VERSION >= 12.2
    "modules/photo",
    # increases size of python binaries, please do not use
    f"modules/{PY_BINDING_MODULE}",
]


PATCH_PREFIX = "#if 0\n"
PATCH_POSTFIX = "#endif\n"


def patch_lines(lines, patch_areas, template_begin, template_end):
    for area in reversed(patch_areas):
        lines.insert(area[1], template_end)
        lines.insert(area[0], template_begin)
    return lines


def patch_data(data, patch_areas, template_begin, template_end):
    for area in reversed(patch_areas):
        data = data[: area[0]] + template_begin + data[area[0] : area[1]] + template_end + data[area[1] :]
    return data


def patch_separate_lines(path_to_patch, template_begin, template_end, verifier):
    with open(path_to_patch) as file:
        lines = list(file)

    patch_areas = []
    begin = None

    for index, line in enumerate(lines):
        if begin is None and verifier(line):
            begin = index
            continue
        if begin is not None and not verifier(line):
            patch_areas.append([begin, index])
            begin = None
    if begin is not None:
        patch_areas.append([begin, len(lines)])

    lines = patch_lines(lines, patch_areas, template_begin, template_end)
    with open(path_to_patch, "w") as file:
        file.write("".join(lines))


def patch_generated_includes(path_to_patch):
    files_to_process = set()

    def verifier(line):
        for name in CUDA_MODULES:
            if f'#include "opencv2/{name}.hpp"' in line:
                files_to_process.add(f"{name}/include/opencv2/{name}.hpp")
                return True
        return False

    patch_separate_lines(path_to_patch, PATCH_PREFIX, PATCH_POSTFIX, verifier)
    return list(files_to_process)


def find_exported_symbols(files_to_process):
    def get_enum(line):
        fragments = line.split()
        if "enum" in fragments:
            return fragments[fragments.index("enum") + 1]
        return None

    def get_function(line):
        if not line.startswith("CV_EXPORTS"):
            return None
        # CV_EXPORTS <returned value type> <function name>(<arguments>)
        return line.split()[2].split("(")[0]

    def get_class(line):
        if not line.startswith("class"):
            return None
        # class CV_EXPORTS <class name>
        return line.split()[2]

    def get_method(line):
        stripped = line.lstrip()
        # CV_WRAP <static/virtual/> <return type> <function_name>(<arguments>)
        if not stripped.startswith("CV_WRAP"):
            return None
        for candidate in [stripped.split()[2], stripped.split()[3]]:
            if "(" in candidate:
                return candidate.split("(")[0]
        return None

    enums = set()
    enum_values = []
    functions = set()
    classes = set()
    methods = []

    for file_path in files_to_process:
        with open(file_path) as file:
            lines = list(file)

        def get_enum_values(lines, start_line, enum_name):
            starting = True
            open_brackets = 0
            for pos in range(start_line, len(lines)):
                opening = lines[pos].count("{")
                closing = lines[pos].count("}")
                open_brackets = open_brackets + opening - closing
                if open_brackets == 0:
                    if starting:
                        starting = False
                    else:
                        break

            enum_data = "".join(lines[start_line:pos])
            # strip comments
            enum_data = re.sub(r"(/\*.*?\*/|//.*|//[^\r\n]*$)", "", enum_data)
            enum_data = enum_data.split("{")[1].split("}")[0]
            enum_data = enum_data.replace("\n", "")
            # got a flat list of enums now, but there might be expressions like ENUM_VAR = 123
            enum_values = [chunk.strip() for chunk in enum_data.split(",")]
            return [item.split()[0] for item in enum_values if item]

        # parse enums separately as they can be nested in classes
        for index, line in enumerate(lines):
            name = get_enum(line)
            if name is not None:
                enums.add(name)
                enum_values += get_enum_values(lines, index, name)

        def skip_class_definition(index, lines, class_name):
            # and add methods
            open_brackets = 0
            starting = True
            for pos in range(index, len(lines)):
                opening = lines[pos].count("{")
                closing = lines[pos].count("}")
                open_brackets = open_brackets + opening - closing
                if open_brackets == 0:
                    if starting:
                        starting = False
                    else:
                        break
                name = get_method(lines[pos])
                if name is not None:
                    methods.append([class_name, name])
            return pos

        index = 0

        while index < len(lines):
            line = lines[index]

            name = get_function(line)
            if name is not None:
                functions.add(name)
                index += 1
                continue

            name = get_class(line)
            if name is not None:
                classes.add(name)
                index = skip_class_definition(index, lines, name)
                continue

            index += 1

    return enums, enum_values, functions, classes, methods


def patch_lines_with_symbols(path_to_patch, symbols):
    def verifier(line):
        for name in symbols:
            if name in line:
                return True
        return False

    patch_separate_lines(path_to_patch, PATCH_PREFIX, PATCH_POSTFIX, verifier)


def patch_function_definitions(path_to_patch, function_names):
    def function_declaration(line):
        for name in function_names:
            if name in line:
                return True
        return False

    with open(path_to_patch) as file:
        lines = list(file)

    patch_areas = []

    index = 0
    while index < len(lines):
        if not function_declaration(lines[index]):
            index += 1
            continue

        # skip function definition and mark if for patch
        open_brackets = 0
        starting = True
        for pos in range(index, len(lines)):
            opening = lines[pos].count("{")
            closing = lines[pos].count("}")
            open_brackets = open_brackets + opening - closing
            if open_brackets == 0:
                if starting:
                    starting = False
                else:
                    break
        patch_areas.append([index, pos + 1])
        index = pos

    lines = patch_lines(lines, patch_areas, PATCH_PREFIX, PATCH_POSTFIX)

    with open(path_to_patch, "w") as file:
        file.write("".join(lines))


def patch_class_declarations(path_to_patch, classes):
    def need_patch_line(line):
        for name in classes:
            if f"cv::cuda::{name}" in line:
                return True
        return False

    with open(path_to_patch) as file:
        lines = list(file)

    patch_areas = []
    begin = None

    for index, line in enumerate(lines):
        if need_patch_line(line) and begin is None:
            begin = index
            continue

        if not need_patch_line(line) and begin is not None:
            patch_areas.append([begin, index])
            begin = None

    if begin is not None:
        patch_areas.append([begin, len(lines)])

    lines = patch_lines(lines, patch_areas, PATCH_PREFIX, PATCH_POSTFIX)
    with open(path_to_patch, "w") as file:
        file.write("".join(lines))


def patch_class_definitions(path_to_patch, class_names):
    with open(path_to_patch) as file:
        data = file.read()

    template = """//================================================================================
// """

    patch_areas = []

    indices = [item.start() for item in re.finditer(template, data)]
    for pos, block_offset in enumerate(indices):
        offset = block_offset + len(template)
        block_name = data[offset:].split(" ", 1)[0]
        if block_name in class_names:
            begin = block_offset
            if pos == len(indices) - 1:
                end = len(data) - 1
            else:
                end = indices[pos + 1]
            patch_areas.append([begin, end])

    data = patch_data(data, patch_areas, "\n" + PATCH_PREFIX, "\n" + PATCH_POSTFIX)

    with open(path_to_patch, "w") as file:
        file.write(data)


def patch_pyopencv(modules_dir):
    # why do we need to patch generated code: we make CUDA modules optional
    # but in generated code for python bindings everything is in one place, so we wont be able to find definitions for symbols from CUDA modules
    # solution:
    # 1. make includes into CUDA modules conditional
    # 2. process these includes as they declare exported classes, functions and enums, list all of them
    # 3. make everything we processed conditional (#ifdef HAVE_CUDA ... #endif)

    bindgings_dir = P.join(modules_dir, "python_bindings_generator")
    generated_includes_path = P.join(bindgings_dir, "pyopencv_generated_include.h")
    files_to_process = patch_generated_includes(generated_includes_path)
    files_to_process = [P.join(modules_dir, path) for path in files_to_process]

    enums, enum_values, functions, classes, methods = find_exported_symbols(files_to_process)

    patch_lines_with_symbols(P.join(bindgings_dir, "pyopencv_generated_enums.h"), enums)

    function_names = []
    for name in functions:
        function_names.append(f"pyopencv_cv_cuda_{name}")
    for item in methods:
        function_names.append(f"pyopencv_cv_cuda_{item[0]}_{item[1]}")

    patch_function_definitions(P.join(bindgings_dir, "pyopencv_generated_funcs.h"), function_names)

    candidates = function_names + enum_values
    patch_lines_with_symbols(P.join(bindgings_dir, "pyopencv_generated_modules_content.h"), candidates)
    patch_class_declarations(P.join(bindgings_dir, "pyopencv_generated_types.h"), classes)

    class_names = [f"cuda_{name}" for name in classes]
    patch_class_definitions(P.join(bindgings_dir, "pyopencv_generated_types_content.h"), class_names)


def cv_cuda_config():
    # CUDA_ARCH_BIN, CUDA_ARCH_FEATURES and CUDA_ARCH_PTX are used by
    # OpenCV dnn module (which is not added yet) and PCL (which requires >=12)
    # set them to 3.0 (lowest compute capability for CUDA 10.2)
    return """IF (NOT HAVE_CUDA)
    CFLAGS(
        -DCUDA_ARCH_BIN=\\"\\"
        -DCUDA_ARCH_FEATURES=\\"\\"
        -DCUDA_ARCH_PTX=\\"\\"
    )
ELSE()
    INCLUDE(${ARCADIA_ROOT}/library/cpp/cuda/wrappers/default_nvcc_flags.make.inc)
    CFLAGS(
        -DHAVE_CUDA
        -DHAVE_CUBLAS
        -DHAVE_CUFFT
        -DHAVE_OPENCV_CUDAARITHM
        -DHAVE_OPENCV_CUDAFILTERS
        -DHAVE_OPENCV_CUDAIMGPROC
        -DHAVE_OPENCV_CUDALEGACY
        -DHAVE_OPENCV_CUDAOPTFLOW
        -DHAVE_OPENCV_CUDAWARPING
        -DHAVE_OPENCV_CUDEV
        -DCUDA_ARCH_BIN=\\"30\\"
        -DCUDA_ARCH_FEATURES=\\"30\\"
        -DCUDA_ARCH_PTX=\\"30\\"
    )
ENDIF()
"""


def opencv_post_install(self):
    # fix headers
    re_sub_dir(self.dstdir, r"/var/empty/tmp/source/(.*\.hpp)", r"contrib/libs/opencv/\1")
    re_sub_dir(
        self.dstdir,
        r"/var/empty/openblas-[^/]*/include/cblas.h",
        r"contrib/libs/cblas/include/cblas.h",
    )
    re_sub_dir(
        self.dstdir,
        r"/var/empty/openblas-[^/]*/include/lapacke.h",
        r"contrib/libs/eigen/Eigen/src/misc/lapacke.h",
    )
    re_sub_file(
        P.join(self.dstdir, "include/opencv2/opencv.hpp"),
        r'#include "(opencv2/.*\.hpp)"',
        r"#include <contrib/libs/opencv/include/\1>",
    )
    re_sub_file(
        P.join(self.dstdir, "include/opencv2/opencv.hpp"),
        r"#include <contrib/libs/opencv/include/opencv2/stitching.hpp>",
        r"// #include <contrib/libs/opencv/include/opencv2/stitching.hpp>",
    )

    for name in [name for name in REGULAR_MODULES if name not in HEAVY_MODULES + CUDA_MODULES]:
        re_sub_dir(
            P.join(self.dstdir, f"modules/{name}"),
            r'#\s*include\s+["<]opencv2/([^/">]+\.hpp)[>"]',
            r"#include <contrib/libs/opencv/include/opencv2/\1>",
            test=lambda x: re.match(r"modules/[^/]+/include/opencv2/.+\.(h|hpp)", P.relpath(x, self.dstdir)),
        )

    re_sub_dir(
        P.join(self.dstdir, "modules"),
        r'#\s*include\s+["<]opencv2/([^/">]+)/((?:[^/">]+/)*)([^">]+)[">]',
        r"#include <contrib/libs/opencv/modules/\1/include/opencv2/\1/\2\3>",
        test=lambda x: re.match(r"modules/[^/]+/include/opencv2/.+\.(h|hpp)", P.relpath(x, self.dstdir)),
    )

    re_sub_file(
        P.join(self.dstdir, "cvconfig-linux.h"),
        r"#define (CUDA_ARCH_BIN|CUDA_ARCH_FEATURES|CUDA_ARCH_PTX|HAVE_CUDA|HAVE_CUBLAS|HAVE_CUFFT)",
        r"// #define \1",
    )

    re_sub_file(
        P.join(self.dstdir, "opencv2/opencv_modules.hpp"),
        r"#define HAVE_OPENCV_(CUDAARITHM|CUDAFILTERS|CUDAIMGPROC|CUDALEGACY|CUDAOPTFLOW|CUDAWARPING|CUDEV)",
        r"// #define HAVE_OPENCV_\1",
    )

    patch_pyopencv(P.join(self.dstdir, "modules"))

    with open(P.join(self.dstdir, "cv_cuda_config.inc"), "w") as file:
        file.write(cv_cuda_config())

    with self.yamakes["modules/videoio"] as videoio:
        videoio.CFLAGS.remove("-DHAVE_OBSENSOR")
        videoio.CFLAGS.remove("-DHAVE_OBSENSOR_V4L2")
        videoio.after(
            "CFLAGS",
            Switch(
                OS_LINUX=Linkable(
                    CFLAGS=[
                        "-DHAVE_OBSENSOR",
                        "-DHAVE_OBSENSOR_V4L2",
                    ]
                )
            ),
        )

    def valid_cflag(flag):
        if (
            flag.startswith("-DCV_CPU_")
            or flag.startswith("-D__CUDA_ARCH_LIST__")
            or flag.startswith("-DHAVE_NVIDIA_OPTFLOW")
        ):
            return False
        return flag not in ["-DNVCC", "-D_FORCE_INLINES", "-D__NVCC_DIAG_PRAGMA_SUPPORT__=1"]

    for n, m in self.yamakes.items():
        if not isinstance(m, Linkable):
            continue

        m.CFLAGS = list(x for x in m.CFLAGS if valid_cflag(x))

        if n not in MODULES_WITHOUT_CUDA_SUPPORT:
            m.INCLUDE = "${ARCADIA_ROOT}/contrib/libs/opencv/cv_cuda_config.inc"

        (
            avx_srcs,
            avx2_srcs,
            avx512_srcs,
            sse4_1_srcs,
            sse4_2_srcs,
            ssse3_srcs,
            fp16_srcs,
            sse2_srcs,
            sse3_srcs,
        ) = ([], [], [], [], [], [], [], [], [])
        m_options = {}
        for src in m.SRCS:
            parts = src.split(".")
            if len(parts) > 2:
                if parts[-2] == "avx":
                    avx_srcs.append(src)
                    m_options["M_AVX"] = "-mavx"
                if parts[-2] == "avx2":
                    avx2_srcs.append(src)
                    m_options["M_AVX2"] = "-mavx2"
                    m_options["M_FMA"] = "-mfma"
                    m_options["M_F16C"] = "-mf16c"
                if parts[-2] == "avx512_skx":
                    avx512_srcs.append(src)
                    m_options["M_AVX512"] = "-mavx512f -mavx512cd -mavx512vl -mavx512bw -mavx512dq"
                    m_options["M_AVX"] = "-mavx"
                    m_options["M_SSSE3"] = "-mssse3"
                    m_options["M_SSE42"] = "-msse4.2"
                if parts[-2] == "fp16":
                    fp16_srcs.append(src)
                    m_options["M_F16C"] = "-mf16c"
                    m_options["M_POPCNT"] = "-mpopcnt"
                    m_options["M_SSSE3"] = "-mssse3"
                    m_options["M_SSE41"] = "-msse4.1"
                    m_options["M_SSE42"] = "-msse4.2"
                if parts[-2] == "sse2":
                    sse2_srcs.append(src)
                if parts[-2] == "sse3":
                    sse3_srcs.append(src)
                if parts[-2] == "ssse3":
                    ssse3_srcs.append(src)
                    m_options["M_SSSE3"] = "-mssse3"
                    m_options["M_SSE41"] = "-msse4.1"
                if parts[-2] == "sse4_1":
                    sse4_1_srcs.append(src)
                    m_options["M_SSE41"] = "-msse4.1"
                if parts[-2] == "sse4_2":
                    sse4_2_srcs.append(src)
                    m_options["M_SSE42"] = "-msse4.2"
        for src in (
            avx_srcs
            + avx2_srcs
            + avx512_srcs
            + ssse3_srcs
            + sse4_1_srcs
            + sse4_2_srcs
            + fp16_srcs
            + sse2_srcs
            + sse3_srcs
        ):
            m.SRCS.remove(src)
        if m_options:
            os_win, not_os_win = [], []
            for m_opt in sorted(m_options):
                os_win.append(f'    SET({m_opt} "")')
                not_os_win.append(f"    SET({m_opt} {m_options[m_opt]})")
            m.after(
                "SRCS",
                "IF (NOT OS_WINDOWS OR CLANG_CL)\n{}\nELSE()\n{}\nENDIF()\n".format(
                    "\n".join(not_os_win), "\n".join(os_win)
                ),
            )

        i386_srcs = []
        x86_64_srcs = []
        for src in avx_srcs:
            x86_64_srcs.append(f"{src} $M_AVX -DCV_CPU_DISPATCH_MODE=AVX -DCV_CPU_COMPILE_AVX=1")
        for src in avx2_srcs:
            x86_64_srcs.append(
                "{} $M_AVX2 $M_FMA $M_F16C -DCV_CPU_COMPILE_AVX2=1 -DCV_CPU_COMPILE_AVX=1 -DCV_CPU_COMPILE_FMA3=1"
                " -DCV_CPU_COMPILE_FP16=1 -DCV_CPU_COMPILE_POPCNT=1 -DCV_CPU_COMPILE_SSE4_1=1 -DCV_CPU_COMPILE_SSE4_2=1 -DCV_CPU_COMPILE_SSSE3=1 -DCV_CPU_DISPATCH_MODE=AVX2".format(
                    src
                )
            )
        for src in avx512_srcs:
            x86_64_srcs.append(
                f"{src} $M_AVX512 $M_AVX $M_AVX2 $M_FMA $M_F16C $M_SSSE3 $M_SSE41 $M_SSE42 -DCV_CPU_COMPILE_AVX2=1 -DCV_CPU_COMPILE_AVX512_COMMON=1 -DCV_CPU_COMPILE_AVX512_SKX=1"
                " -DCV_CPU_COMPILE_AVX=1 -DCV_CPU_COMPILE_AVX_512F=1 -DCV_CPU_COMPILE_FMA3=1 -DCV_CPU_COMPILE_FP16=1 -DCV_CPU_COMPILE_POPCNT=1 -DCV_CPU_COMPILE_SSE4_1=1 "
                "-DCV_CPU_COMPILE_SSE4_2=1 -DCV_CPU_COMPILE_SSSE3=1 -DCV_CPU_DISPATCH_MODE=AVX512_SKX"
            )
        for src in fp16_srcs:
            x86_64_srcs.append(
                "{} $M_SSSE3 $M_SSE41 $M_POPCNT $M_SSE42 $M_F16C -DCV_CPU_COMPILE_AVX=1 -DCV_CPU_COMPILE_FP16=1"
                " -DCV_CPU_COMPILE_POPCNT=1 -DCV_CPU_COMPILE_SSE4_1=1 -DCV_CPU_COMPILE_SSE4_2=1 -DCV_CPU_COMPILE_SSSE3=1 -DCV_CPU_DISPATCH_MODE=FP16".format(
                    src
                )
            )
        for src in sse2_srcs:
            i386_srcs.append(f"{src} -DCV_CPU_DISPATCH_MODE=SSE2")
        for src in sse3_srcs:
            x86_64_srcs.append(f"{src} -DCV_CPU_DISPATCH_MODE=SSE3")
        for src in ssse3_srcs:
            x86_64_srcs.append(
                f"{src} $M_SSSE3 $M_SSE41 -DCV_CPU_COMPILE_SSE4_1=1 -DCV_CPU_COMPILE_SSSE3=1 -DCV_CPU_DISPATCH_MODE=SSSE3"
            )
        for src in sse4_1_srcs:
            x86_64_srcs.append(
                f"{src} $M_SSE41 -DCV_CPU_COMPILE_SSE4_1=1 -DCV_CPU_COMPILE_SSSE3=1 -DCV_CPU_DISPATCH_MODE=SSE4_1"
            )
        for src in sse4_2_srcs:
            x86_64_srcs.append(
                f"{src} $M_SSE42 -DCV_CPU_COMPILE_SSE4_1=1 -DCV_CPU_COMPILE_SSE4_2=1 -DCV_CPU_COMPILE_SSSE3=1 -DCV_CPU_DISPATCH_MODE=SSE4_2"
            )
        if i386_srcs:
            m.after(
                "SRCS",
                Switch({"ARCH_I386 OR ARCH_X86_64": Linkable(SRC=[s.split() for s in i386_srcs])}),
            )
        if x86_64_srcs:
            m.after(
                "SRCS",
                Switch(ARCH_X86_64=Linkable(SRC=[s.split() for s in x86_64_srcs])),
            )

    with self.yamakes["modules/core"] as m:
        m.PEERDIR.add("contrib/libs/clapack")
        m.PEERDIR.add("library/cpp/sanitizer/include")
        m.CFLAGS.remove("-DHAVE_MEMALIGN=1")
        m.CFLAGS.remove("-DHAVE_POSIX_MEMALIGN=1")
        m.after(
            "CFLAGS",
            Switch({"NOT OS_WINDOWS": Linkable(CFLAGS=["-DHAVE_MEMALIGN=1", "-DHAVE_POSIX_MEMALIGN=1"])}),
        )
        if "contrib/libs/eigen" in m.ADDINCL:
            m.ADDINCL.remove("contrib/libs/eigen")
            m.ADDINCL.add(GLOBAL("contrib/libs/eigen"))
    with self.yamakes["modules/calib3d"] as m:
        m.after(
            "NO_SANITIZE",
            Switch({'SANITIZER_TYPE == "undefined"': "NO_SANITIZE()  # disabled due to huge compilation time"}),
        )

    with self.yamakes["modules/features2d"] as m:
        m.SRCS.add("src/mser_old.cpp")

    with self.yamakes["modules/imgproc"] as m:
        if "src/lsd.cpp" in m.SRCS:
            m.SRCS.remove("src/lsd.cpp")
            m.SRCS.add("src/lsd_old.cpp")

    with self.yamakes["modules/cudaoptflow"] as m:
        for pyrlk_type in ["float", "int", "u8", "u16"]:
            for pyrlk_cn in ["1", "3", "4"]:
                m.SRCS.add(f"src/cuda/pyrlk.{pyrlk_type}.{pyrlk_cn}.cu")

    for module in ["modules/" + name for name in CUDA_MODULES]:
        with self.yamakes[module] as m:
            m.EXTRALIBS = [
                "-lnppc_static",
                "-lnppial_static",
                "-lnppicc_static",
                "-lnppidei_static",
                "-lnppif_static",
                "-lnppig_static",
                "-lnppim_static",
                "-lnppist_static",
                "-lnppitc_static",
            ]
            m.PEERDIR.add("contrib/libs/nvidia/cublas")
            m.PEERDIR.add("contrib/libs/nvidia/cufft")

    python_cv2 = self.yamakes[f"modules/{PY_BINDING_MODULE}"]
    with python_cv2:
        python_cv2.to_py_library(
            module="PY23_LIBRARY",
            PY_REGISTER=["cv2"],
        )

    with self.yamakes["."] as m:
        android_filtered = {x for x in m.RECURSE if x in (f"modules/{PY_BINDING_MODULE}",)}
        if android_filtered:
            m.RECURSE = list(x for x in m.RECURSE if x not in android_filtered)
        self.yamakes["."] = self.module(
            Library,
            PEERDIR=[
                f"contrib/libs/opencv/modules/{module}" for module in REGULAR_MODULES if module not in HEAVY_MODULES
            ],
            ADDINCL={GLOBAL("contrib/libs/opencv/include")},
            RECURSE=m.RECURSE,
        )
        self.yamakes["."].after(
            "RECURSE",
            Switch({"NOT ARCH_ARM AND NOT OS_ANDROID": Recursable(RECURSE=android_filtered)}),
        )

    def is_conditional_src(src):
        return src.endswith(".cu") or src.endswith(".cuh")

    for module_name in ["."] + [f"modules/{name}" for name in [PY_BINDING_MODULE] + REGULAR_MODULES]:
        with self.yamakes[module_name] as m:
            conditional_peerdir = []
            for target in ["build/internal/platform/cuda"] + [
                "contrib/libs/opencv/modules/" + name for name in CUDA_MODULES
            ]:
                if target in m.PEERDIR:
                    conditional_peerdir.append(target)
                    m.PEERDIR.remove(target)

            conditional_recurse = []
            for target in [f"modules/{name}" for name in CUDA_MODULES]:
                if target in m.RECURSE:
                    conditional_recurse.append(target)
                    m.RECURSE.remove(target)

            conditional_src = []
            for src in m.SRCS:
                if is_conditional_src(src):
                    conditional_src.append(src)
            m.SRCS.difference_update(conditional_src)

            if not conditional_peerdir and not conditional_recurse and not conditional_src:
                continue

            if module_name in MODULES_WITHOUT_CUDA_SUPPORT:
                continue

            if conditional_peerdir:
                m.after(
                    "PEERDIR",
                    Switch({"HAVE_CUDA": "PEERDIR(\n{}\n)".format("\n    ".join(sorted(conditional_peerdir)))}),
                )

            if conditional_recurse:
                m.after("RECURSE", Switch({"HAVE_CUDA": Recursable(RECURSE=sorted(conditional_recurse))}))

            if conditional_src:
                m.after(
                    "SRCS",
                    Switch({"HAVE_CUDA": "SRCS(\n{}\n)".format("\n    ".join(sorted(conditional_src)))}),
                )

    for module_name in CUDA_MODULES + HEAVY_MODULES:
        with self.yamakes[f"modules/{module_name}"] as m:
            m.ADDINCL.add(GLOBAL(f"contrib/libs/opencv/modules/{module_name}/include"))


opencv_inclink = {
    "include/opencv2": ["opencv2/opencv_modules.hpp"]
    + [f"modules/{name}/include/opencv2/*.hpp" for name in REGULAR_MODULES if name not in HEAVY_MODULES],
    "include/opencv2/core/hal": [
        "modules/core/include/opencv2/core/hal/*.h",
        "modules/core/include/opencv2/core/hal/*.hpp",
    ],
}


for pth in (
    "/core",
    "/imgproc",
    "/calib3d",
    "/features2d",
    "/flann",
    "/highgui",
    "/imgcodecs",
    "/ml",
    "/objdetect",
    "/photo",
    "/shape",
    "/video",
    "/videoio",
    "/videostab",
):
    opencv_inclink["include/opencv2" + pth] = [
        f"modules{pth}/include/opencv2{pth}/*.h",
        f"modules{pth}/include/opencv2{pth}/*.hpp",
    ]

opencv_inclink["include/opencv2/imgcodecs/legacy"] = ["modules/imgcodecs/include/opencv2/imgcodecs/legacy/*.h"]

opencv = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/opencv",
    nixattr=python.make_nixattr("opencv4"),
    post_install=opencv_post_install,
    inclink=opencv_inclink,
    keep_paths=[
        "modules/imgproc/src/lsd_old.cpp",
        "modules/features2d/src/mser_old.cpp",
    ],
    unbundle_from={
        "opencl": "3rdparty/include/opencl/1.2",
    },
    disable_includes=[
        # macro includes
        "OPENCV_INCLUDE_PORT_FILE",
        "OPENCV_STDINT_HEADER",
        "acxx_demangle.h",
        "ittnotify.h",
        "tegra_round.hpp",
        "ippversion.h",
        "ippicv.h",
        "iw++/iw.hpp",
        "iw/iw_ll.h",
        "opencv2/core/core_tegra.hpp",
        "directx.inc.hpp",
        "tbb/task_arena.h",
        "C=.h",
        # ifdef HAVE_JPEGXL
        "jxl/encode_cxx.h",
        "jxl/decode_cxx.h",
        "jxl/version.h",
        "jxl/thread_parallel_runner_cxx.h",
        # ifdef HAVE_SPNG
        "spng.h",
        # ifdef HAVE_AVIF
        "avif/avif.h",
        "ImfHeader.h",
        "ImfInputFile.h",
        "ImfOutputFile.h",
        "ImfChannelList.h",
        "half.h",
        "ImfChromaticities.h",
        "ImfStandardAttributes.h",
        "ImathBox.h",
        "opencv2/highgui/highgui_tegra.hpp",
        # contrib/libs/gdal
        "cpl_conv.h",
        "gdal_priv.h",
        "gdal.h",
        # end
        "lasxintrin.h",
        "lsxintrin.h",
        "gdcmImageReader.h",
        "jasper/jasper.h",
        "libpng/png.h",  # libpng
        "opencv2/imgproc/imgproc_tegra.hpp",
        "pxcsession.h",
        "pxcsmartptr.h",
        "pxccapture.h",
        "agile.h",
        "absl/strings/string_view.h",
        "absl/types/optional.h",
        "absl/types/variant.h",
        "absl/debugging/failure_signal_handler.h",
        "absl/debugging/stacktrace.h",
        "absl/debugging/symbolize.h",
        "absl/strings/str_cat.h",
        "emscripten/version.h",
        "lib/fdio/io.h",
        "lib/fdio/spawn.h",
        "zircon/processargs.h",
        "zircon/syscalls/port.h",
        "zircon/process.h",
        "opencv2/calib3d/calib3d_tegra.hpp",
        "opencv2/features2d/features2d_tegra.hpp",
        "opencv2/objdetect/objdetect_tegra.hpp",
        "opencv2/photo/photo_tegra.hpp",
        "opencv2/video/video_tegra.hpp",
        "boost/dynamic_bitset.hpp",
        "opencv2/aruco/charuco.hpp",
        "opencv2/cudafeatures2d.hpp",
        "opencv2/stitching/stitching_tegra.hpp",
        "opencv2/xfeatures2d/cuda.hpp",
        "opencv2/xfeatures2d.hpp",
        "opencv2/cudacodec.hpp",
        "opencv2/dnn.hpp",
        "opencv2/viz.hpp",
        "opencv2/cudabgsegm.hpp",
        "opencv2/cudaobjdetect.hpp",
        "opencv2/cudastereo.hpp",
        "ClpSimplex.hpp",
        "ClpPresolve.hpp",
        "ClpPrimalColumnSteepest.hpp",
        "ClpDualRowSteepest.hpp",
        "_modelest.h",
        "Eigen/Array",
        "EGL/egl.h",
        "tbb/tbb.h",
        "tbb/task.h",
        "tbb/tbb_stddef.h",
        "OpenCL/cl_platform.h",
        # if USE_JPIP
        "cidx_manager.h",
        "indexbox_manager.h",
        # if USE_JPWL
        "openjpwl/jpwl.h",
        # if CV_CPU_COMPILE_MSA
        "hal/msa_macros.h",
        # if __EMSCRIPTEN__
        "wasm_simd128.h",
        # if CV_CPU_COMPILE_RVV
        "riscv_vector.h",
        "riscv-vector.h",
        # if OPENCV_CORE_OCL_RUNTIME_CLAMDFFT_HPP
        "clFFT.h",
        # if OPENCV_CORE_OCL_RUNTIME_CLAMDBLAS_HPP
        "clBLAS.h",
        # if HAVE_HPX
        "hpx/parallel/algorithms/for_loop.hpp",
        "hpx/parallel/execution.hpp",
        "hpx/hpx_start.hpp",
        "hpx/hpx_suspend.hpp",
        "hpx/include/apply.hpp",
        "hpx/util/yield_while.hpp",
        "hpx/include/threadmanager.hpp",
        "hpx/hpx_main.hpp",
        # if __QNX__
        "sys/elf.h",
        "sys/neutrino.h",
        "sys/syspage.h",
        "elfdefinitions.h",
        # if __aarch64__
        "aarch64/syspage.h",
        # if HAVE_VA and not OPENCV_LIBVA_LINK
        "va_wrapper.impl.hpp",
        # if HAVE_OPENEXR
        "ImfFrameBuffer.h",
        "ImfRgbaFile.h",
        "OpenEXRConfig.h",
        "test_exr.impl.hpp",
        # if HAVE_LIBREALSENSE
        "librealsense2/rs.hpp",
        # if HAVE_OPENCV_XFEATURES2D
        "opencv2/xfeatures2d/nonfree.hpp",
        # if defined(HAVE_OBSENSOR_MSMF)
        "obsensor_stream_channel_msmf.hpp",
    ],
    ignore_commands={
        "python2.7",
    },
    copy_sources=[
        "include/opencv2/opencv.hpp",
        "modules/core/include/**/*.hpp",
        "modules/core/src/ocl_deprecated.hpp",
        "modules/core/src/opencl/runtime/**/*.hpp",
        "modules/videoio/src/*.hpp",
        "modules/imgcodecs/include/**/legacy/*.h",
        "modules/core/src/ocl_disabled.impl.hpp",
        "modules/calib3d/include/opencv2/calib3d/calib3d.hpp",
        "modules/highgui/include/opencv2/highgui/highgui.hpp",
        "modules/imgcodecs/include/opencv2/imgcodecs/imgcodecs.hpp",
        "modules/imgproc/include/opencv2/imgproc/imgproc.hpp",
    ],
    platform_dispatchers=[
        # cv_cpu_config.h should be dispatched on CPU basis
        "cv_cpu_config.h",
        # cvconfig.h should be dispatched on OS basis
        "cvconfig.h",
    ],
)
