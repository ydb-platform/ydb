from devtools.yamaker import fileutil, pathutil
from devtools.yamaker.modules import Switch, Linkable
from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.PEERDIR.add("library/cpp/sanitizer/include")
        m.ADDINCL.add(ArcPath(f"{self.arcdir}", FOR="asm"))

        arch_x86 = Linkable(SRCS=[])

        special_arch_sources = [
            # ends with, ya.make macro
            ("ssse3.c", "SRC_C_SSSE3"),
            ("sse2.c", "SRC_C_SSE2"),
            ("sse3.c", "SRC_C_SSE3"),
            ("sse4.c", "SRC_C_SSE4"),
            ("avx2.c", "SRC_C_AVX2"),
            ("avx.c", "SRC_C_AVX"),
        ]
        for src in sorted(m.SRCS):
            for postfix, ya_macro in special_arch_sources:
                if src.endswith(postfix):
                    m.SRCS.remove(src)
                    arch_x86.before("SRCS", f"{ya_macro}({src})")
                    break

        arch_x86_srcs = set(["aom_ports/float.asm"])
        for src in sorted(m.SRCS):
            if src.count("x86"):
                m.SRCS.remove(src)
                arch_x86_srcs.add(src)
        arch_x86.SRCS |= arch_x86_srcs

        def arm_test(filename):
            return (
                pathutil.is_source(filename)
                and not filename.endswith("dotprod.c")
                and not filename.endswith("i8mm.c")
                and not filename.endswith("sve.c")
            )

        arm_srcs = fileutil.files(self.srcdir + "/aom_dsp/arm", rel=self.srcdir, test=arm_test)

        m.before(
            "SRCS",
            Switch(ARCH_X86_64=arch_x86, ARCH_ARM64=Linkable(SRCS=arm_srcs)),
        )


aom = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/aom",
    nixattr="aom",
    ignore_commands=["sh", "perl"],
    post_install=post_install,
    install_targets=["aom"],
    copy_sources=[
        "aom_ports/float.asm",
        "aom_dsp/arm/*",
        "common/ivfdec.c",
        "common/ivfdec.h",
        "common/ivfenc.c",
        "common/ivfenc.h",
    ],
    disable_includes=[
        "AOM_MEM_PLTFRM",
        "aom_dsp/simd/v128_intrinsics_arm.h",
        "aom_dsp/simd/v128_intrinsics.h",
        "aom_util/debug_util.h",
        "av1/decoder/accounting.h",
        "av1/decoder/inspection.h",
        "av1/encoder/av1_temporal_denoiser.h",
        "av1/encoder/blockiness.h",
        "av1/encoder/deltaq4_model.c",
        "av1/encoder/saliency_map.h",
        "av1/encoder/thirdpass.h",
        "av1/encoder/tune_butteraugli.h",
        "av1/encoder/tune_vmaf.h",
        "lddk.h",
        "libyuv/mjpeg_decoder.h",
        "simd/v256_intrinsics_arm.h",
        "simd/v256_intrinsics.h",
        "tensorflow/lite/c/c_api.h",
    ],
    platform_dispatchers=[
        "config/aom_config.h",
        "config/aom_dsp_rtcd.h",
        "config/aom_scale_rtcd.h",
        "config/av1_rtcd.h",
    ],
)
