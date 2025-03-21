import os.path
import itertools

from devtools.yamaker import boost, fileutil
from devtools.yamaker.modules import GLOBAL, Library, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    # give masm sources the extension expected by ymake
    for p in fileutil.listdir(os.path.join(self.dstdir, "src", "asm")):
        # match both masm and armasm
        if p.endswith("masm.asm"):
            fname = os.path.basename(p)
            fname_wo_ext, _ = os.path.splitext(fname)
            fileutil.rename(p, fname_wo_ext + ".masm")

    self.yamakes["."] = self.module(
        Library,
        # duplicated for visibility
        ADDINCL=[GLOBAL(os.path.join(self.arcdir, "include"))],
        RECURSE=["impl_common", "fcontext_impl", "ucontext_impl"],
    )
    with self.yamakes["."] as context:
        context.after(
            "PEERDIR",
            Switch(
                {
                    "SANITIZER_TYPE": Linkable(PEERDIR=[os.path.join(self.arcdir, "ucontext_impl")]),
                    "default": Linkable(PEERDIR=[os.path.join(self.arcdir, "fcontext_impl")]),
                }
            ),
        )

    self.yamakes["impl_common"] = boost.make_library(
        self,
        LICENSE=["BSL-1.0"],
        SRCDIR=[os.path.join(self.arcdir, "src")],
        CFLAGS=["-DBOOST_CONTEXT_SOURCE"],
    )
    with self.yamakes["impl_common"] as impl_common:
        impl_common.after("CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_CONTEXT_DYN_LINK")])}))
        impl_common.after(
            "SRCS",
            Switch(
                {
                    "OS_WINDOWS": Linkable(SRCS=["windows/stack_traits.cpp"]),
                    "default": Linkable(SRCS=["posix/stack_traits.cpp"]),
                }
            ),
        )

    self.yamakes["ucontext_impl"] = self.module(
        Library,
        LICENSE=["BSL-1.0"],
        # PROVIDES=["boost_context_impl"], -- introduce after breaking asio dependency on fcontext
        PEERDIR=[os.path.join(self.arcdir, "impl_common")],
        ADDINCL=[os.path.join(self.arcdir, "include")],
        SRCDIR=[os.path.join(self.arcdir, "src")],
        CFLAGS=[GLOBAL("-DBOOST_USE_UCONTEXT"), "-DBOOST_CONTEXT_SOURCE"],
        SRCS=["continuation.cpp", "fiber.cpp"],
        NO_UTIL=True,
        NO_COMPILER_WARNINGS=True,
    )
    with self.yamakes["ucontext_impl"] as ucontext_impl:
        ucontext_impl.after(
            "CFLAGS",
            Switch(
                {
                    'SANITIZER_TYPE == "address"': Linkable(CFLAGS=[GLOBAL("-DBOOST_USE_ASAN")]),
                    'SANITIZER_TYPE == "thread"': Linkable(CFLAGS=[GLOBAL("-DBOOST_USE_TSAN")]),
                }
            ),
        )
        ucontext_impl.PEERDIR.add("library/cpp/sanitizer/include")

    self.yamakes["fcontext_impl"] = self.module(
        Library,
        LICENSE=["BSL-1.0"],
        PEERDIR=[os.path.join(self.arcdir, "impl_common")],
        SRCDIR=[self.arcdir],
        NO_UTIL=True,
        NO_COMPILER_WARNINGS=True,
    )

    with self.yamakes["fcontext_impl"] as fcontext_impl:
        fcontext_impl.after(
            "CFLAGS",
            Switch(
                {
                    "OS_WINDOWS": Switch(
                        {
                            "DYNAMIC_BOOST": Linkable(MASMFLAGS=["/DBOOST_CONTEXT_EXPORT=EXPORT"]),
                            "default": Linkable(MASMFLAGS=["/DBOOST_CONTEXT_EXPORT="]),
                        }
                    )
                }
            ),
        )
        fcontext_impl.after(
            "SRCDIR",
            """
            IF (OS_WINDOWS AND ARCH_I386)
                MASMFLAGS(/safeseh)
            ENDIF()

            IF (ARCH_I386)
                SET(FCONTEXT_ARCH "i386")
            ELSEIF (ARCH_X86_64)
                SET(FCONTEXT_ARCH "x86_64")
            ELSEIF (ARCH_ARM64)
                SET(FCONTEXT_ARCH "arm64")
            ELSEIF (ARCH_ARM)
                SET(FCONTEXT_ARCH "arm")
            ENDIF()

            IF (OS_WINDOWS)
                SET(FCONTEXT_ABI ms)
            ELSEIF (ARCH_ARM64 OR ARCH_ARM)
                SET(FCONTEXT_ABI aapcs)
            ELSE (OS_LINUX OR OS_ANDROID)
                SET(FCONTEXT_ABI sysv)
            ENDIF()

            IF (OS_DARWIN OR OS_IOS)
                SET(FCONTEXT_FMT macho)
                SET(FCONTEXT_SUF gas.S)
            ELSEIF (OS_LINUX OR OS_ANDROID)
                SET(FCONTEXT_FMT elf)
                SET(FCONTEXT_SUF gas.S)
            ELSEIF (OS_WINDOWS AND ARCH_ARM64)
                SET(FCONTEXT_FMT pe)
                SET(FCONTEXT_SUF asmasm.masm)
            ELSEIF (OS_WINDOWS)
                SET(FCONTEXT_FMT pe)
                SET(FCONTEXT_SUF masm.masm)
            ENDIF()
            """,
        )
        fcontext_impl.after(
            "SRCDIR",
            """
            SRCS(
                src/asm/make_${FCONTEXT_ARCH}_${FCONTEXT_ABI}_${FCONTEXT_FMT}_${FCONTEXT_SUF}
                src/asm/jump_${FCONTEXT_ARCH}_${FCONTEXT_ABI}_${FCONTEXT_FMT}_${FCONTEXT_SUF}
                src/asm/ontop_${FCONTEXT_ARCH}_${FCONTEXT_ABI}_${FCONTEXT_FMT}_${FCONTEXT_SUF}
                src/fcontext.cpp
            )
            """,
        )


FMT_ASM = [
    f"{fmt}_{asm}"
    for fmt, asm in zip(
        ("elf", "macho", "pe", "pe"),
        # Windows on arm64 requires custom assember armasm, handle it
        ("gas.S", "gas.S", "masm.asm", "armasm.asm"),
    )
]


boost_context = NixSourceProject(
    nixattr="boost_context",
    arcdir=boost.make_arcdir("context"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    # fmt: off
    copy_sources=[
        "include/boost/",
        "src/*.cpp",
        "src/posix/stack_traits.cpp",
        "src/windows/stack_traits.cpp",
    ] + [
        f"src/asm/{cmd}_{arch}_{abi}_{fmt_asm}"
        for cmd, arch, abi, fmt_asm in itertools.product(
            ["jump", "make", "ontop"],
            ["arm", "arm64", "i386", "x86_64"],
            ["aapcs", "ms", "sysv"],
            FMT_ASM,
        )
    ],
    # fmt: on
    post_install=post_install,
)
