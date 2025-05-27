import os

from devtools.yamaker.project import NixProject


def post_install(self):
    with self.yamakes["static"] as libaio:
        # Drop sources providing libaio==0.1 ABI compatibility,
        # as we build everything from the sources.
        os.remove(f"{self.dstdir}/src/compat-0_1.c")
        libaio.SRCS.remove("compat-0_1.c")

    self.make_dll_dispatcher(
        switch_flag="USE_AIO",
        switch_as_enum=True,
        handle_local=True,
        default_local_flags={
            "CFLAGS": ("USE_LOCAL_AIO_CFLAGS",),
            "LDFLAGS": ("USE_LOCAL_AIO_LDFLAGS", "-laio"),
        },
        # It is hard to maintain both static and dynamic linkage against versioned symbols.
        # We will create library with every symbol visible to the linker.
        exports_script=None,
        before_switch="""
            IF(EXPORT_CMAKE)
                OPENSOURCE_EXPORT_REPLACEMENT(
                    CMAKE AIO
                    CMAKE_TARGET AIO::aio
                )
            ENDIF()

            """,
        or_local_condition="EXPORT_CMAKE",
    )


libaio = NixProject(
    arcdir="contrib/libs/libaio",
    nixattr="libaio",
    put={
        "aio": "static",
    },
    inclink={
        "include": ["src/libaio.h"],
    },
    addincl_global={
        "static": ["../include"],
    },
    copy_sources=[
        "src/syscall-generic.h",
    ],
    disable_includes=[
        "syscall-alpha.h",
        "syscall-arm.h",
        "syscall-i386.h",
        "syscall-ia64.h",
        "syscall-ppc.h",
        "syscall-s390.h",
        "syscall-sparc.h",
    ],
    post_install=post_install,
)
