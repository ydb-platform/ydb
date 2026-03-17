import os
import os.path

from devtools.yamaker.modules import Switch, Linkable
from devtools.yamaker.project import NixProject

# libpq implements some of the standard POSIX APIs for Windows
PORTS_FOR_WINDOWS = [
    "src/interfaces/libpq/pthread-win32.c",
    "src/interfaces/libpq/win32.c",
    "src/port/dirmod.c",
    "src/port/inet_aton.c",
    "src/port/open.c",
    "src/port/win32common.c",
    "src/port/win32gettimeofday.c",
    "src/port/win32gai_strerror.c",
    "src/port/win32error.c",
    "src/port/win32ntdll.c",
    "src/port/win32setlocale.c",
    "src/port/win32stat.c",
]

SRCS_FOR_SSE42 = [
    "src/port/pg_crc32c_sse42.c",
    "src/port/pg_crc32c_sse42_choose.c",
]

SRCS_FOR_AVX512 = [
    "src/port/pg_popcount_avx512.c",
]


def post_install(self):
    m = self.yamakes["."]
    # Provide strlcat / strlcpy
    m.PEERDIR.add("contrib/libs/libc_compat")

    # libpq has two implementations of pqsignal method. Removing one of them
    legacy_pqsignal_path = "src/interfaces/libpq/legacy-pqsignal.c"
    m.SRCS.remove(legacy_pqsignal_path)
    os.remove(os.path.join(self.dstdir, legacy_pqsignal_path))

    # Restore CFLAGS from the manually imported version
    m.CFLAGS = [
        "-DFRONTEND",
        "-DUNSAFE_STAT_OK",
        "-D_POSIX_PTHREAD_SEMANTICS",
        "-D_REENTRANT",
        "-D_THREAD_SAFE",
    ]
    # src/common/relpath.c includes "catalog/pg_tablespace_d.h",
    # while the file is located in src/backend/catalog/pg_tablespace_d.h"
    m.ADDINCL.append(os.path.join(self.arcdir, "src/backend"))
    m.SRCS -= set(SRCS_FOR_SSE42) | set(SRCS_FOR_AVX512)
    amd64_srcs = Linkable(
        SRCS=SRCS_FOR_SSE42,
    )
    for src in SRCS_FOR_AVX512:
        amd64_srcs.after(
            "SRCS",
            f"SRC_C_AVX512({src} -mavx512vpopcntdq)",
        )
    m.after(
        "SRCS",
        Switch(
            ARCH_X86_64=amd64_srcs,
        ),
    )
    m.after(
        "SRCS",
        Switch(
            OS_WINDOWS=Linkable(
                ADDINCL=[
                    os.path.join(self.arcdir, "src/include/port"),
                    os.path.join(self.arcdir, "src/include/port/win32"),
                    os.path.join(self.arcdir, "src/include/port/win32_msvc"),
                ],
                SRCS=PORTS_FOR_WINDOWS,
            )
        ),
    )


libpq = NixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libpq",
    nixattr="postgresql_15",  # Надо не забывать переодически поднимать тут версию
    ignore_commands=["bash", "perl", "sed", "sh"],
    install_targets=["pq"],
    put_with={
        # Merge pgport and pgcommn libraries into libpq,
        # as libpq itself is not functional without it,
        # and we do not need these libraries as standalone.
        "pq": {"pgcommon", "pgport"}
    },
    # libpq includes "postgres_ext.h" from "src/interfaces/libpq/libpq-fe.h"
    # using just `#include "postgres_ext.h", while the file is located in src/include.
    # Making ADDINCL for src/include GLOBAL.
    addincl_global={".": {"./src/include"}},
    copy_sources=[
        "src/include/port/*.h",
        "src/include/port/win32/**/*.h",
        "src/include/port/win32_msvc/**/*.h",
        "src/include/port/win32_port.h",
        "src/port/pthread-win32.h",
    ]
    + PORTS_FOR_WINDOWS,
    disable_includes=(
        "local.h",
        # These headers are related to BACKEND (server) part of the build
        "pgstat.h",
        "storage/fd.h",
        "utils/builtins.h",
        "utils/inet.h",
        "utils/memutils.h",
        "utils/resowner.h",
        "utils/resowner_private.h",
        # Unused (?) autorization algorithms?
        "fe-gssapi-common.h",
        "gssapi/",
        "gssapi_ext.h",
        "ldap.h",
        "libpq/pqsignal.h",
        "numa.h",
        "numaif.h",
        "common/unicode_normprops_table.h",
        "common/unicode_norm_hashfunc.h",
        "sys/procctl.h",
    ),
    platform_dispatchers=(
        "src/include/pg_config.h",
        "src/include/pg_config_os.h",
    ),
    post_install=post_install,
)
