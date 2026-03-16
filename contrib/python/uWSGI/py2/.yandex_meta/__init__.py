from devtools.yamaker import python
from devtools.yamaker.fileutil import re_sub_dir
from devtools.yamaker.modules import py_srcs
from devtools.yamaker.pathutil import is_source
from devtools.yamaker.project import GNUMakeNixProject

uwsgi_yamake = """\
IF (OS_LINUX)
    IF (NOT MUSL)
        # For crypt_r in plugins/router_basicauth/router_basicauth.c.
        EXTRALIBS(crypt)
    ENDIF()
    SRCS(
        lib/linux_ns.c
    )
ENDIF()

IF (UWSGI_MAX_CRONS)
    CONLYFLAGS(
        GLOBAL
        -DMAX_CRONS=${UWSGI_MAX_CRONS}
    )
ENDIF()
"""


def post_build(self):
    # uwsgi uses CFLAGS values which are too complex to be written directly into ya.make
    # store them in config.h instead.
    #
    # NB: this has to be done during post_build in order
    # to rename given config to config-linux.h during platform dispatchers install.
    uwsgi = self.yamakes["."]
    with open(f"{self.dstdir}/config.h", "w") as config:
        config.write("#pragma once\n")
        for flag in uwsgi.CFLAGS:
            parsed_define = flag.removeprefix("-D").split("=", 1)
            if len(parsed_define) == 1:
                parsed_define.append("")

            name, value = parsed_define
            if name in ("UWSGI_DECLARE_EMBEDDED_PLUGINS", "UWSGI_LOAD_EMBEDDED_PLUGINS"):
                value = value.split(";")
                if value[0].startswith("UDEP"):
                    value.append("UDEP(stats_pusher_json)")
                else:
                    value.append("ULEP(stats_pusher_json)")
                value = "\\\n".join(f"\t{x};" for x in value if x)
                config.write(f"#define {name}\\\n{value}\n")
            elif value:
                config.write(f"#define {name} {value}\n")
            else:
                config.write(f"#define {name}\n")

    re_sub_dir(self.dstdir, "^", f"#include <{self.arcdir}/config.h>\n", is_source)
    uwsgi.CFLAGS = []
    self.nix_shell(self.builddir, 'eval "$yamakerPostBuild"')


def post_install(self):
    dist_files = python.extract_dist_info(self)

    self.yamakes["."].to_py_library(
        module="PY2_LIBRARY",
        PY_REGISTER=["pyuwsgi"],
        PY_SRCS=py_srcs(self.dstdir),
        NO_UTIL=True,
        NO_CHECK_IMPORTS=["uwsgidecorators"],
        RESOURCE_FILES=python.prepare_resource_files(self, *dist_files),
    )

    with self.yamakes["."] as m:
        m.SRCS.remove("lib/linux_ns.c")  # Linux-only
        m.SRCS.add("plugins/stats_pusher_json/plugin.c")  # Custom plugin.
        m.after("SRCS", uwsgi_yamake)


uwsgi = GNUMakeNixProject(
    owners=["g:python-contrib"],
    arcdir="contrib/python/uWSGI/py2",
    nixattr="uwsgi",
    makeflags=lambda self: [
        f"PROFILE={self.ctx.arc}/{self.arcdir}/.yandex_meta/buildconf.ini",
    ],
    disable_includes=[
        "kernel/OS.h",
        "libxml/",
        "pcre2.h",
        "port.h",
        "procfs.h",
        "sys/byteorder.h",
    ],
    post_build=post_build,
    post_install=post_install,
    copy_sources=["uwsgidecorators.py"],
    keep_paths=[
        "plugins/stats_pusher_json",
    ],
    platform_dispatchers=["config.h"],
)
