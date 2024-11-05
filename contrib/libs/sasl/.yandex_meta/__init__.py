import os.path as P

from devtools.yamaker.fileutil import copy
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    copy([P.join(self.dstdir, "include/")], P.join(self.dstdir, "include/sasl/"))


sasl = GNUMakeNixProject(
    arcdir="contrib/libs/sasl",
    nixattr="cyrus_sasl",
    copy_sources=[
        "include/*.h",
        "lib/staticopen.h",
    ],
    install_targets=[
        "sasl2",
        "plugin_common",
    ],
    put={
        "sasl2": ".",
    },
    put_with={
        "sasl2": ["plugin_common"],
    },
    ignore_commands=[
        "makemd5",
    ],
    disable_includes=[
        "des.h",
        "door.h",
        "gssapi/gssapi_ext.h",
        "sioux.h",
        "parse_cmd_line.h",
        "sasl_anonymous_plugin_decl.h",
        "sasl_crammd5_plugin_decl.h",
        "sasl_cram_plugin_decl.h",
        "sasl_digestmd5_plugin_decl.h",
        "sasl_gs2_plugin_decl.h",
        "sasl_gssapiv2_plugin_decl.h",
        "sasl_login_plugin_decl.h",
        "sasl_md5_plugin_decl.h",
        "sasl_otp_plugin_decl.h",
        "sasl_plain_plugin_decl.h",
        "sasl_scram_plugin_decl.h",
    ],
    platform_dispatchers=["config.h"],
    post_install=post_install,
)

sasl.copy_top_sources_except |= {
    "NTMakefile",
}
