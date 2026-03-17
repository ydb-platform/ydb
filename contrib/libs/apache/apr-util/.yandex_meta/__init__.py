from devtools.yamaker.modules import GLOBAL
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as m:
        # -DLINUX is not used yet this define looks confusing
        m.CFLAGS.remove("-DLINUX")
        m.CFLAGS.append(GLOBAL("-DAPU_DECLARE_STATIC"))


apache_apr_util = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/apache/apr-util",
    nixattr="aprutil",
    cflags=["-DHAVE_ICONV_H"],
    disable_includes=[
        "apr_ldap_url.h",
        "apr_ldap_init.h",
        "apr_ldap_option.h",
        "apr_ldap_rebind.h",
        "apr_private.h",
        "db.h",
    ],
    makeflags=["libaprutil-1.la"],
    copy_sources=["include/apu_want.h"],
    platform_dispatchers=["include/private/apu_config.h"],
    post_install=post_install,
)

# NWGNU* are Makefile components
apache_apr_util.copy_top_sources_except |= {
    "NWGNUdbdmysql",
    "NWGNUdbdpgsql",
    "NWGNUdbdsqli2",
    "NWGNUdbdsqli3",
    "NWGNUdbmdb",
    "NWGNUdbmgdbm",
}
