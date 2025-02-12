import os

from devtools.yamaker.fileutil import re_sub_dir
from devtools.yamaker.modules import Switch, Linkable
from devtools.yamaker.project import NixProject


def openldap_post_install(self):
    re_sub_dir(
        os.path.join(self.dstdir, "include"),
        r"/\* Generated from ./ldap_config.hin on .* \*/",
        "/* Generated from ./ldap_config.hin on */",
    )
    self.yamakes.make_recursive(recurse_from_modules=True)
    with self.yamakes["libraries/liblmdb"] as m:
        m.after("CFLAGS", Switch({'DEFINED LMDB_IDL_LOGN': Linkable(CFLAGS=["-DMDB_IDL_LOGN=${LMDB_IDL_LOGN}"])}))
        m.after("CFLAGS", Switch(OS_ANDROID=Linkable(CFLAGS=["-DMDB_USE_ROBUST=0"])))


openldap = NixProject(
    arcdir="contrib/libs/openldap",
    nixattr="openldap",
    ignore_commands={"../../build/mkversion", "cat", "echo", "sed"},
    put={
        "ldap": ".",
        "lmdb": "libraries/liblmdb",
        "lber": "libraries/liblber",
    },
    platform_dispatchers=[
        "include/portable.h",
    ],
    addincl_global={".": {"./include"}},
    disable_includes=[
        "ac/regex.h",
        "asm/cachectl.h",
        "getopt-compat.h",
        "gnutls/",
        "libmalloc.h",
        "nspr/",
        "nss/",
        "ssl.h",
        "synch.h",
        "tklib.h",
    ],
    post_install=openldap_post_install,
)
