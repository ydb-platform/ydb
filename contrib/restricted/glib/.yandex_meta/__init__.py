import os

from devtools.yamaker import fileutil
from devtools.yamaker.modules import GLOBAL, Library, Linkable, Recursable, Switch
from devtools.yamaker.project import MesonNixProject

DARWIN_SRCS = {
    "gio/gcocoanotificationbackend.m",
    "gio/gnextstepsettingsbackend.m",
    "gio/gosxappinfo.m",
    "gio/gosxcontenttype.m",
    "gio/kqueue/gkqueuefilemonitor.c",
    "gio/kqueue/kqueue-helper.c",
    "gio/kqueue/kqueue-missing.c",
    "gio/kqueue/dep-list.c",
    "glib/gosxutils.m",
}

DARWIN_HEADERS = {
    "gio/gosxappinfo.h",
    "gio/kqueue/kqueue-helper.h",
    "gio/kqueue/dep-list.h",
}

LINUX_SRCS = {
    # inotify
    "gio/inotify/ginotifyfilemonitor.c",
    "gio/inotify/inotify-helper.c",
    "gio/inotify/inotify-kernel.c",
    "gio/inotify/inotify-missing.c",
    "gio/inotify/inotify-path.c",
    "gio/inotify/inotify-sub.c",
}

LINUX_ANDROID_SRCS = {
    # not have_cocoa
    "gio/gcontenttype.c",
    "gio/gdesktopappinfo.c",
    # if HAVE_NETLINK
    "gio/gnetworkmonitornetlink.c",
    "gio/gnetworkmonitornm.c",
}

UNIX_SRCS = {
    # unix_sources
    "gio/gfiledescriptorbased.c",
    "gio/giounix-private.c",
    "gio/gunixconnection.c",
    "gio/gunixcredentialsmessage.c",
    "gio/gunixfdlist.c",
    "gio/gunixfdmessage.c",
    "gio/gunixmount.c",
    "gio/gunixmounts.c",
    "gio/gunixsocketaddress.c",
    "gio/gunixvolume.c",
    "gio/gunixvolumemonitor.c",
    "gio/gunixinputstream.c",
    "gio/gunixoutputstream.c",
    "gio/gfdonotificationbackend.c",
    "gio/ggtknotificationbackend.c",
    # portal_sources
    "gio/gdocumentportal.c",
    "gio/gopenuriportal.c",
    "gio/gmemorymonitorportal.c",
    "gio/gnetworkmonitorportal.c",
    "gio/gproxyresolverportal.c",
    "gio/gtrashportal.c",
    "gio/gportalsupport.c",
    "gio/gportalnotificationbackend.c",
    "gio/xdp-dbus.c",
    # xdgmime
    "gio/xdgmime/xdgmime.c",
    "gio/xdgmime/xdgmimealias.c",
    "gio/xdgmime/xdgmimecache.c",
    "gio/xdgmime/xdgmimeglob.c",
    "gio/xdgmime/xdgmimeicon.c",
    "gio/xdgmime/xdgmimeint.c",
    "gio/xdgmime/xdgmimemagic.c",
    "gio/xdgmime/xdgmimeparent.c",
    "glib/glib-unix.c",
    "glib/gspawn.c",
    "glib/giounix.c",
    "glib/gthread-posix.c",
}

WIN_SRCS = {
    "gio/gcontenttype-win32.c",
    "gio/gregistrysettingsbackend.c",
    "gio/gmemorymonitorwin32.c",
    "gio/gunixcredentialsmessage.c",
    "gio/gunixconnection.c",
    "gio/gunixsocketaddress.c",
    "gio/gwin32appinfo.c",
    "gio/gwin32file-sync-stream.c",
    "gio/gwin32inputstream.c",
    "gio/gwin32mount.c",
    "gio/gwin32networkmonitor.c",
    "gio/gwin32notificationbackend.c",
    "gio/gwin32outputstream.c",
    "gio/gwin32packageparser.c",
    "gio/gwin32registrykey.c",
    "gio/gwin32sid.c",
    "gio/gwin32volumemonitor.c",
    "gio/win32/gwin32filemonitor.c",
    "gio/win32/gwin32fsmonitorutils.c",
    "gio/win32/gwinhttpfile.c",
    "gio/win32/gwinhttpfileinputstream.c",
    "gio/win32/gwinhttpfileoutputstream.c",
    "gio/win32/gwinhttpvfs.c",
    "glib/dirent/wdirent.c",
    "glib/giowin32.c",
    "glib/gspawn-win32.c",
    "glib/gthread-win32.c",
    "glib/gwin32.c",
}

WIN_HEADERS = {
    "gio/giowin32-private.c",  # this looks like a .c file, but it's actually include-only!
    "gio/giowin32-priv.h",
    "gio/giowin32-afunix.h",
    "gio/gregistrysettingsbackend.h",
    "gio/gwin32api-application-activation-manager.h",
    "gio/gwin32api-iterator.h",
    "gio/gwin32api-misc.h",
    "gio/gwin32api-package.h",
    "gio/gwin32api-storage.h",
    "gio/gwin32appinfo.h",
    "gio/gwin32file-sync-stream.h",
    "gio/gwin32inputstream.h",
    "gio/gwin32mount.h",
    "gio/gwin32networking.h",
    "gio/gwin32networkmonitor.h",
    "gio/gwin32outputstream.h",
    "gio/gwin32packageparser.h",
    "gio/gwin32registrykey.h",
    "gio/gwin32sid.h",
    "gio/gwin32volumemonitor.h",
    "gio/win32/gwin32filemonitor.h",
    "gio/win32/gwin32fsmonitorutils.h",
    "gio/win32/gwinhttpfile.h",
    "gio/win32/gwinhttpfileinputstream.h",
    "gio/win32/gwinhttpfileoutputstream.h",
    "gio/win32/gwinhttpvfs.h",
    "glib/dirent/dirent.c",
    "glib/dirent/dirent.h",
    "glib/gstdio-private.c",
    "glib/gwin32-private.c",
    "glib/gwin32.h",
    "glib/win_iconv.c",
    "gmodule/gmodule-win32.c",
}


def glib_post_build(self):
    os.rename(self.dstdir + "/config.h", self.dstdir + "/config-linux-x86_64.h")
    os.rename(
        self.dstdir + "/glib/glibconfig.h",
        self.dstdir + "/glib/glibconfig-linux-x86_64.h",
    )


def glib_post_install(self):
    # Make config.h inclusions Arcadia-root relative
    fileutil.re_sub_dir(self.dstdir, r'[<"]config\.h[>"]', "<contrib/restricted/glib/config.h>")

    # Generate include library with containing inclinks to glib interface headers
    self.yamakes["include"] = Library(
        SUBSCRIBER=self.owners,
        LICENSE=[self.license],
        NO_UTIL=True,
        NO_RUNTIME=True,
        ADDINCL=[
            GLOBAL(f"{self.arcdir}/include"),
            GLOBAL(f"{self.arcdir}/include/gio"),
            GLOBAL(f"{self.arcdir}/include/glib"),
            GLOBAL(f"{self.arcdir}/include/gmodule"),
        ],
    )

    with self.yamakes["static"] as glib:
        glib.PEERDIR += {
            f"{self.arcdir}/include",
            "contrib/libs/pcre",
            "contrib/restricted/gettext-stub",
        }
        glib.ADDINCL = {include for include in glib.ADDINCL if "glib" not in include}
        glib.ADDINCL |= {
            "contrib/libs/pcre",
            GLOBAL("contrib/restricted/gettext-stub"),
        }
        glib.ADDINCL = sorted(glib.ADDINCL, key=str)

        # Get builtins linked statically
        glib.NO_RUNTIME = False
        glib.NO_UTIL = True

        glib.SRCS -= UNIX_SRCS
        glib.SRCS -= LINUX_SRCS
        glib.SRCS -= LINUX_ANDROID_SRCS
        glib.after(
            "SRCS",
            Switch(
                OS_LINUX=Linkable(
                    SRCS=UNIX_SRCS | LINUX_SRCS | LINUX_ANDROID_SRCS,
                    EXTRALIBS=["resolv"],
                ),
                OS_ANDROID=Linkable(
                    SRCS=UNIX_SRCS | LINUX_ANDROID_SRCS,
                ),
                OS_WINDOWS=Linkable(
                    SRCS=WIN_SRCS,
                ),
                OS_DARWIN=Linkable(
                    SRCS=UNIX_SRCS | DARWIN_SRCS,
                    EXTRALIBS=["resolv"],
                    CFLAGS=["-DBIND_8_COMPAT"],
                    LDFLAGS=[
                        "-framework",
                        "Foundation",
                        "-framework",
                        "AppKit",
                        "-framework",
                        "Carbon",
                    ],
                ),
            ),
        )
        glib.PEERDIR.add("library/cpp/sanitizer/include")

    os.makedirs(f"{self.dstdir}/dynamic")
    with open(f"{self.dstdir}/dynamic/glib.exports", "w") as f:
        f.write("C g_*\n")
        f.write("C glib_*\n")

    self.make_dll_dispatcher(
        switch_flag="USE_DYNAMIC_GLIB",
        exports_script="glib.exports",
        dynamic_library_name="glib",
    )

    with self.yamakes["."] as glib_dispatcher:
        glib_dispatcher.RECURSE.remove("dynamic")
        glib_dispatcher.after("RECURSE", Switch({"NOT OS_WINDOWS": Recursable(RECURSE={"dynamic"})}))

    with self.yamakes["dynamic"] as glib:
        glib.LICENSE_RESTRICTION_EXCEPTIONS |= {
            "contrib/libs/libiconv/static",
            f"{self.arcdir}/include",
        }


glib = MesonNixProject(
    nixattr="glib",
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/glib",
    ignore_commands=["python3", "gengiotypefuncs.py"],
    copy_sources=(
        # fmt: off
        DARWIN_SRCS |
        DARWIN_HEADERS |
        DARWIN_SRCS |
        WIN_SRCS |
        WIN_HEADERS
        # fmt: on
    ),
    copy_sources_except=["build/*", "build/**/*", "fuzzing/*"],
    # FIXME:
    # Switch to normal platform dispatchers
    # uncomment this for new mutually exclusive platform_dispatchers
    # platform_dispatchers=['config.h', 'glib/glibconfig.h'],
    keep_paths=[
        "config.h",
        "config-*.h",
        "glib/glibconfig.h",
        "glib/glibconfig-*.h",
        "gmodule/gmoduleconf.h",
        "gmodule/gmoduleconf-*.h",
    ],
    disable_includes=[
        "afunix.h",
        "c-ctype.h",
        "gio_probes.h",
        "glib_probes.h",
        "gnulib/printf.h",
        "relocatable.h",
        "gobject_probes.h",
        "libmount.h",
        # TODO (thegeorg@): find a configure flag to use external pcre
        "pcre/pcre.h",
        "sys/cygwin.h",
        "sysprof-capture.h",
        "wprintf-parse.h",
        "vasnwprintf.h",
        # if (G_MODULE_IMPL == G_MODULE_IMPL_AR)
        "gmodule-ar.c",
        # various includes
        "os2.h",
    ],
    inclink={
        "include/glib/glibconfig.h": "glib/glibconfig.h",
        "include/glib": ["glib/*.h"],
        "include/glib/deprecated": ["glib/deprecated/*.h"],
        "include/gio": ["gio/*.h"],
        "include/gio/gvdb": ["gio/gvdb/*.h"],
        "include/gmodule": ["gmodule/*.h"],
        "include/gobject": ["gobject/*.h"],
    },
    post_build=glib_post_build,
    post_install=glib_post_install,
    install_targets={
        "charset",
        "gio-2.0",
        "glib-2.0",
        "gmodule-2.0",
        "gobject-2.0",
        "inotify",
        "xdgmime",
    },
    put={
        "glib-2.0": "static",
    },
    put_with={
        "glib-2.0": [
            "charset",
            "gio-2.0",
            "gmodule-2.0",
            "gobject-2.0",
            "inotify",
            "xdgmime",
        ],
    },
)
