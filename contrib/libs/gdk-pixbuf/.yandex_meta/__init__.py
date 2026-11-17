from devtools.yamaker.modules import GLOBAL
from devtools.yamaker.project import MesonNixProject


def post_install(self):
    with self.yamakes["."] as m:
        # Replace glib include-only peerdir with full glib peerdir
        m.PEERDIR.discard("contrib/restricted/glib/include")
        m.PEERDIR.add("contrib/restricted/glib")
        # Replace libpng include-only peerdir with full libpng peerdir
        m.PEERDIR.discard("contrib/libs/libpng/include")
        m.PEERDIR.add("contrib/libs/libpng")
        # Add static compilation flag globally
        m.CFLAGS.append(GLOBAL("-DGDK_PIXBUF_STATIC_COMPILATION"))
        # io-png.c includes png.h which lives in contrib/libs/libpng/include
        m.ADDINCL.add("contrib/libs/libpng/include")
        # Remove leaked build-time paths from CFLAGS:
        # - /tmp/... temporary build directories
        # - GDK_PIXBUF_PREFIX points to a nix sandbox path (e.g. /var/empty/...)
        #   and is not needed for static builds
        # - GDK_PIXBUF_LIBDIR and GDK_PIXBUF_LOCALEDIR contain nix store paths
        #   (e.g. /nix/store/.../lib); replace LIBDIR and LOCALEDIR with a safe fallback value
        m.CFLAGS = [
            f
            for f in m.CFLAGS
            if "/tmp/" not in str(f)
            and "GDK_PIXBUF_PREFIX" not in str(f)
            and "GDK_PIXBUF_LIBDIR" not in str(f)
            and "GDK_PIXBUF_LOCALEDIR" not in str(f)
        ]
        m.CFLAGS.append("-DGDK_PIXBUF_LIBDIR=\"/usr/lib\"")
        m.CFLAGS.append("-DGDK_PIXBUF_LOCALEDIR=\"/usr/local/lib\"")


gdk_pixbuf = MesonNixProject(
    arcdir="contrib/libs/gdk-pixbuf",
    nixattr="gdk-pixbuf",
    platform_dispatchers=[
        "config.h",
    ],
    install_targets=[
        "gdk_pixbuf-2.0",
    ],
    ignore_commands=["glib-genmarshal"],
    keep_paths=[
        "config-win.h",
    ],
    post_install=post_install,
)
