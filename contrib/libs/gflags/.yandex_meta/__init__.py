from devtools.yamaker.modules import Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    m = self.yamakes["."]
    m.after("LDFLAGS", Switch(OS_WINDOWS="LDFLAGS(shlwapi.lib)"))


gflags = CMakeNinjaNixProject(
    owners=["g:contrib"],
    arcdir="contrib/libs/gflags",
    nixattr="gflags",
    flags=["-DGFLAGS_ATTRIBUTE_UNUSED="],
    build_targets=["gflags_shared"],
    copy_sources=["src/windows_port.h"],
    platform_dispatchers=["include/gflags/config.h", "include/gflags/defines.h"],
    post_install=post_install,
)
