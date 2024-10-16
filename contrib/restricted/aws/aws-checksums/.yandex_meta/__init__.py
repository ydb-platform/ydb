from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    # Otherwise debug build fails with gcc, as expected by the authors.
    self.yamakes["."].after("CFLAGS", Switch({'BUILD_TYPE == "DEBUG"': Linkable(CFLAGS=["-DDEBUG_BUILD"])}))


aws_checksums = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-checksums",
    nixattr="aws-checksums",
    owners=["g:cpp-contrib"],
    flags=["-DBUILD_JNI_BINDINGS=OFF"],
    disable_includes=["aws/checksums/crc_jni.h"],
    post_install=post_install,
)
