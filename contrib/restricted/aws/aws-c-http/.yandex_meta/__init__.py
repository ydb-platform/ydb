from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    m = self.yamakes["."]

    m.after(
        "CFLAGS",
        Switch(
            CLANG_CL=Linkable(
                CFLAGS=["-DAWS_HTTP_EXPORTS", "-std=c99"],
            ),
            OS_WINDOWS=Linkable(
                CFLAGS=["-DAWS_HTTP_EXPORTS"],
            ),
        ),
    )


aws_c_cal = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-http",
    nixattr="aws-c-http",
    post_install=post_install,
)
