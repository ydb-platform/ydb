from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.after(
            "CFLAGS",
            Switch(
                OS_WINDOWS=Linkable(
                    CFLAGS=["-DAWS_CRT_CPP_EXPORTS"],
                ),
            ),
        )


aws_crt_cpp = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-crt-cpp",
    nixattr="aws-crt-cpp",
    post_install=post_install,
)
