from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.after(
            "CFLAGS",
            Switch(
                OS_WINDOWS=Linkable(
                    CFLAGS=["-DAWS_AUTH_EXPORTS"],
                ),
            ),
        )


aws_c_auth = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-auth",
    nixattr="aws-c-auth",
    post_install=post_install,
)
