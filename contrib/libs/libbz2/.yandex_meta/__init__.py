from devtools.yamaker.modules import Switch
from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    self.yamakes["."].after(
        "NO_SANITIZE",
        Switch(
            {
                "SANITIZER_TYPE == undefined": "NO_SANITIZE()",
            }
        ),
    )


bzip2 = GNUMakeNixProject(
    arcdir="contrib/libs/libbz2",
    nixattr="bzip2",
    install_targets=["bz2"],
    post_install=post_install,
)
