from devtools.yamaker.modules import Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(Library)


miniselect = NixSourceProject(
    nixattr="pocketfft",
    arcdir="contrib/libs/pocketfft",
    owners=["g:cpp-contrib"],
    copy_sources=[
        "pocketfft_hdronly.h",
    ],
    post_install=post_install,
)
