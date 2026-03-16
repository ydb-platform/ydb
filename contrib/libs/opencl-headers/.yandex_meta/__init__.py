from devtools.yamaker.modules import GLOBAL, Library
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = self.module(
        Library,
        ADDINCL=[GLOBAL(self.arcdir)],
    )


opencl_headers = NixSourceProject(
    arcdir="contrib/libs/opencl-headers",
    nixattr="opencl-headers",
    copy_sources=["CL/"],
    post_install=post_install,
)
