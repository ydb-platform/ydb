import shutil

from devtools.yamaker.modules import py_srcs
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    shutil.copy(f"{self.srcdir}/python/cn_tn.py", self.dstdir)
    self.yamakes["."] = self.module(
        module="PY3_LIBRARY",
        PY_SRCS=py_srcs(self.dstdir, rel=self.dstdir),
        NO_LINT=True,
    )


chinese_text_normalization = NixSourceProject(
    arcdir="contrib/python/chinese-text-normalization",
    nixattr="chinese-text-normalization",
    owners=["g:python-contrib"],
    post_install=post_install,
)
