import os.path

from devtools.yamaker import boost, fileutil, pathutil
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(
        self,
        CFLAGS=[GLOBAL("-DBOOST_COROUTINES_NO_DEPRECATION_WARNING"), "-DBOOST_COROUTINES_SOURCE"],
        SRCS=fileutil.files(
            self.dstdir, rel=True, test=lambda p: pathutil.is_source(p) and "windows" not in p and "posix" not in p
        ),
    )
    with self.yamakes["."] as coroutine:
        # uses fcontext directly (also this lib is deprecated)
        boost_context_arcdir = boost.make_arcdir("context")
        coroutine.PEERDIR.remove(boost_context_arcdir)
        coroutine.PEERDIR.add(f"{boost_context_arcdir}/fcontext_impl")

        coroutine.after("CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_COROUTINES_DYN_LINK")])}))
        coroutine.after(
            "CFLAGS",
            Switch(
                {
                    "OS_WINDOWS": Linkable(
                        SRCS=fileutil.files(
                            os.path.join(self.dstdir, "src", "windows"), rel=self.dstdir, test=pathutil.is_source
                        ),
                    ),
                    "default": Linkable(
                        SRCS=fileutil.files(
                            os.path.join(self.dstdir, "src", "posix"), rel=self.dstdir, test=pathutil.is_source
                        ),
                    ),
                }
            ),
        )


boost_coroutine = NixSourceProject(
    nixattr="boost_coroutine",
    arcdir=boost.make_arcdir("coroutine"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src/",
    ],
    post_install=post_install,
)
