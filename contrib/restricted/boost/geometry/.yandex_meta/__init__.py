from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(
        self,
        # variant2 is an optional dependency,
        # do not peerdir if from the module.
        #
        # See: https://github.com/boostorg/geometry/issues/1225
        optional_deps=["variant2"],
    )


boost_geometry = NixSourceProject(
    nixattr="boost_geometry",
    arcdir=boost.make_arcdir("geometry"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
