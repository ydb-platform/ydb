from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_intrusive = NixSourceProject(
    nixattr="boost_intrusive",
    arcdir=boost.make_arcdir("intrusive"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "BOOST_INTRUSIVE_INVARIANT_ASSERT_INCLUDE",
        "BOOST_INTRUSIVE_SAFE_HOOK_DEFAULT_ASSERT_INCLUDE",
        "BOOST_INTRUSIVE_SAFE_HOOK_DESTRUCTOR_ASSERT_INCLUDE",
    ],
    post_install=post_install,
)
