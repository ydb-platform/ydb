from devtools.yamaker import boost, fileutil, pathutil
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    # fmt: off
    posix_srcs = set(fileutil.files(
        f"{self.dstdir}/src/boost/locale/posix",
        rel=self.dstdir,
        test=pathutil.is_source,
    ))

    win_srcs = set(fileutil.files(
        f"{self.dstdir}/src/boost/locale/win32",
        rel=self.dstdir,
        test=pathutil.is_source,
    ))

    generic_srcs = set(fileutil.files(
        self.dstdir,
        rel=True,
        test=pathutil.is_source,
    )) - posix_srcs - win_srcs
    # fmt: on

    self.yamakes["."] = boost.make_library(
        self,
        CFLAGS=["-DBOOST_LOCALE_WITH_ICU"],
        SRCS=generic_srcs,
    )

    with self.yamakes["."] as locale:
        locale.ADDINCL.append(f"{self.arcdir}/src")
        locale.PEERDIR.add("contrib/libs/icu")
        locale.after("CFLAGS", Switch({"DYNAMIC_BOOST": Linkable(CFLAGS=[GLOBAL("-DBOOST_LOCALE_DYN_LINK")])}))
        locale.after(
            "CFLAGS",
            Switch(
                {
                    # not sure about that, but was used previously
                    "OS_ANDROID": Linkable(
                        CFLAGS=[
                            "-DBOOST_LOCALE_NO_POSIX_BACKEND",
                            "-DBOOST_LOCALE_NO_WINAPI_BACKEND",
                        ],
                    ),
                    "OS_WINDOWS": Linkable(
                        CFLAGS=["-DBOOST_LOCALE_NO_POSIX_BACKEND"],
                        SRCS=win_srcs,
                    ),
                    "default": Linkable(
                        CFLAGS=["-DBOOST_LOCALE_NO_WINAPI_BACKEND"],
                        SRCS=posix_srcs,
                    ),
                }
            ),
        )


boost_locale = NixSourceProject(
    nixattr="boost_locale",
    arcdir="contrib/restricted/boost/locale",
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
        "src/",
    ],
    post_install=post_install,
)
