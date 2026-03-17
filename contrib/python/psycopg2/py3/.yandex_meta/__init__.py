from devtools.yamaker.modules import Linkable, py_srcs, Switch
from devtools.yamaker.project import NixProject
from devtools.yamaker import python


def post_install(self):
    dist_files = python.extract_dist_info(self)

    # Pythonize.
    with self.yamakes["."] as psycopg:
        psycopg.to_py_library(
            PY_SRCS=py_srcs(
                self.dstdir,
                rel=f"{self.dstdir}/lib",
                ns="psycopg2",
            ),
            SRCDIR=[f"{self.arcdir}/lib"],
            RESOURCE_FILES=python.prepare_resource_files(self, *dist_files),
        )
        # fmt: off

        # -DPSYCOPG_VERSION contains spaces which break things apart.
        # Leave only numeric part of the version.
        psycopg.CFLAGS = [
            flag
            for flag in psycopg.CFLAGS
            if not flag.startswith("-DPSYCOPG_VERSION")
        ]

        # psycopg adds -I to libpq/server which are not present in Arcadia
        psycopg.ADDINCL = [
            addincl
            for addincl in psycopg.ADDINCL
            if "server" not in addincl
        ]

        # fmt: on
        psycopg.CFLAGS.append(f"-DPSYCOPG_VERSION={self.version}")

        psycopg.after(
            "CFLAGS",
            Switch(
                {
                    # To avoid symbol character conflict when using libpq on windows
                    "OS_WINDOWS": Linkable(CFLAGS=["-Dgettimeofday=gettimeofday_pycopg2"])
                }
            ),
        )


python_psycopg2 = NixProject(
    owners=["g:python-contrib"],
    arcdir="contrib/python/psycopg2/py3",
    nixattr=python.make_nixattr("psycopg2"),
    copy_sources=["lib/**/*.py"],
    disable_includes=[
        "mxDateTime.h",
        "OS.h",
        "psycopg/adapter_mxdatetime.h",
        "psycopg/typecast_mxdatetime.c",
    ],
    build_install_subdir=python.BUILD_INSTALL_SUBDIR,
    post_install=post_install,
)
