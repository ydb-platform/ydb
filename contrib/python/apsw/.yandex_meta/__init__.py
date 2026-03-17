from devtools.yamaker.project import NixProject
from devtools.yamaker import python


def post_install(self):
    dist_files = python.extract_dist_info(self)

    with self.yamakes["."] as m:
        m.PY_REGISTER = ["apsw"]
        m.RESOURCE_FILES = python.prepare_resource_files(self, *dist_files)


apsw = NixProject(
    owners=["g:python-contrib"],
    nixattr=python.make_nixattr("apsw"),
    arcdir="contrib/python/apsw",
    disable_includes=[
        # ifdef PYPY_VERSION
        "pypycompat.c",
        "sqlite3config.h",
    ],
    post_install=post_install,
)
