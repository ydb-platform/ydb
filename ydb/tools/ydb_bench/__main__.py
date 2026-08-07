import sys

from library.python import resource
from library.python import svn_version

from ydb.tools.ydb_bench.lib.cli import main


def load_resource(name):
    data = resource.find(name)
    if data is None:
        raise RuntimeError("bundled resource {!r} was not found".format(name))
    return data


def tool_revision():
    return {
        "build_type": resource.find("ydb_bench/build_type").decode("ascii").lower(),
        "commit_id": svn_version.commit_id(),
        "hash": svn_version.hash(),
        "vcs": svn_version.vcs(),
    }


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:], resource_loader=load_resource, tool_revision=tool_revision()))
