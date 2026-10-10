"""Remove SDK source dependencies from rendered documentation before publishing."""

import shutil
import sys
from pathlib import Path

if __package__:
    from .cli import STAGING_ROOT
else:
    from cli import STAGING_ROOT


def clean(output):
    output = Path(output).resolve()
    docs = Path(__file__).resolve().parents[2]
    if output == docs:
        raise ValueError("cleanup requires a build output directory, not documentation input")
    staging = output / STAGING_ROOT
    if staging.exists():
        if staging.is_symlink() or not staging.resolve().is_relative_to(output):
            raise ValueError("SDK output staging escapes the build directory")
        shutil.rmtree(staging)


if __name__ == "__main__":
    for directory in sys.argv[1:]:
        clean(directory)
