# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.

from fnmatch import fnmatch
import os
from pathlib import Path
import re
import subprocess

import pytest

import cartopy


# Add shebang possibility or C comment starter to the LICENSE_RE_PATTERN
SHEBANG_PATTERN = r'((\#\!.*|\/\*)\n)?'


LICENSE_TEMPLATE = """
# Copyright Crown and Cartopy Contributors
#
# This file is part of Cartopy and is released under the BSD 3-clause license.
# See LICENSE in the root of the repository for full licensing details.
""".strip()
LICENSE_RE_PATTERN = re.escape(LICENSE_TEMPLATE)
LICENSE_RE = re.compile(SHEBANG_PATTERN + LICENSE_RE_PATTERN, re.MULTILINE)


# Guess cartopy repo directory of cartopy - realpath is used to mitigate
# against Python finding the cartopy package via a symlink.
CARTOPY_DIR = Path(cartopy.__file__).parent.resolve()
REPO_DIR = Path(os.getenv('CARTOPY_GIT_DIR',
                          CARTOPY_DIR.parent.parent))


class TestLicenseHeaders:
    @staticmethod
    def list_tracked_files():
        """
        Return a list of all the files under git.

        .. note::

            This function raises a ValueError if the repo root does
            not have a ".git" folder. If git is not installed on the system,
            or cannot be found by subprocess, an IOError may also be raised.

        """
        # Check the ".git" folder exists at the repo dir.
        if not (REPO_DIR / '.git').is_dir():
            raise ValueError(f'{REPO_DIR} is not a git repository.')

        output = subprocess.check_output(['git', 'ls-tree', '-z', '-r',
                                          '--name-only', 'HEAD'],
                                         cwd=REPO_DIR)
        output = output.rstrip(b'\0').split(b'\0')
        res = [fname.decode() for fname in output]

        return res

    def test_license_headers(self):
        exclude_patterns = ('build/*',
                            'dist/*',
                            'docs/build/*',
                            'docs/source/gallery/*',
                            'examples/*',
                            'lib/cartopy/_version.py',
                            )

        try:
            tracked_files = self.list_tracked_files()
        except ValueError as e:
            # Caught the case where this is not a git repo.
            return pytest.skip('cartopy installation did not look like a git '
                               f'repo: {e}')

        failed = []
        for fname in sorted(tracked_files):
            full_fname = REPO_DIR / fname
            ext = full_fname.suffix
            if ext in ('.py', '.pyx', '.c', '.cpp', '.h') and \
                    full_fname.is_file() and \
                    not any(fnmatch(fname, pat) for pat in exclude_patterns):

                if full_fname.stat().st_size == 0:
                    # Allow completely empty files (e.g. ``__init__.py``)
                    continue

                content = full_fname.read_text(encoding="utf-8")

                if not bool(LICENSE_RE.match(content)):
                    failed.append(full_fname)

        assert failed == [], 'There were license header failures.'
