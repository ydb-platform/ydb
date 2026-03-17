# coding: utf-8

from __future__ import print_function, absolute_import, division, unicode_literals

"""
create a temporary directory that can be used in a with statement (auto removed)

  with TempDir() a td:
     file_name = td.directory / 'myfile.txt'
     ......

or that keeps a number of old temporary directories per user

  self._tempdir = TempDir(prefix=utilname, keep=3)


"""


import os
import tempfile
from ruamel.std.pathlib import Path


class TempDir(object):
    """self removing (unless keep=True) temporary directory"""

    def __init__(self, prefix=None, keep=False, basedir=None):
        self._keep = keep
        kw = dict(dir=basedir)
        if prefix is not None:
            try:
                self._keep = int(self._keep)
            except TypeError:
                pass
            else:
                fmtstr = prefix + '-{}'
                if basedir is None:
                    # tmpdir = Path(tempfile.mkdtemp(**kw))   # set once?
                    # tmpdir.rmdir()
                    tmpdir = Path(tempfile.gettempdir())
                else:
                    tmpdir = Path(basedir)
                tmpdir = tmpdir / '{}-of-{}'.format(prefix, os.environ.get('USER', 'nobody'))
                # print(tmpdir)
                existing_dirs = {}
                for x in tmpdir.glob(fmtstr.format('*')):
                    v = int(str(x).rsplit('-', 1)[1])
                    existing_dirs[v] = x
                keys = sorted(existing_dirs.keys())
                for key in keys:
                    if len(existing_dirs) < self._keep:  # smaller as we are going to make one
                        break
                    existing_dirs.pop(key).rmtree()
                try:
                    next_key = keys[-1] + 1
                except IndexError:
                    next_key = 1
                # print('new', next_key)
                self._tmpdir = tmpdir / fmtstr.format(next_key)
                self._tmpdir.mkdir(parents=next_key == 1)
                return

        # mkdtemp creates with permissions rwx------
        self._prefix = prefix
        if self._prefix is not None:
            kw['prefix'] = prefix
        # mkdtemp doesn't do the right thing if None is passed in
        # as it has prefix=template in definition
        self._tmpdir = tempfile.mkdtemp(**kw)

    def remove(self):
        self._tmpdir.rmtree()

    def chdir(self):
        os.chdir(self._tmpdir)

    def tempfilename(self, extension=''):
        fd, name = tempfile.mkstemp(suffix=extension, dir=self._tmpdir)
        os.close(fd)
        return name

    @property
    def directory(self):
        return self._tmpdir

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._keep is False:
            self.remove()


if __name__ == '__main__':
    with TempDir(prefix='txt', keep=3) as tempdir:
        (tempdir.directory / 'hallo.txt').write_text('hallo')
