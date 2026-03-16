# coding: utf-8

from __future__ import print_function, absolute_import, division, unicode_literals

_package_data = dict(
    full_package_name='ruamel.std.pathlib',
    version_info=(0, 13, 0),
    __version__='0.13.0',
    version_timestamp='2024-03-03 07:56:23',
    author='Anthon van der Neut',
    author_email='a.van.der.neut@ruamel.eu',
    since=2013,
    description='improvements over the standard pathlib module and pathlib2 package',
    entry_points=None,
    extras_require={':python_version<="3.4"': ['pathlib2']},
    license='MIT License',
    # universal=True,
    python_requires='>=3',
    tox=dict(env='3', deps=['orjson', 'ujson']),
)
# pathlib2 for 3.4 -> expanduser added

version_info = _package_data['version_info']
__version__ = _package_data['__version__']

###########

import os  # NOQA
import sys  # NOQA
import time  # NOQA
import inspect  # NOQA

if sys.version_info < (3, 5):
    # 3.4 e.g has no expanduser()
    from pathlib2 import *  # NOQA

    FileNotFoundError = IOError
else:
    from pathlib import *  # NOQA


class DirStack(object):
    def __init__(self):
        self._dir_stack = []

    def pushd(self, d):
        self._dir_stack.insert(0, os.getcwd())
        os.chdir(str(d))

    def popd(self):
        os.chdir(self._dir_stack.pop())


# global stack, always there
_dir_stack = DirStack()


def pushd(d):
    _dir_stack.pushd(d)


def popd():
    _dir_stack.popd()


# pathlib 0.8 was not compatible with pathlib in python 3.4,
# but 0.97 is
# should check here but there is no version number in pathlib ...
# switched to pathlib2 which aims at compatibility
#
#     @property
#     def root_stem(self):
#         """The final path component, minus suffices."""
#         return self.name.split('.', 1)[0]


# just for some sanity
Path.remove = Path.unlink


def _rmtree(self):
    import shutil

    assert self.is_dir()
    shutil.rmtree(str(self))


Path.rmtree = _rmtree


def _chdir(self):
    assert self.is_dir()
    os.chdir(str(self))


Path.chdir = _chdir


def _hash(self, typ=None, size=-1):
    """hash of the contents, typ can be any of the hashlib.new() acceptable strings
    if size is provided and positive only read that amount of bytes from the start of the file
    """
    import hashlib

    if typ is None:
        typ = 'sha224'
    with self.open(mode='rb') as f:
        data = f.read(size)
    try:
        # calling hashlib.sha224() is faster, but does it take data argument?
        h = hashlib.new(typ, data=data)
    except TypeError:
        # 2.7 didn't have data argument to new
        h = hashlib.new(typ)
        h.update(data)
    return h


Path.hash = _hash


def _copy(self, target):
    import shutil

    assert self.is_file()
    shutil.copy(str(self), str(target))


Path.copy = _copy


# this is the times parameter of os.utime, taking int/float seconds (and not nanoseconds)
def _utime(self, atime, mtime):
    os.utime(str(self), (atime, mtime), follow_symlinks=False)


Path.utime = _utime


def _stabilise(self, duration=5, recheck=0.2, msg=None):
    """wait if modified in the last duration seconds"""
    while (self.stat().st_mtime + duration) > time.time():
        if msg:
            print(msg)
            msg = None
        time.sleep(recheck)


Path.stabilise = _stabilise
Path.stabilize = _stabilise

# #
# # - If the check level is set at 1, any calls that are issued with strings that
# #     could be changed are logged (to the console), those are the positions where
# #     arguments should still be converted from string to Path.
# # - If check level is set at 2, all call to pl methods are logged, those
# #     are the areas where the calls can be replaced by methods on the Path instance,
# #     check level 3 is the same as level 1, but any remaining string isntances throw
# #         an error.


class PathLibConversionHelper(object):
    """
    if you are changing to use the standard pathlib library, it is cumbersome
    to change everything at once, and also to change all the arguments to calls to
    os.path.join, os.rename, os.path.dirname to be made encapsulated in str()

    by making an instance of PathLibConversionHelper named pl you can change
    os.path.join() to pl.path.join(), etc., and then start passing in Path instances
    instead of strings.

    if the check level is set at 1 any calls that are issued with strings that
    could be changed are logged (to the console), those are the positions where
    arguments should still be converted from string to Path
    if check level is set at 2, all calls to pl methods are logged, those
    are the areas where the calls can be replaced by methods on the Path instance,
    check level 3 is the same as level 1, but any remaining string instances throw
    an error.

    maybe look at https://github.com/mikeorr/Unipath#comparision-with-osospathshutil-and-pathpy
    and incorporate
    """

    def __init__(self, check=0):
        class Container(object):
            pass

        self._check = check
        self._usage = {}
        path = self.path = Container()
        for m in dir(self):
            if not m.startswith('_path_'):
                continue
            attr = m.split('_', 2)[2]
            setattr(path, attr, getattr(self, m))

    def add_usage(self, vars, comment):
        if isinstance(vars, (list, tuple)):
            vt = tuple([isinstance(x, Path) for x in vars])
        else:
            vt = tuple([isinstance(vars, Path)])
        if self._check < 1:
            return
        caller = inspect.stack()[2]
        t = caller[1], caller[2], comment, vt
        count = self._usage.setdefault(t, [0,])
        count[0] += 1
        if self._check > 1 and count[0] == 1:
            print('{2} [{0}:{1} / Path {3}]'.format(*t))

    def dump(self, stream=None, show_all=False):
        """dump unique invocations of methods

        unless show_all is True, invocations that uniquely use Path arguments
        are not shown
        """
        if stream is None:
            stream = sys.stdout
        for t in self._usage:
            if show_all or not all(t[3]):
                print(
                    '{t[2]} [{t[0]}:{t[1]} / {count} / Path {t[3]}]'.format(
                        count=self._usage[t][0], t=t
                    )
                )

    def rename(self, old_name, new_name):
        """os.rename replacement that can handle Path argument"""
        self.add_usage([old_name, new_name], 'update .rename')
        if isinstance(old_name, Path):
            old_name.rename(new_name)  # rename works with strings and Path
        else:
            os.rename(old_name, str(new_name))

    # os.rename
    # os.getcwd
    # os.path.expanduser

    def walk(self, top, *args, **kw):
        for r, d, f in os.walk(str(top), *args, **kw):
            yield Path(r), d, f

    def _path_join(self, base, *args):
        self.add_usage([base] + list(args), 'update .path.join to use "/"')
        # os.path.join
        if isinstance(base, Path):
            return base.joinpath(*args)
        else:
            return os.path.join(base, *args)

    def _path_exists(self, path):
        self.add_usage(path, 'update .exists to use Path.exists()')
        if isinstance(path, Path):
            return path.exists()
        else:
            return os.path.exists(path)

    def _path_dirname(self, file_name):
        self.add_usage(file_name, 'update .path.dirname to use Path.parent')
        if isinstance(file_name, Path):
            return file_name.parent
        else:
            return os.path.dirname(file_name)

    def _path_getmtime(self, file_name):
        self.add_usage(file_name, 'update .path.getmtime to use Path.parent')
        if isinstance(file_name, Path):
            return file_name.stat().st_mtime
        else:
            return os.path.getmtime(file_name)

    def _path_expanduser(self, path):
        self.add_usage(path, 'update .path.expanduser to use Path.expanduser')
        if isinstance(path, Path):
            return path.expanduser()
        else:
            return os.path.expanduser(path)

    def _path_basename(self, path):
        self.add_usage(path, 'update .path.basename to use Path.basename')
        if isinstance(path, Path):
            return path.name
        else:
            return os.path.basename(path)

    def _path_isdir(self, path):
        self.add_usage(path, 'update .path.isdir to use Path.isdir')
        if isinstance(path, Path):
            return path.is_dir()
        else:
            return os.path.isdir(path)

    def _path_splitext(self, path):
        self.add_usage(
            path, 'update .path.splitext to use Path.parent / Path.stem + Path.suffix'
        )
        if isinstance(path, Path):
            return path.parent / path.stem, path.suffix
        else:
            return os.path.splitext(path)

    def chdir(self, path):
        """replaces: os.chdir()"""
        self.add_usage(path, 'update .chdir to use Path.chdir() or os.chdir(str(Path))')
        if isinstance(path, Path):
            return path.chdir()
        else:
            return os.chdir(path)

    # os.mkdir

    def rmdir(self, path):
        """replaces: os.rmdir()"""
        self.add_usage(path, 'update .rmdir to use Path.rmdir() or os.rmdir(str(Path))')
        if isinstance(path, Path):
            return path.rmdir()
        else:
            return os.rmdir(path)

    def makedirs(self, path, *args, **kw):
        """replaces: os.makedirs()"""
        self.add_usage(path, 'update .makedirs to use Path.mkdir()')
        os.makedirs(str(path), *args, **kw)

    def remove(self, path):
        """replaces: os.remove()"""
        self.add_usage(path, 'update .chdir to use Path.chdir() or os.chdir(str(Path))')
        if isinstance(path, Path):
            return path.remove()
        else:
            return os.remove(path)

    def listdir(self, path='.'):
        """replaces: os.listdir()"""
        return os.listdir(str(path))

    def glob(self, pattern):
        """replaces: glob.glob()"""
        import glob

        self.add_usage(path, 'update .glob to use Path.glob()')
        if isinstance(pattern, Path):
            return [Path(fn) for fn in glob.glob(str(pattern))]
        else:
            return glob.glob(pattern)

    # shutil.rmtree
    def rmtree(self, path):
        """replaces: shutil.rmtree()"""
        import shutil

        self.add_usage(path, 'update .rmtree to use Path.rmtree() or shutil.rmtree(str(Path))')
        if isinstance(path, Path):
            return path.rmtree()
        else:
            return shutil.rmtree(path)

    # shutil.copy
    def copy(self, src, dst):
        """replaces: shutil.copy()"""
        import shutil

        self.add_usage(
            src, 'update .copy to use Path.copy() or shutil.copy(str(src), str(dst))'
        )
        if isinstance(src, Path):
            return src.copy(dst)
        else:
            return shutil.copy(str(str), str(dst))

    # built-in open
    def open(self, path, mode='r', buffering=1):
        """replaces: built-in open()"""
        self.add_usage(path, 'update .open to use Path.open()')
        if isinstance(path, Path):
            return path.open(mode, buffering)
        else:
            return open(path, mode, buffering)

    def mkstemp(self, suffix="", prefix=None, dir=None, text=False):
        """replaces: tempfile.mkstemp()"""
        import tempfile

        if prefix is None:
            prefix = tempfile.template
        self.add_usage(dir, 'update .mkstemp to use Path.mkstemp()')
        if isinstance(dir, Path):
            dir = str(dir)
        return tempfile.mkstemp(suffix, prefix, dir, text)
