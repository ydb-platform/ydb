
# ruamel.std.pathlib

[package ruamel.std.pathlib](https://bitbucket.org/ruamel/std.pathlib)
is a drop-in replacement to extend the Python standard
`pathlib` module.

You can just replace:

    from pathlib import PosixPath, Path

with:

    from ruamel.std.pathlib import PosixPath, Path

[![image](https://sourceforge.net/p/ruamel-std-pathlib/code/ci/default/tree/_doc/_static/pypi.svg?format=raw)](https://pypi.org/project/ruamel.std.pathlib)
[![image](https://sourceforge.net/p/ruamel-std-pathlib/code/ci/default/tree/_doc/_static/license.svg?format=raw)](https://opensource.org/licenses/MIT)
[![image](https://sourceforge.net/p/oitnb/code/ci/default/tree/_doc/_static/oitnb.svg?format=raw)](https://pypi.org/project/oitnb/)

Starting with 0.8.0 `ruamel.std.pathlib` no longer supports Python 2

## Extra Path functionality

- alias `remove` for `unlink` on Path
- add `copy()` and `rmtree()` from shutil to Path
- add `hash()` to path return the `hashlib.new()` value on the file
  content (defaults to \'sha224\', other algorithms can be given as
  parameter: `print(p.hash('md5').hexdigest())`)
- add `pushd()` and `popd()` to a (directory) Path (this keeps a stack
  of potentially more than one entry).
- add `rmtree()`, to call `shutil.rmtree()` on the Path
- add `chdir()` to a (directory) Path
- add `stabilise()` to a Path, to wait for it to no longer change,
  `duration` (default 5 seconds), `recheck` (default 0.2s) can be set,
  as well as message that is displayed once if not stable yet.

### blake3

when you have `blake3` installed (this is not a dependency for this package) and do
`import ruamel.std.pathlib.blake3`, you can also use that method on
`Path` instances.

### json

when you do `import ruamel.std.pathlib.json` this loads orjson/ujson/json (in that order)
to a Path so you can do
`data = path.json.load()` and `path.json.dump(data)`

### msgpack

when you have `ruamel.ext.msgpack` installed and do `import ruamel.std.pathlib.msgpack`
and you have a `Path` instance `path` you can do
`data = path.msgpack.load()` and `path.msgpack.dump(data)`

### tar

when you have `zstandard` installed and do `import ruamel.std.pathlib.tar`
and you have a `Path` instance `path` you can do
`with path.tar.open()` on a `.tar.zst` file with in memory decompression.


## Transition helper

If you are starting to use the standard pathlib library, it is
cumbersome to change everything at once, and also to change all the
arguments to calls to `os.path.join`, `os.rename`, `os.path.dirname` to
be made encapsulated in `str()`

By making an instance of `PathLibConversionHelper` named `pl` you can
change `os.path.join()` to `pl.path.join()`, etc., and then start
passing in Path instances instead of strings.

`PathLibConversionHelper` currently supports the following replacements for `os`,

:   `os.path`, `shutil` and built-in functions:

        .chdir()             replaces: os.chdir()
        .copy()              replaces: shutil.copy()
        .glob()              replaces: glob.glob()
        .listdir()           replaces: os.listdir()
        .makedirs()          replaces: os.makedirs()
        .mkstemp()           replaces: tempfile.mkstemp()
        .open()              replaces: built-in open()
        .path.basename()     replaces: os.path.basename()
        .path.dirname()      replaces: os.path.dirname()
        .path.exists()       replaces: os.path.exists()
        .path.expanduser()   replaces: os.path.expanduser()
        .path.getmtime()     replaces: os.path.getmtime()
        .path.isdir()        replaces: os.path.isdir()
        .path.join()         replaces: os.path.join()
        .path.splitext()     replaces: os.path.splitext()
        .remove()            replaces: os.remove()
        .rename()            replaces: os.rename()
        .rmdir()             replaces: os.rmdir()
        .rmtree()            replaces: shutil.rmtree()
        .walk()              replaces: os.walk()

You can provide a check level when you create the
`PathLibConversionHelper()` instance.

-   If check is non-zero, all calls are being logged and the invocations
    can be dumped e.g. at the end of the program with
    `pl.dump(stream, show_all=False)` This will include the number of
    invocations not using Path (and using Path uniquely as well if
    `show_all=True`)
-   If check is greater than 1, first usage is dumped immediately.

If you start with the following code:

    # coding: utf-8

    import os
    import glob
    import tempfile
    import shutil
    import random


    class TempDir(object):
        """self removing (unless keep=True) temporary directory"""
        def __init__(self, keep=False, basedir=None, prefix=None):
            self._keep = keep
            # mkdtemp creates with permissions rwx------
            kw = dict(dir=basedir)
            if prefix is not None:
                kw['prefix'] = prefix
            # mkdtemp doesn't do the right thing if None is passed in
            # as it has prefix=template in definition
            self._tmpdir = tempfile.mkdtemp(**kw)

        def remove(self):
            shutil.rmtree(self._tmpdir)

        def chdir(self):
            os.chdir(self._tmpdir)

        def tempfilename(self, extension=''):
            fd, name = tempfile.mkstemp(suffix=extension, dir=self._tmpdir)
            os.close(fd)
            return name

        def tempfilename2(self, extension=''):
            while True:
                name = os.path.join(
                    self._tmpdir,
                    '%08d' % random.randint(0, 100000) + extension
                )
                if not os.path.exists(name):
                    break
            return name

        @property
        def directory(self):
            return self._tmpdir

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if not self._keep:
                self.remove()


    def main():
        """contrived example using TempDir"""
        org_dir = os.getcwd()
        with TempDir() as td:
            for n in range(3):
                t1 = td.tempfilename(extension='.sample')
                with open(t1, 'w') as fp:
                    fp.write('content\n')
            t2 = td.tempfilename2(extension='.sample2')
            with open(t2, 'w') as fp:
                fp.write('content\n')
            os.chdir(td.directory)
            count = 0
            for file_name in glob.glob('*.samp*'):
                full_name = os.path.join(os.getcwd(), file_name)  # noqa
                # print(full_name)
                count += 1
            os.chdir('/tmp')  # not using Path
            os.chdir(org_dir)
        print('{} files found in temporary directory'.format(count))

    main()

you get:

    4 files found in temporary directory

When you start to change `TempDir()` to store the actual directory as a
Path, things start to break immediately:

    # coding: utf-8

    import os
    import glob
    import tempfile
    import shutil
    import random

    from ruamel.std.pathlib import Path                                   # added


    class TempDir(object):
        """self removing (unless keep=True) temporary directory"""
        def __init__(self, keep=False, basedir=None, prefix=None):
            self._keep = keep
            # mkdtemp creates with permissions rwx------
            kw = dict(dir=basedir)
            if prefix is not None:
                kw['prefix'] = prefix
            # mkdtemp doesn't do the right thing if None is passed in
            # as it has prefix=template in definition
            self._tmpdir = Path(tempfile.mkdtemp(**kw))                   # changed

        def remove(self):
            shutil.rmtree(self._tmpdir)

        def chdir(self):
            os.chdir(self._tmpdir)

        def tempfilename(self, extension=''):
            fd, name = tempfile.mkstemp(suffix=extension, dir=self._tmpdir)
            os.close(fd)
            return name

        def tempfilename2(self, extension=''):
            while True:
                name = os.path.join(
                    self._tmpdir,
                    '%08d' % random.randint(0, 100000) + extension
                )
                if not os.path.exists(name):
                    break
            return name

        @property
        def directory(self):
            return self._tmpdir

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if not self._keep:
                self.remove()


    def main():
        """contrived example using TempDir"""
        org_dir = os.getcwd()
        with TempDir() as td:
            for n in range(3):
                t1 = td.tempfilename(extension='.sample')
                with open(t1, 'w') as fp:
                    fp.write('content\n')
            t2 = td.tempfilename2(extension='.sample2')
            with open(t2, 'w') as fp:
                fp.write('content\n')
            os.chdir(td.directory)
            count = 0
            for file_name in glob.glob('*.samp*'):
                full_name = os.path.join(os.getcwd(), file_name)  # noqa
                # print(full_name)
                count += 1
            os.chdir('/tmp')  # not using Path
            os.chdir(org_dir)
        print('{} files found in temporary directory'.format(count))

    main()

With some errors:

    Traceback (most recent call last):
      File "_example/stage1.py", line 80, in <module>
        main()
      File "_example/stage1.py", line 77, in main
        os.chdir(org_dir)
      File "_example/stage1.py", line 56, in __exit__
        self.remove()
      File "_example/stage1.py", line 27, in remove
        shutil.rmtree(self._tmpdir)
      File "/opt/python/2.7.13/lib/python2.7/shutil.py", line 228, in rmtree
        if os.path.islink(path):
      File "/home/venv/dev/lib/python2.7/posixpath.py", line 135, in islink
        st = os.lstat(path)
    TypeError: coercing to Unicode: need string or buffer, PosixPath found

Instead of changing every usage in your program in one go, and hope it
will work again, you replace the routines from the standard module:

    # coding: utf-8

    import os
    import glob
    import tempfile
    import shutil                       # noqa
    import random

    from ruamel.std.pathlib import Path, PathLibConversionHelper            # changed
    pl = PathLibConversionHelper()                                          # added


    class TempDir(object):
        """self removing (unless keep=True) temporary directory"""
        def __init__(self, keep=False, basedir=None, prefix=None):
            self._keep = keep
            # mkdtemp creates with permissions rwx------
            kw = dict(dir=basedir)
            if prefix is not None:
                kw['prefix'] = prefix
            # mkdtemp doesn't do the right thing if None is passed in
            # as it has prefix=template in definition
            self._tmpdir = Path(tempfile.mkdtemp(**kw))

        def remove(self):
            pl.rmtree(self._tmpdir)

        def chdir(self):
            os.chdir(self._tmpdir)

        def tempfilename(self, extension=''):
            fd, name = pl.mkstemp(suffix=extension, dir=self._tmpdir)     # changed
            os.close(fd)
            return name

        def tempfilename2(self, extension=''):
            while True:
                name = pl.path.join(
                    self._tmpdir,
                    '%08d' % random.randint(0, 100000) + extension
                )
                if not pl.path.exists(name):                              # changed
                    break
            return name

        @property
        def directory(self):
            return self._tmpdir

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if not self._keep:
                self.remove()


    def main():
        """contrived example using TempDir"""
        org_dir = os.getcwd()
        with TempDir() as td:
            for n in range(3):
                t1 = td.tempfilename(extension='.sample')
                with open(t1, 'w') as fp:
                    fp.write('content\n')
            t2 = td.tempfilename2(extension='.sample2')
            with pl.open(t2, 'w') as fp:
                c = 'content\n'                                           # added
                if not isinstance(fp, file):                              # added
                    c = unicode(c)                                        # added
                fp.write(c)                                               # changed
            pl.chdir(td.directory)
            count = 0
            for file_name in glob.glob('*.samp*'):
                full_name = pl.path.join(os.getcwd(), file_name)  # noqa  # changed
                # print(full_name)
                count += 1
            pl.chdir('/tmp')  # not using Path
            pl.chdir(org_dir)                                             # changed
        print('{} files found in temporary directory'.format(count))

    main()

giving (again):

    4 files found in temporary directory

Change back just the creation of `self._tempdir` to the original:

    self._tmpdir = tempfile.mkdtemp(**kw)

and the output stays:

    4 files found in temporary directory

If you now change the creation of `pl` to:

    pl = PathLibConversionHelper(check=2)

you get as output:

    update .mkstemp to use Path.mkstemp() [_example/stage3.py:34 / Path (True,)]
    update .path.join to use "/" [_example/stage3.py:42 / Path (True, False)]
    update .exists to use Path.exists() [_example/stage3.py:44 / Path (True,)]
    update .open to use Path.open() [_example/stage3.py:69 / Path (True,)]
    update .chdir to use Path.chdir() or os.chdir(str(Path)) [_example/stage3.py:74 / Path (True,)]
    update .path.join to use "/" [_example/stage3.py:77 / Path (False, False)]
    update .chdir to use Path.chdir() or os.chdir(str(Path)) [_example/stage3.py:80 / Path (False,)]
    update .chdir to use Path.chdir() or os.chdir(str(Path)) [_example/stage3.py:81 / Path (False,)]
    update .rmtree to use Path.rmtree() or shutil.rmtree(str(Path)) [_example/stage3.py:28 / Path (True,)]
    4 files found in temporary directory

If you use `check=1` and at the end `pl.dump()`, you get:

    4 files found in temporary directory
    update .path.join to use "/" [_example/stage4.py:42 / 1 / Path (True, False)]
    update .chdir to use Path.chdir() or os.chdir(str(Path)) [_example/stage4.py:81 / 1 / Path (False,)]
    update .path.join to use "/" [_example/stage4.py:77 / 4 / Path (False, False)]
    update .chdir to use Path.chdir() or os.chdir(str(Path)) [_example/stage4.py:80 / 1 / Path (False,)]

showing where you still use string based paths/filenames.

The message part `file_name.py: 123 / 2 / Path (True, False)` means that
there were two calls on line 123 in `file_name.py` and that they were
called with the first parameter being a Path, the second not being a
Path (when replacing `os.path.join()` with Path\'s `"/"` concatenation
operator that would be a good starting point, for other situation you
might want to convert the second parameter to a Path instance as well).

## Extending `PathLibConversionHelper`

If `PathLibConversionHelper` doesn\'t contain a particular function
(yet) you can easily subclass it and add your own:

    from ruamel.std.pathlib import Path, PathLibConversionHelper


    class MyPLCH(PathLibConversionHelper):
        # an example, ruamel.std.pathlib already adds mkstemp
        def mkstemp(self, suffix="", prefix=None, dir=None, text=False):
            import tempfile
            # would be much better if prefix defaults to built-in value (int, None, string)
            if prefix is None:
                prefix = tempfile.template
            self.__add_usage(dir, 'update .mkstemp to use Path.mkstemp()')
            if isinstance(dir, Path):
                dir = str(dir)
            return tempfile.mkstemp(suffix, prefix, dir, text)

    pl = MyPLCH(check=1)

The first parameter for `self.add_usage()` is used to determine if a
Path is used or not. This should be a list of all relevant variables
(that could be `Path` instances or not). If the list would only have a
single element it doesn\'t have to be passed in as a list (as in the
example). The second parameter should be a string with some help on
further getting rid of the call to `.mkstemp()`.
