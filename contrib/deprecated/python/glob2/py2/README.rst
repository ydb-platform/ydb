python-glob2
============

This is an extended version of Python's builtin glob module
(http://docs.python.org/library/glob.html) which adds:

- The ability to capture the text matched by glob patterns, and
  return those matches alongside the filenames.

- A recursive '**' globbing syntax, akin for example to the ``globstar``
  option of the bash shell.

- The ability to replace the filesystem functions used, in order to glob
  on virtual filesystems.

- Compatible with Python 2 and Python 3 (tested with 3.3).

It's currently based on the glob code from Python 3.3.1.


Examples
--------

Matches being returned:
~~~~~~~~~~~~~~~~~~~~~~~

::

    import glob2

    for filename, (version,) in glob2.iglob('./binaries/project-*.zip', with_matches=True):
        print version


Recursive glob:
~~~~~~~~~~~~~~~

::

    >>> import glob2
    >>> all_header_files = glob2.glob('src/**/*.h')
    ['src/fs.h', 'src/media/mp3.h', 'src/media/mp3/frame.h', ...]


Note that ``**`` must appear on it's own as a directory
element to have its special meaning. ``**h`` will not have the
desired effect.

``**`` will match ".", so ``**/*.py`` returns Python files in the
current directory. If this is not wanted, ``*/**/*.py`` should be used
instead.


Custom Globber:
~~~~~~~~~~~~~~~

::

    from glob2 import Globber

    class VirtualStorageGlobber(Globber):
        def __init__(self, storage):
            self.storage = storage
        def listdir(self, path):
            # Must raise os.error if path is not a directory
            return self.storage.listdir(path)
        def exists(self, path):
            return self.storage.exists(path)
        def isdir(self, path):
            # Used only for trailing slash syntax (``foo/``).
            return self.storage.isdir(path)
        def islink(self, path):
            # Used only for recursive glob (``**``).
            return self.storage.islink(path)

    globber = VirtualStorageGlobber(sftp_storage)
    globber.glob('/var/www/**/*.js')


If ``isdir`` and/or ``islink`` cannot be implemented for a storage, you can
make them return a fixed value, with the following consequences:

- If ``isdir`` returns ``True``, a glob expression ending with a slash
  will return all items, even non-directories, if it returns ``False``,
  the same glob expression will return nothing.

- Return ``islink`` ``True``, the recursive globbing syntax ** will
  follow all links. If you return ``False``, it will not work at all.
