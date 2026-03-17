# coding: utf8
from __future__ import unicode_literals, absolute_import, division, print_function

import os
import sys
import tempfile

from six import BytesIO

from django.core.files import File
from django.core.files.storage import Storage, FileSystemStorage
from django.utils.deconstruct import deconstructible

from library.python import resource


class ResfsStorage(FileSystemStorage):
    """Хранилище внутри исполняемых аркадийных файлов ("ресурсной файловой системы" - resfs).

     Позволяет обращаться к статике, впаянной внутрь исполняемых аркадиныйх файлов,
     и вынимать её в файловую систему в STATIC_ROOT для последующей раздачи сервером.

     """
    def __init__(self, *args, **kwargs):
        super(ResfsStorage, self).__init__(*args, **kwargs)
        self._prefix = ''

    def path(self, name):
        # prefix - базовая директория (как PREFIX в RESOURCE_FILES);
        # location - поддиректория;
        # name - имя объекта (файла/директории)
        return os.path.join(self._prefix, self._location, name)

    def listdir(self, path):
        prefix = self.path(path)
        dirs = []
        files = [filename.replace(prefix, '', 1) for filename in resource.resfs_files(prefix)]

        return dirs, files

    def _open(self, name, mode='rb'):
        return File(BytesIO(resource.resfs_read(self.path(name))))


@deconstructible
class ResourceFilesStorage(Storage):

    def __init__(self, prefix=''):
        if prefix != '':
            assert prefix.endswith('/'), prefix
        self._prefix = prefix
        self.location = prefix
        self._abspath_prefix = sys.executable + '/' + prefix
        if sys.version_info.major >= 3:
            self._tempdir = tempfile.TemporaryDirectory()

    def path(self, name):
        """
        Returns a local filesystem path where the file can be retrieved using
        Python's built-in open() function. Storage systems that can't be
        accessed using open() should *not* implement this method.
        """
        if sys.version_info.major >= 3:
            path = os.path.join(self._tempdir.name, name)
            if not os.path.exists(path):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as f:
                    f.write(resource.resfs_read(self._prefix + name))
            return path
        else:
            return self._abspath_prefix + name

    def exists(self, name):
        """
        Returns True if a file referenced by the given name already exists in the
        storage system, or False if the name is available for a new file.
        """
        if name == "":
            return True
        else:
            return resource.resfs_file_exists(self._prefix + name)

    def listdir(self, path):
        """
        Lists the contents of the specified path, returning a 2-tuple of lists;
        the first item being directories, the second item being files.

        !!Warning!! Actually this method breaks contract of BaseFinder, as in our case list of dirs
        will always be empty, while list of files will contain paths to files from given path.

        Example:
            Given file tree in resfs is:
            somewhere/foo/bar/baz.py
            somewhere/foo/bar/quz/qux.html

            ResourceFilesStorage('somewhere/').listdir('foo')
            >>>> (
            >>>>     [],
            >>>>     [
            >>>>        'bar/baz.py',
            >>>>        'bar/quz/qux.html'
            >>>>     ],
            >>>> )
        """
        prefix = self._prefix + path
        if not prefix.endswith('/'):
            prefix += '/'
        dirs = []
        files = [s[len(prefix):] for s in resource.resfs_files(prefix=prefix)]
        return dirs, files

    def _open(self, name, mode='rb'):
        content = resource.resfs_read(self._prefix + name)
        return File(BytesIO(content))

    def _save(self, name, content):
        """
        Not implemented, as it's not obvious, whether we want to store something in resfs or not.
        It could be implemented by using NResource::TRegistry tough.
        """
        raise NotImplementedError("_save method isn't implemented")
