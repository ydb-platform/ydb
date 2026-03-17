# coding: utf-8

import os
import shutil

from django.core.files.move import file_move_safe
from django.utils.encoding import smart_str

from filebrowser.base import FileObject
from filebrowser.settings import DEFAULT_PERMISSIONS


class StorageMixin(object):
    """
    Adds some useful methods to the Storage class.
    """

    def isdir(self, name):
        """
        Returns true if name exists and is a directory.
        """
        raise NotImplementedError()

    def isfile(self, name):
        """
        Returns true if name exists and is a regular file.
        """
        raise NotImplementedError()

    def move(self, old_file_name, new_file_name, allow_overwrite=False):
        """
        Moves safely a file from one location to another.

        If allow_ovewrite==False and new_file_name exists, raises an exception.
        """
        raise NotImplementedError()

    def makedirs(self, name):
        """
        Creates all missing directories specified by name. Analogue to os.mkdirs().
        """
        raise NotImplementedError()

    def rmtree(self, name):
        """
        Deletes a directory and everything it contains. Analogue to shutil.rmtree().
        """
        raise NotImplementedError()

    def setpermission(self, name):
        """
        Sets file permission
        """
        raise NotImplementedError()


class FileSystemStorageMixin(StorageMixin):

    def isdir(self, name):
        return os.path.isdir(self.path(name))

    def isfile(self, name):
        return os.path.isfile(self.path(name))

    def move(self, old_file_name, new_file_name, allow_overwrite=False):
        file_move_safe(self.path(old_file_name), self.path(new_file_name), allow_overwrite=True)

    def makedirs(self, name):
        os.makedirs(self.path(name))

    def rmtree(self, name):
        shutil.rmtree(self.path(name))

    def setpermission(self, name):
        full_path = FileObject(smart_str(name), site=self).path_full
        os.chmod(full_path, DEFAULT_PERMISSIONS)


class S3BotoStorageMixin(StorageMixin):

    def isfile(self, name):
        return self.exists(name)

    def isdir(self, name):
        # That's some inefficient implementation...
        # If there are some files having 'name' as their prefix, then
        # the name is considered to be a directory
        if not name:  # Empty name is a directory
            return True

        if self.isfile(name):
            return False

        name = self._normalize_name(self._clean_name(name))
        dirlist = self.bucket.list(self._encode_name(name))

        # Check whether the iterator is empty
        for item in dirlist:
            return True
        return False

    def move(self, old_file_name, new_file_name, allow_overwrite=False):

        if self.exists(new_file_name):
            if allow_overwrite:
                self.delete(new_file_name)
            else:
                raise "The destination file '%s' exists and allow_overwrite is False" % new_file_name

        old_key_name = self._encode_name(self._normalize_name(self._clean_name(old_file_name)))
        new_key_name = self._encode_name(self._normalize_name(self._clean_name(new_file_name)))

        k = self.bucket.copy_key(new_key_name, self.bucket.name, old_key_name)

        if not k:
            raise "Couldn't copy '%s' to '%s'" % (old_file_name, new_file_name)

        self.delete(old_file_name)

    def makedirs(self, name):
        pass

    def rmtree(self, name):
        name = self._normalize_name(self._clean_name(name))
        dirlist = self.bucket.list(self._encode_name(name))
        for item in dirlist:
            item.delete()

    def setpermission(self, name):
        # Permissions for S3 uploads with django-storages
        # is set in settings.py with AWS_DEFAULT_ACL.
        # More info: http://django-common-configs.readthedocs.org/en/latest/configs/storage.html
        pass


class DropBoxStorageMixin(StorageMixin):

    def isdir(self, name):
        # NOTE: We are importing inside the function to not to require
        # the dropbox library if DropBoxStorageMixin is not used
        # This could changed if storage module is broken down
        # into one module for each storage Mixin?
        from dropbox.exceptions import ApiError
        from dropbox.files import FileMetadata, FolderMetadata
        try:
            metadata = self.client.files_get_metadata(self._full_path(name))
            return isinstance(metadata, FolderMetadata)
        except ApiError:
            return False

    def isfile(self, name):
        from dropbox.exceptions import ApiError
        from dropbox.files import FileMetadata, FolderMetadata
        try:
            metadata = self.client.files_get_metadata(self._full_path(name))
            return isinstance(metadata, FileMetadata)
        except ApiError:
            return False

    def move(self, old_file_name, new_file_name, allow_overwrite=False):
        if self.exists(new_file_name):
            if allow_overwrite:
                self.delete(new_file_name)
            else:
                raise "The destination file '%s' exists and allow_overwrite is False" % new_file_name

        self.client.files_move(old_file_name, new_file_name)

    def makedirs(self, name):
        self.client.files_create_folder(self._full_path(name))

    def rmtree(self, name):
        self.client.files_delete(self._full_path(name))

    def setpermission(self, name):
        pass
