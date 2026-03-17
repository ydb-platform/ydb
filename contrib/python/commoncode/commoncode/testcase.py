#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import filecmp
import os
import shutil
import stat
import sys

from os import path
from collections import defaultdict
from itertools import chain
from unittest import TestCase as TestCaseClass

from commoncode import fileutils
from commoncode import filetype
from commoncode.system import on_posix
from commoncode.system import on_windows
from commoncode.archive import extract_tar
from commoncode.archive import extract_tar_raw
from commoncode.archive import extract_tar_uni
from commoncode.archive import extract_zip
from commoncode.archive import extract_zip_raw
from commoncode.archive import tar_can_extract  # NOQA

# a base test dir specific to a given test run
# to ensure that multiple tests run can be launched in parallel
test_run_temp_dir = None

# set to 1 to see the slow tests
timing_threshold = sys.maxsize


def to_os_native_path(path):
    """
    Normalize a path to use the native OS path separator.
    """
    OS_PATH_SEP = '\\' if on_windows else '/'

    return (
        path.replace('/', OS_PATH_SEP)
        .replace(u'\\', OS_PATH_SEP)
        .rstrip(OS_PATH_SEP)
    )


def get_test_loc(
    test_path,
    test_data_dir,
    debug=False,
    must_exist=True,
):
    """
    Given a `test_path` relative to the `test_data_dir` directory, return the
    location to a test file or directory for this path. No copy is done.
    Raise an IOError if `must_exist` is True and the `test_path` does not exists.
    """
    if debug:
        import inspect
        caller = inspect.stack()[1][3]
        print('\nget_test_loc,%(caller)s,"%(test_path)s","%(test_data_dir)s"' % locals())

    assert test_path
    assert test_data_dir

    if not path.exists(test_data_dir):
        raise IOError("[Errno 2] No such directory: test_data_dir not found:"
                      " '%(test_data_dir)s'" % locals())

    tpath = to_os_native_path(test_path)
    test_loc = path.abspath(path.join(test_data_dir, tpath))

    if must_exist and not path.exists(test_loc):
        raise IOError("[Errno 2] No such file or directory: "
                      "test_path not found: '%(test_loc)s'" % locals())

    return test_loc


class FileDrivenTesting(object):
    """
    Add support for handling test files and directories, including managing
    temporary test resources and doing file-based assertions.
    This can be used as a standalone object if needed.
    """
    test_data_dir = None

    def get_test_loc(self, test_path, copy=False, debug=False, must_exist=True):
        """
        Given a `test_path` relative to the self.test_data_dir directory, return the
        location to a test file or directory for this path. Copy to a temp
        test location if `copy` is True.

        Raise an IOError if `must_exist` is True and the `test_path` does not
        exists.
        """
        test_data_dir = self.test_data_dir
        if debug:
            import inspect
            caller = inspect.stack()[1][3]
            print('\nself.get_test_loc,%(caller)s,"%(test_path)s"' % locals())

        test_loc = get_test_loc(
            test_path,
            test_data_dir,
            debug=debug,
            must_exist=must_exist,
        )
        if copy:
            base_name = path.basename(test_loc)
            if filetype.is_file(test_loc):
                # target must be an existing dir
                target_dir = self.get_temp_dir()
                fileutils.copyfile(test_loc, target_dir)
                test_loc = path.join(target_dir, base_name)
            else:
                # target must be a NON existing dir
                target_dir = path.join(self.get_temp_dir(), base_name)
                fileutils.copytree(test_loc, target_dir)
                # cleanup of VCS that could be left over from checkouts
                self.remove_vcs(target_dir)
                test_loc = target_dir
        return test_loc

    def get_temp_file(self, extension=None, dir_name='td', file_name='tf'):
        """
        Return a unique new temporary file location to a non-existing temporary
        file that can safely be created without a risk of name collision.
        """
        if extension is None:
            extension = '.txt'

        if extension and not extension.startswith('.'):
                extension = '.' + extension

        file_name = file_name + extension
        temp_dir = self.get_temp_dir(dir_name)
        location = path.join(temp_dir, file_name)
        return location

    def get_temp_dir(self, sub_dir_path=None):
        """
        Create a unique new temporary directory location. Create directories
        identified by sub_dir_path if provided in this temporary directory.
        Return the location for this unique directory joined with the
        `sub_dir_path` if any.
        """
        # ensure that we have a new unique temp directory for each test run
        global test_run_temp_dir
        if not test_run_temp_dir:
            import tempfile
            test_tmp_root_dir = tempfile.gettempdir()
            # now we add a space in the path for testing path with spaces
            test_run_temp_dir = fileutils.get_temp_dir(
                base_dir=test_tmp_root_dir, prefix='scancode-tk-tests -')

        test_run_temp_subdir = fileutils.get_temp_dir(
            base_dir=test_run_temp_dir, prefix='')

        if sub_dir_path:
            # create a sub directory hierarchy if requested
            sub_dir_path = to_os_native_path(sub_dir_path)
            test_run_temp_subdir = path.join(test_run_temp_subdir, sub_dir_path)
            fileutils.create_dir(test_run_temp_subdir)
        return test_run_temp_subdir

    def remove_vcs(self, test_dir):
        """
        Remove some version control directories and some temp editor files.
        """
        vcses = ('CVS', '.svn', '.git', '.hg')
        for root, dirs, files in os.walk(test_dir):
            for vcs_dir in vcses:
                if vcs_dir in dirs:
                    for vcsroot, vcsdirs, vcsfiles in os.walk(test_dir):
                        for vcsfile in vcsdirs + vcsfiles:
                            vfile = path.join(vcsroot, vcsfile)
                            fileutils.chmod(vfile, fileutils.RW, recurse=False)
                    shutil.rmtree(path.join(root, vcs_dir), False)

            # editors temp file leftovers
            tilde_files = [path.join(root, file_loc)
                           for file_loc in files if file_loc.endswith('~')]
            for tf in tilde_files:
                os.remove(tf)

    def __extract(self, test_path, extract_func=None, verbatim=False):
        """
        Given an archive file identified by test_path relative
        to a test files directory, return a new temp directory where the
        archive file has been extracted using extract_func.
        If `verbatim` is True preserve the permissions.
        """
        assert test_path and test_path != ''
        test_path = to_os_native_path(test_path)
        target_path = path.basename(test_path)
        target_dir = self.get_temp_dir(target_path)
        original_archive = self.get_test_loc(test_path)
        extract_func(original_archive, target_dir, verbatim=verbatim)
        return target_dir

    def extract_test_zip(self, test_path, *args, **kwargs):
        return self.__extract(test_path, extract_zip)

    def extract_test_zip_raw(self, test_path, *args, **kwargs):
        return self.__extract(test_path, extract_zip_raw)

    def extract_test_tar(self, test_path, verbatim=False):
        return self.__extract(test_path, extract_tar, verbatim)

    def extract_test_tar_raw(self, test_path, *args, **kwargs):
        return self.__extract(test_path, extract_tar_raw)

    def extract_test_tar_unicode(self, test_path, *args, **kwargs):
        return self.__extract(test_path, extract_tar_uni)


class FileBasedTesting(TestCaseClass, FileDrivenTesting):
    pass


class dircmp(filecmp.dircmp):
    """
    Compare the content of dir1 and dir2. In contrast with filecmp.dircmp,
    this subclass also compares the content of files with the same path.
    """

    def phase3(self):
        """
        Find out differences between common files.
        Ensure we are using content comparison, not os.stat-only.
        """
        comp = filecmp.cmpfiles(self.left, self.right, self.common_files, shallow=False)
        self.same_files, self.diff_files, self.funny_files = comp


def is_same(dir1, dir2):
    """
    Compare two directory trees for structure and file content.
    Return False if they differ, True is they are the same.
    """
    compared = dircmp(dir1, dir2)
    if (compared.left_only or compared.right_only or compared.diff_files
        or compared.funny_files):
        return False

    for subdir in compared.common_dirs:
        if not is_same(path.join(dir1, subdir),
                       path.join(dir2, subdir)):
            return False
    return True


def file_cmp(file1, file2, ignore_line_endings=False):
    """
    Compare two files content.
    Return False if they differ, True is they are the same.
    """
    with open(file1, 'rb') as f1:
        f1c = f1.read()
        if ignore_line_endings:
            f1c = b'\n'.join(f1c.splitlines(False))
    with open(file2, 'rb') as f2:
        f2c = f2.read()
        if ignore_line_endings:
            f2c = b'\n'.join(f2c.splitlines(False))
    assert f2c == f1c


def make_non_readable(location):
    """
    Make location non readable for tests purpose.
    """
    if on_posix:
        current_stat = stat.S_IMODE(os.lstat(location).st_mode)
        os.chmod(location, current_stat & ~stat.S_IREAD)
    else:
        os.chmod(location, 0o555)


def make_non_writable(location):
    """
    Make location non writable for tests purpose.
    """
    if on_posix:
        current_stat = stat.S_IMODE(os.lstat(location).st_mode)
        os.chmod(location, current_stat & ~stat.S_IWRITE)
    else:
        make_non_readable(location)


def make_non_executable(location):
    """
    Make location non executable for tests purpose.
    """
    if on_posix:
        current_stat = stat.S_IMODE(os.lstat(location).st_mode)
        os.chmod(location, current_stat & ~stat.S_IEXEC)


def get_test_file_pairs(test_dir):
    """
    Yield tuples of (data_file, test_file) from a test data `test_dir` directory.
    Raise exception for orphaned/dangling files.
    Each test consist of a pair of files:
    - a test file.
    - a data file with the same name as a test file and a '.yml' extension added.
    Each test file path should be unique in the tree ignoring case.
    """
    # collect files with .yml extension and files with other extensions
    data_files = {}
    test_files = {}
    dangling_test_files = set()
    dangling_data_files = set()
    paths_ignoring_case = defaultdict(list)

    for top, _, files in os.walk(test_dir):
        for tfile in files:
            if tfile.endswith('~'):
                continue
            file_path = path.abspath(path.join(top, tfile))

            if tfile.endswith('.yml'):
                data_file_path = file_path
                test_file_path = file_path.replace('.yml', '')
            else:
                test_file_path = file_path
                data_file_path = test_file_path + '.yml'

            if not path.exists(test_file_path):
                dangling_test_files.add(test_file_path)

            if not path.exists(data_file_path):
                dangling_data_files.add(data_file_path)

            paths_ignoring_case[file_path.lower()].append(file_path)

            data_files[test_file_path] = data_file_path
            test_files[test_file_path] = test_file_path

    # ensure that we haev no dangling files
    if dangling_test_files or dangling_data_files:
        msg = ['Dangling missing test files without a YAML data file:'] + sorted(dangling_test_files)
        msg += ['Dangling missing YAML data files without a test file'] + sorted(dangling_data_files)
        msg = '\n'.join(msg)
        print(msg)
        raise Exception(msg)

    # ensure that each data file has a corresponding test file
    diff = set(data_files.keys()).symmetric_difference(set(test_files.keys()))
    if diff:
        msg = [
            'Orphaned copyright test file(s) found: '
            'test file without its YAML test data file '
            'or YAML test data file without its test file.'] + sorted(diff)
        msg = '\n'.join(msg)
        print(msg)
        raise Exception(msg)

    # ensure that test file paths are unique when you ignore case
    # we use the file names as test method names (and we have Windows that's
    # case insensitive
    dupes = list(chain.from_iterable(
        paths for paths in paths_ignoring_case.values() if len(paths) != 1))
    if dupes:
        msg = ['Non unique test/data file(s) found when ignoring case!'] + sorted(dupes)

        msg = '\n'.join(msg)
        print(msg)
        raise Exception(msg)

    for test_file in test_files:
        yield test_file + '.yml', test_file
