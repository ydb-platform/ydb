# Copyright (c) 2015-2019, Activision Publishing, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import os
import sys

if sys.version_info[0] == 3:
    str_types = (str,)
else:
    str_types = (basestring,)

__tracebackhide__ = True


def contents_of(file, encoding='utf-8'):
    """Helper to read the contents of the given file or path into a string with the given encoding.

    Args:
        file: a *path-like object* (aka a file name) or a *file-like object* (aka a file)
        encoding (str): the target encoding.  Defaults to ``utf-8``, other useful encodings are ``ascii`` and ``latin-1``.

    Examples:
        Usage::

            from assertpy import assert_that, contents_of

            contents = contents_of('foo.txt')
            assert_that(contents).starts_with('foo').ends_with('bar').contains('oob')

    Returns:
        str: returns the file contents as a string

    Raises:
        IOError: if file not found
        TypeError: if file is not a *path-like object* or a *file-like object*
    """
    try:
        contents = file.read()
    except AttributeError:
        try:
            with open(file, 'r') as fp:
                contents = fp.read()
        except TypeError:
            raise ValueError('val must be file or path, but was type <%s>' % type(file).__name__)
        except OSError:
            if not isinstance(file, str_types):
                raise ValueError('val must be file or path, but was type <%s>' % type(file).__name__)
            raise

    if sys.version_info[0] == 3 and type(contents) is bytes:
        # in PY3 force decoding of bytes to target encoding
        return contents.decode(encoding, 'replace')
    elif sys.version_info[0] == 2 and encoding == 'ascii':
        # in PY2 force encoding back to ascii
        return contents.encode('ascii', 'replace')
    else:
        # in all other cases, try to decode to target encoding
        try:
            return contents.decode(encoding, 'replace')
        except AttributeError:
            pass
    # if all else fails, just return the contents "as is"
    return contents


class FileMixin(object):
    """File assertions mixin."""

    def exists(self):
        """Asserts that val is a path and that it exists.

        Examples:
            Usage::

                assert_that('myfile.txt').exists()
                assert_that('mydir').exists()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** exist
        """
        if not isinstance(self.val, str_types):
            raise TypeError('val is not a path')
        if not os.path.exists(self.val):
            self.error('Expected <%s> to exist, but was not found.' % self.val)
        return self

    def does_not_exist(self):
        """Asserts that val is a path and that it does *not* exist.

        Examples:
            Usage::

                assert_that('missing.txt').does_not_exist()
                assert_that('missing_dir').does_not_exist()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val **does** exist
        """
        if not isinstance(self.val, str_types):
            raise TypeError('val is not a path')
        if os.path.exists(self.val):
            self.error('Expected <%s> to not exist, but was found.' % self.val)
        return self

    def is_file(self):
        """Asserts that val is a *file* and that it exists.

        Examples:
            Usage::

                assert_that('myfile.txt').is_file()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** exist, or is **not** a file
        """
        self.exists()
        if not os.path.isfile(self.val):
            self.error('Expected <%s> to be a file, but was not.' % self.val)
        return self

    def is_directory(self):
        """Asserts that val is a *directory* and that it exists.

        Examples:
            Usage::

                assert_that('mydir').is_directory()

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** exist, or is **not** a directory
        """
        self.exists()
        if not os.path.isdir(self.val):
            self.error('Expected <%s> to be a directory, but was not.' % self.val)
        return self

    def is_named(self, filename):
        """Asserts that val is an existing path to a file and that file is named filename.

        Args:
            filename: the expected filename

        Examples:
            Usage::

                assert_that('/path/to/mydir/myfile.txt').is_named('myfile.txt')

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** exist, or is **not** a file, or is **not** named the given filename
        """
        self.is_file()
        if not isinstance(filename, str_types):
            raise TypeError('given filename arg must be a path')
        val_filename = os.path.basename(os.path.abspath(self.val))
        if val_filename != filename:
            self.error('Expected filename <%s> to be equal to <%s>, but was not.' % (val_filename, filename))
        return self

    def is_child_of(self, parent):
        """Asserts that val is an existing path to a file and that file is a child of parent.

        Args:
            parent: the expected parent directory

        Examples:
            Usage::

                assert_that('/path/to/mydir/myfile.txt').is_child_of('mydir')
                assert_that('/path/to/mydir/myfile.txt').is_child_of('to')
                assert_that('/path/to/mydir/myfile.txt').is_child_of('path')

        Returns:
            AssertionBuilder: returns this instance to chain to the next assertion

        Raises:
            AssertionError: if val does **not** exist, or is **not** a file, or is **not** a child of the given directory
        """
        self.is_file()
        if not isinstance(parent, str_types):
            raise TypeError('given parent directory arg must be a path')
        val_abspath = os.path.abspath(self.val)
        parent_abspath = os.path.abspath(parent)
        if not val_abspath.startswith(parent_abspath):
            self.error('Expected file <%s> to be a child of <%s>, but was not.' % (val_abspath, parent_abspath))
        return self
