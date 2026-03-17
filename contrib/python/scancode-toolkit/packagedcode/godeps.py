#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from collections import namedtuple
import io
import json
import logging


"""
Handle Godeps-like Go package dependency data.

Note: there are other dependency tools for Go beside Godeps, yet
several use the same format.
"""
# FIXME: update to use the latest vendor conventions.

TRACE = False


def logger_debug(*args):
    pass


logger = logging.getLogger(__name__)

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))


class Dep(namedtuple('Dep', 'import_path revision comment')):

    def __new__(cls, import_path=None, revision=None, comment=None):
        return super(Dep, cls).__new__(cls, import_path, revision, comment)


# map Godep names to our own attribute names
NAMES = {
    'ImportPath': 'import_path',
    'GoVersion': 'go_version',
    'Packages': 'packages',
    'Deps': 'dependencies',
    'Comment': 'comment',
    'Rev': 'revision',
}


class Godep(object):
    """
    A JSON dep file with this structure:
        type Godeps struct {
            ImportPath string
            GoVersion  string   // Abridged output of 'go version'.
            Packages   []string // Arguments to godep save, if any.
            Deps       []struct {
                ImportPath string
                Comment    string // Description of commit, if present.
                Rev        string // VCS-specific commit ID.
            }
        }

    ImportPath
    GoVersion
    Packages
    Deps
        ImportPath
        Comment
        Rev
    """

    def __init__(self, location=None, import_path=None, go_version=None,
                 packages=None, dependencies=None):

        self.location = location
        self.import_path = None
        self.go_version = None
        self.dependencies = []
        self.packages = []

        if location:
            self.load(location)
        else:
            self.import_path = import_path
            self.go_version = go_version
            self.dependencies = dependencies or []
            self.packages = packages or []

    def load(self, location):
        """
        Load self from a location string or a file-like object
        containing a Godeps JSON.
        """
        if isinstance(location, str):
            with io.open(location, encoding='utf-8') as godep:
                data = json.load(godep)
        else:
            data = json.load(location)

        for key, value in data.items():
            name = NAMES.get(key)
            if name == 'dependencies':
                self.dependencies = self.parse_deps(value)
            else:
                setattr(self, name, value)
        return self

    def loads(self, string):
        """
        Load a Godeps JSON string.
        """
        from io import StringIO
        self.load(StringIO(string))
        return self

    def parse_deps(self, deps):
        deps_list = []
        for dep in deps:
            data = dict((NAMES[key], value) for key, value in dep.items())
            deps_list.append(Dep(**data))
        return deps_list or []

    def to_dict(self):
        dct = {}
        dct.update([
            ('import_path', self.import_path),
            ('go_version', self.go_version),
            ('packages', self.packages),
            ('dependencies', [d._asdict() for d in self.dependencies]),
        ])
        return dct

    def __repr__(self):
        return ('Godep(%r)' % self.to_dict())

    __str__ = __repr__


def parse(location):
    """
    Return a mapping of parsed Godeps from the file at `location`.
    """
    return Godep(location).to_dict()
