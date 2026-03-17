# Copyright (C) 2006 Jelmer Vernooij <jelmer@jelmer.uk>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2.1 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA

"""Server backend base classes."""


class ServerBackend(object):
    """A server backend."""

    def open_repository(self, location):
        raise NotImplementedError(self.open_repository)


def generate_random_id():
    """Create a UUID for a repository."""
    import uuid
    return str(uuid.uuid4())


class ServerRepositoryBackend(object):

    def get_uuid(self):
        raise NotImplementedError(self.get_uuid)

    def get_latest_revnum(self):
        raise NotImplementedError(self.get_latest_revnum)

    def log(self, send_revision, target_path, start_rev, end_rev,
            changed_paths, strict_node, limit):
        raise NotImplementedError(self.log)

    def update(self, editor, revnum, target_path, recurse=True):
        raise NotImplementedError(self.update)

    def check_path(self, path, revnum):
        raise NotImplementedError(self.check_path)

    def stat(self, path, revnum):
        """Stat a path.

        Should return a dictionary with the following keys: name, kind, size,
            has-props, created-rev, created-date, last-author.
        """
        raise NotImplementedError(self.stat)

    def rev_proplist(self, revnum):
        raise NotImplementedError(self.rev_proplist)

    def get_locations(self, path, peg_revnum, revnums):
        raise NotImplementedError(self.get_locations)
