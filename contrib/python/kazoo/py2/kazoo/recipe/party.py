"""Party

:Maintainer: Ben Bangert <ben@groovie.org>
:Status: Production

A Zookeeper pool of party members. The :class:`Party` object can be
used for determining members of a party.

"""
import uuid

from kazoo.exceptions import NodeExistsError, NoNodeError


class BaseParty(object):
    """Base implementation of a party."""
    def __init__(self, client, path, identifier=None):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The party path to use.
        :param identifier: An identifier to use for this member of the
                           party when participating.

        """
        self.client = client
        self.path = path
        self.data = str(identifier or "").encode('utf-8')
        self.ensured_path = False
        self.participating = False

    def _ensure_parent(self):
        if not self.ensured_path:
            # make sure our parent node exists
            self.client.ensure_path(self.path)
            self.ensured_path = True

    def join(self):
        """Join the party"""
        return self.client.retry(self._inner_join)

    def _inner_join(self):
        self._ensure_parent()
        try:
            self.client.create(self.create_path, self.data, ephemeral=True)
            self.participating = True
        except NodeExistsError:
            # node was already created, perhaps we are recovering from a
            # suspended connection
            self.participating = True

    def leave(self):
        """Leave the party"""
        self.participating = False
        return self.client.retry(self._inner_leave)

    def _inner_leave(self):
        try:
            self.client.delete(self.create_path)
        except NoNodeError:
            return False
        return True

    def __len__(self):
        """Return a count of participating clients"""
        self._ensure_parent()
        return len(self._get_children())

    def _get_children(self):
        return self.client.retry(self.client.get_children, self.path)


class Party(BaseParty):
    """Simple pool of participating processes"""
    _NODE_NAME = "__party__"

    def __init__(self, client, path, identifier=None):
        BaseParty.__init__(self, client, path, identifier=identifier)
        self.node = uuid.uuid4().hex + self._NODE_NAME
        self.create_path = self.path + "/" + self.node

    def __iter__(self):
        """Get a list of participating clients' data values"""
        self._ensure_parent()
        children = self._get_children()
        for child in children:
            try:
                d, _ = self.client.retry(self.client.get, self.path +
                                         "/" + child)
                yield d.decode('utf-8')
            except NoNodeError:  # pragma: nocover
                pass

    def _get_children(self):
        children = BaseParty._get_children(self)
        return [c for c in children if self._NODE_NAME in c]


class ShallowParty(BaseParty):
    """Simple shallow pool of participating processes

    This differs from the :class:`Party` as the identifier is used in
    the name of the party node itself, rather than the data. This
    places some restrictions on the length as it must be a valid
    Zookeeper node (an alphanumeric string), but reduces the overhead
    of getting a list of participants to a single Zookeeper call.

    """
    def __init__(self, client, path, identifier=None):
        BaseParty.__init__(self, client, path, identifier=identifier)
        self.node = '-'.join([uuid.uuid4().hex, self.data.decode('utf-8')])
        self.create_path = self.path + "/" + self.node

    def __iter__(self):
        """Get a list of participating clients' identifiers"""
        self._ensure_parent()
        children = self._get_children()
        for child in children:
            yield child[child.find('-') + 1:]
