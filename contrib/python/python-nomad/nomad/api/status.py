"""Nomad Status API: https://developer.hashicorp.com/nomad/api-docs/status"""

import nomad.api.exceptions

from nomad.api.base import Requester


class Status:
    """
    By default, the agent's local region is used

    https://www.nomadproject.io/docs/http/status.html
    """

    def __init__(self, **kwargs):
        self.leader = Leader(**kwargs)
        self.peers = Peers(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)


class Leader(Requester):
    """This endpoint returns the address of the current leader in the region."""

    ENDPOINT = "status/leader"

    def __contains__(self, item):
        try:
            leader = self.get_leader()

            if leader == item:
                return True

            return False
        except nomad.api.exceptions.URLNotFoundNomadException:
            return False

    def __len__(self):
        leader = self.get_leader()
        return len(leader)

    def get_leader(self):
        """Returns the address of the current leader in the region.

        https://www.nomadproject.io/docs/http/status.html

        returns: string
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(method="get").json()


class Peers(Requester):
    """This endpoint returns the set of raft peers in the region."""

    ENDPOINT = "status/peers"

    def __contains__(self, item):
        try:
            peers = self.get_peers()

            for peer in peers:
                if peer == item:
                    return True
            return False
        except nomad.api.exceptions.URLNotFoundNomadException:
            return False

    def __len__(self):
        peers = self.get_peers()
        return len(peers)

    def __getitem__(self, item):
        try:
            peers = self.get_peers()

            for peer in peers:
                if peer == item:
                    return peer
            raise KeyError
        except nomad.api.exceptions.URLNotFoundNomadException as exc:
            raise KeyError from exc

    def __iter__(self):
        peers = self.get_peers()
        return iter(peers)

    def get_peers(self):
        """Returns the set of raft peers in the region.

        https://www.nomadproject.io/docs/http/status.html

        returns: list
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(method="get").json()
