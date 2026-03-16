"""Nomad Operator: https://developer.hashicorp.com/nomad/api-docs/operator"""

from nomad.api.base import Requester


class Operator(Requester):
    """
    The Operator endpoint provides cluster-level tools for
    Nomad operators, such as interacting with the Raft subsystem.

    https://www.nomadproject.io/docs/http/operator.html
    """

    ENDPOINT = "operator"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)

    def get_configuration(self, stale=False):
        """Query the status of a client node registered with Nomad.

        https://www.nomadproject.io/docs/http/operator.html

        returns: dict
        optional arguments:
          - stale, (defaults to False), Specifies if the cluster should respond without an active leader.
                                        This is specified as a querystring parameter.
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """

        params = {"stale": stale}
        return self.request("raft", "configuration", params=params, method="get").json()

    def delete_peer(self, peer_address, stale=False):
        """Remove the Nomad server with given address from the Raft configuration.
        The return code signifies success or failure.

        https://www.nomadproject.io/docs/http/operator.html

        arguments:
          - peer_address, The address specifies the server to remove and is given as an IP:port
        optional arguments:
          - stale, (defaults to False), Specifies if the cluster should respond without an active leader.
                                        This is specified as a querystring parameter.
        returns: Boolean
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """

        params = {"address": peer_address, "stale": stale}
        return self.request("raft", "peer", params=params, method="delete").ok
