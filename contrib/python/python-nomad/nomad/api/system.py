"""Nomad System API: https://developer.hashicorp.com/nomad/api-docs/system"""

from nomad.api.base import Requester


class System(Requester):
    """
    The system endpoint is used to for system maintenance
    and should not be necessary for most users.
    By default, the agent's local region is used.

    https://www.nomadproject.io/docs/http/system.html
    """

    ENDPOINT = "system"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)

    def initiate_garbage_collection(self):
        """Initiate garbage collection of jobs, evals, allocations and nodes.

        https://www.nomadproject.io/docs/http/system.html

        returns: Boolean
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request("gc", method="put").ok

    def reconcile_summaries(self):
        """This endpoint reconciles the summaries of all registered jobs.

        https://www.nomadproject.io/docs/http/system.html

        returns: Boolean
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request("reconcile", "summaries", method="put").ok
