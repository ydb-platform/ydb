"""Nomad Evaluation: https://developer.hashicorp.com/nomad/api-docs/evaluations"""

import nomad.api.exceptions

from nomad.api.base import Requester


class Evaluation(Requester):
    """
    The evaluation endpoint is used to query a specific evaluations.
    By default, the agent's local region is used; another region can
    be specified using the ?region= query parameter.

    https://www.nomadproject.io/docs/http/eval.html
    """

    ENDPOINT = "evaluation"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)

    def __contains__(self, item):
        try:
            self.get_evaluation(item)
            return True
        except nomad.api.exceptions.URLNotFoundNomadException:
            return False

    def __getitem__(self, item):
        try:
            evaluation = self.get_evaluation(item)
            if evaluation["ID"] == item:
                return evaluation
            raise KeyError
        except nomad.api.exceptions.URLNotFoundNomadException as exc:
            raise KeyError from exc

    def get_evaluation(self, id_):
        """Query a specific evaluation.

        https://www.nomadproject.io/docs/http/eval.html

         arguments:
           - id_
         returns: dict
         raises:
           - nomad.api.exceptions.BaseNomadException
           - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, method="get").json()

    def get_allocations(self, id_):
        """Query the allocations created or modified by an evaluation.

        https://www.nomadproject.io/docs/http/eval.html

         arguments:
           - id_
         returns: list
         raises:
           - nomad.api.exceptions.BaseNomadException
           - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, "allocations", method="get").json()
