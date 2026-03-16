"""Nomad Evaluations: https://developer.hashicorp.com/nomad/api-docs/evaluations"""

import nomad.api.exceptions

from nomad.api.base import Requester


class Evaluations(Requester):
    """
    The evaluations endpoint is used to query the status of evaluations.
    By default, the agent's local region is used; another region can
    be specified using the ?region= query parameter.

    https://www.nomadproject.io/docs/http/evals.html
    """

    ENDPOINT = "evaluations"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        raise AttributeError

    def __contains__(self, item):
        try:
            evaluations = self.get_evaluations()

            for evaluation in evaluations:
                if evaluation["ID"] == item:
                    return True

            return False
        except nomad.api.exceptions.URLNotFoundNomadException:
            return False

    def __len__(self):
        evaluations = self.get_evaluations()
        return len(evaluations)

    def __getitem__(self, item):
        try:
            evaluations = self.get_evaluations()

            for evaluation in evaluations:
                if evaluation["ID"] == item:
                    return evaluation
            raise KeyError
        except nomad.api.exceptions.URLNotFoundNomadException as exc:
            raise KeyError from exc

    def __iter__(self):
        evaluations = self.get_evaluations()
        return iter(evaluations)

    def get_evaluations(self, prefix=None):
        """Lists all the evaluations.

        https://www.nomadproject.io/docs/http/evals.html
        arguments:
          - prefix :(str) optional, specifies a string to filter evaluations on based on an prefix.
                    This is specified as a querystring parameter.
        returns: list
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        params = {"prefix": prefix}
        return self.request(method="get", params=params).json()
