"""Nomad Scalling API: https://developer.hashicorp.com/nomad/api-docs/scaling-policies"""

import nomad.api.exceptions

from nomad.api.base import Requester


class Scaling(Requester):
    """
    Endpoints are used to list and view scaling policies.

    https://developer.hashicorp.com/nomad/api-docs/scaling-policies
    """

    ENDPOINT = "scaling"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)

    # we want to have common arguments name with Nomad API
    def get_scaling_policies(self, job="", type_=""):  # pylint: disable=redefined-builtin
        """
        This endpoint returns the scaling policies from all jobs.

        https://developer.hashicorp.com/nomad/api-docs/scaling-policies#list-scaling-policies

        arguments:
          - job
          - type_
        returns: list of dicts
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        type_of_scaling_policies = [
            "horizontal",
            "vertical_mem",
            "vertical_cpu",
            "",
        ]  # we have only horizontal in OSS

        if type_ not in type_of_scaling_policies:
            raise nomad.api.exceptions.InvalidParameters(
                "type is invalid " f"(expected values are {type_of_scaling_policies} but got {type_})"
            )

        params = {"job": job, "type": type_}

        return self.request("policies", method="get", params=params).json()

    def get_scaling_policy(self, id_):
        """
        This endpoint reads a specific scaling policy.

        https://developer.hashicorp.com/nomad/api-docs/scaling-policies#read-scaling-policy

        arguments:
          - id_
        returns: list of dicts
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(f"policy/{id_}", method="get").json()
