"""Nomad Sentinel API: https://developer.hashicorp.com/nomad/api-docs/sentinel-policies"""

from nomad.api.base import Requester


class Sentinel(Requester):
    """
    The endpoint manage sentinel policies (Enterprise Only)

    https://www.nomadproject.io/api/sentinel-policies.html
    """

    ENDPOINT = "sentinel"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)

    def get_policies(self):
        """Get a list of policies.

        https://www.nomadproject.io/api/sentinel-policies.html

        returns: list

        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request("policies", method="get").json()

    def create_policy(self, id_, policy):
        """Create policy.

        https://www.nomadproject.io/api/sentinel-policies.html

        arguments:
            - policy
        returns: requests.Response

        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request("policy", id_, json=policy, method="post")

    def get_policy(self, id_):
        """Get a spacific policy.

        https://www.nomadproject.io/api/sentinel-policies.html

        returns: dict

        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request("policy", id_, method="get").json()

    def update_policy(self, id_, policy):
        """Create policy.

        https://www.nomadproject.io/api/sentinel-policies.html

        arguments:
            - name
            - policy
        returns: requests.Response

        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request("policy", id_, json=policy, method="post")

    def delete_policy(self, id_):
        """Delete specific policy.

        https://www.nomadproject.io/api/sentinel-policies.html

        arguments:
            - id_
        returns: Boolean

        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request("policy", id_, method="delete").ok
