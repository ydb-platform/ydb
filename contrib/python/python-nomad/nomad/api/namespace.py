"""Nomad namespace: https://developer.hashicorp.com/nomad/api-docs/namespaces"""

import nomad.api.exceptions

from nomad.api.base import Requester


class Namespace(Requester):
    """
    The job endpoint is used for CRUD on a single namespace.
    By default, the agent's local region is used.

    https://www.nomadproject.io/api/namespaces.html
    """

    ENDPOINT = "namespace"

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
            self.get_namespace(item)
            return True
        except nomad.api.exceptions.URLNotFoundNomadException:
            return False

    def __getitem__(self, item):
        try:
            job = self.get_namespace(item)

            if job["ID"] == item:
                return job
            if job["Name"] == item:
                return job

            raise KeyError
        except nomad.api.exceptions.URLNotFoundNomadException as exc:
            raise KeyError from exc

    def get_namespace(self, id_):
        """Query a single namespace.

        https://www.nomadproject.io/api/namespaces.html

        arguments:
          - id_
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, method="get").json()

    def create_namespace(self, namespace):
        """create namespace

        https://www.nomadproject.io/api/namespaces.html

        arguments:
          - id
          - namespace (dict)
        returns: requests.Response
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(json=namespace, method="post")

    def update_namespace(self, id_, namespace):
        """Update namespace

        https://www.nomadproject.io/api/namespaces.html

        arguments:
          - id_
          - namespace (dict)
        returns: requests.Response
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, json=namespace, method="post")

    def delete_namespace(self, id_):
        """delete namespace.

        https://www.nomadproject.io/api/namespaces.html

        arguments:
          - id_
        returns: requests.Response
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request(id_, method="delete")
