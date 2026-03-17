"""Nomad Valiables API: https://developer.hashicorp.com/nomad/api-docs/variables"""

import nomad.api.exceptions

from nomad.api.base import Requester


class Variable(Requester):
    """
    The /var endpoints are used to read or create variables.
    https://developer.hashicorp.com/nomad/api-docs/variables
    """

    ENDPOINT = "var"

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
            self.get_variable(item)
            return True
        except nomad.api.exceptions.URLNotFoundNomadException:
            return False

    def __getitem__(self, item):
        try:
            return self.get_variable(item)
        except nomad.api.exceptions.URLNotFoundNomadException as exc:
            raise KeyError from exc

    def get_variable(self, var_path, namespace=None):
        """
        This endpoint reads a specific variable by path. This API returns the decrypted variable body.
        https://developer.hashicorp.com/nomad/api-docs/variables#read-variable

        arguments:
          - var_path :(str), path to variable
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        params = {}
        if namespace:
            params["namespace"] = namespace

        return self.request(var_path, params=params, method="get").json()

    def create_variable(self, var_path, payload, namespace=None, cas=None):
        """
        This endpoint creates or updates a variable.
        https://developer.hashicorp.com/nomad/api-docs/variables#create-variable

        arguments:
          - var_path :(str), path to variable
          - payload :(dict), variable object. Example:
            https://developer.hashicorp.com/nomad/api-docs/variables#sample-payload
          - namespace :(str) optional, specifies the target namespace. Specifying * would return all jobs.
                This is specified as a querystring parameter.
          - cas :(int) optional, If set, the variable will only be deleted if the cas value matches the
            current variables ModifyIndex.
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
          - nomad.api.exceptions.VariableConflict
        """
        params = {}
        if cas is not None:
            params["cas"] = cas
        if namespace:
            params["namespace"] = namespace

        return self.request(var_path, params=params, json=payload, method="put").json()

    def delete_variable(self, var_path, namespace=None, cas=None):
        """
        This endpoint reads a specific variable by path. This API returns the decrypted variable body.
        https://developer.hashicorp.com/nomad/api-docs/variables#delete-variable

        arguments:
          - var_path :(str), path to variable
          - namespace :(str) optional, specifies the target namespace. Specifying * would return all jobs.
                This is specified as a querystring parameter.
          - cas :(int) optional, If set, the variable will only be deleted if the cas value matches the
                current variables ModifyIndex.
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
          - nomad.api.exceptions.VariableConflict
        """
        params = {}
        if cas is not None:
            params["cas"] = cas
        if namespace:
            params["namespace"] = namespace

        # we need to return json here but because of bug we recieve empty response
        return self.request(var_path, params=params, method="delete")
