"""Nomad Validate API: https://developer.hashicorp.com/nomad/api-docs/validate"""

from nomad.api.base import Requester


class Validate(Requester):
    """
    The system endpoint is used to for system maintenance
    and should not be necessary for most users.
    By default, the agent's local region is used.

    https://www.nomadproject.io/docs/http/system.html
    """

    ENDPOINT = "validate"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        return f"{self.__dict__}"

    def __getattr__(self, item):
        msg = f"{item} does not exist"
        raise AttributeError(msg)

    def validate_job(self, nomad_job_dict):
        """This endpoint validates a Nomad job file. The local Nomad agent forwards the request to a server.
        In the event a server can't be reached the agent verifies the job file locally but skips validating driver
        configurations.

        https://www.nomadproject.io/api/validate.html

        arguments:
          - nomad_job_json, any valid nomad job IN dict FORMAT
        returns: dict
        raises:
          - nomad.api.exceptions.BaseNomadException
          - nomad.api.exceptions.URLNotFoundNomadException
        """
        return self.request("job", json=nomad_job_dict, method="post")
