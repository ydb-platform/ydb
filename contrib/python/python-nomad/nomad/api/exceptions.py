"""Internal library exceptions"""

import requests


class BaseNomadException(Exception):
    """General Error occurred when interacting with nomad API"""

    def __init__(self, nomad_resp):
        self.nomad_resp = nomad_resp

    def __str__(self):
        if isinstance(self.nomad_resp, requests.Response) and hasattr(self.nomad_resp, "text"):
            return f"The {self.__class__.__name__} was raised with following response: {self.nomad_resp.text}."

        return f"The {self.__class__.__name__} was raised due to the following error: {self.nomad_resp}"


class URLNotFoundNomadException(BaseNomadException):
    """The requeted URL given does not exist"""


class URLNotAuthorizedNomadException(BaseNomadException):
    """The requested URL is not authorized. ACL"""


class BadRequestNomadException(BaseNomadException):
    """Validation failure and if a parameter is modified in the request, it could potentially succeed."""


class VariableConflict(BaseNomadException):
    """In the case of a compare-and-set variable conflict"""


class InvalidParameters(Exception):
    """Invalid parameters given"""


class TimeoutNomadException(requests.exceptions.RequestException):
    """Timeout on request, if using a stream and timeout in conjunction"""
