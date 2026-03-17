import abc
import json
import logging
import re
from typing import Optional

import requests


class _ErrorDeserializer(abc.ABC):
    """A parser for errors from the Databricks REST API."""

    @abc.abstractmethod
    def deserialize_error(self, response: requests.Response, response_body: bytes) -> Optional[dict]:
        """Parses an error from the Databricks REST API. If the error cannot be parsed, returns None."""


class _EmptyDeserializer(_ErrorDeserializer):
    """A parser that handles empty responses."""

    def deserialize_error(self, response: requests.Response, response_body: bytes) -> Optional[dict]:
        if len(response_body) == 0:
            return {"message": response.reason}
        return None


class _StandardErrorDeserializer(_ErrorDeserializer):
    """
    Parses errors from the Databricks REST API using the standard error format.
    """

    def deserialize_error(self, response: requests.Response, response_body: bytes) -> Optional[dict]:
        try:
            payload_str = response_body.decode("utf-8")
            resp = json.loads(payload_str)
        except UnicodeDecodeError as e:
            logging.debug(
                "_StandardErrorParser: unable to decode response using utf-8",
                exc_info=e,
            )
            return None
        except json.JSONDecodeError as e:
            logging.debug(
                "_StandardErrorParser: unable to deserialize response as json",
                exc_info=e,
            )
            return None
        if not isinstance(resp, dict):
            logging.debug("_StandardErrorParser: response is valid JSON but not a dictionary")
            return None

        error_args = {
            "message": resp.get("message", "request failed"),
            "error_code": resp.get("error_code"),
            "details": resp.get("details"),
        }

        # Handle API 1.2-style errors
        if "error" in resp:
            error_args["message"] = resp["error"]

        # Handle SCIM Errors
        detail = resp.get("detail")
        status = resp.get("status")
        scim_type = resp.get("scimType")
        if detail:
            # Handle SCIM error message details
            # @see https://tools.ietf.org/html/rfc7644#section-3.7.3
            if detail == "null":
                detail = "SCIM API Internal Error"
            error_args["message"] = f"{scim_type} {detail}".strip(" ")
            error_args["error_code"] = f"SCIM_{status}"
        return error_args


class _StringErrorDeserializer(_ErrorDeserializer):
    """
    Parses errors from the Databricks REST API in the format "ERROR_CODE: MESSAGE".
    """

    __STRING_ERROR_REGEX = re.compile(r"([A-Z_]+): (.*)")

    def deserialize_error(self, response: requests.Response, response_body: bytes) -> Optional[dict]:
        payload_str = response_body.decode("utf-8")
        match = self.__STRING_ERROR_REGEX.match(payload_str)
        if not match:
            logging.debug("_StringErrorParser: unable to parse response as string")
            return None
        error_code, message = match.groups()
        return {
            "error_code": error_code,
            "message": message,
            "status": response.status_code,
        }


class _HtmlErrorDeserializer(_ErrorDeserializer):
    """
    Parses errors from the Databricks REST API in HTML format.
    """

    __HTML_ERROR_REGEXES = [
        re.compile(r"<pre>(.*)</pre>"),
        re.compile(r"<title>(.*)</title>"),
    ]

    def deserialize_error(self, response: requests.Response, response_body: bytes) -> Optional[dict]:
        payload_str = response_body.decode("utf-8")
        for regex in self.__HTML_ERROR_REGEXES:
            match = regex.search(payload_str)
            if match:
                message = match.group(1) if match.group(1) else response.reason
                return {
                    "status": response.status_code,
                    "message": message,
                    "error_code": response.reason.upper().replace(" ", "_"),
                }
        logging.debug("_HtmlErrorParser: no <pre> tag found in error response")
        return None
