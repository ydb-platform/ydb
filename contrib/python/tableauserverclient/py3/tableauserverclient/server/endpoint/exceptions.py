from defusedxml.ElementTree import fromstring
from typing import Mapping, Optional, TypeVar


def split_pascal_case(s: str) -> str:
    return "".join([f" {c}" if c.isupper() else c for c in s]).strip()


class TableauError(Exception):
    pass


T = TypeVar("T")


class XMLError(TableauError):
    def __init__(self, code: str, summary: str, detail: str, url: Optional[str] = None) -> None:
        self.code = code
        self.summary = summary
        self.detail = detail
        self.url = url
        super().__init__(str(self))

    def __str__(self):
        return f"\n\n\t{self.code}: {self.summary}\n\t\t{self.detail}"

    @classmethod
    def from_response(cls, resp, ns, url):
        # Check elements exist before .text
        parsed_response = fromstring(resp)
        try:
            error_response = cls(
                parsed_response.find("t:error", namespaces=ns).get("code", ""),
                parsed_response.find(".//t:summary", namespaces=ns).text,
                parsed_response.find(".//t:detail", namespaces=ns).text,
                url,
            )
        except Exception as e:
            raise NonXMLResponseError(resp)
        return error_response


class ServerResponseError(XMLError):
    pass


class InternalServerError(TableauError):
    def __init__(self, server_response, request_url: Optional[str] = None):
        self.code = server_response.status_code
        self.content = server_response.content
        self.url = request_url or "server"

    def __str__(self):
        return f"\n\nInternal error {self.code} at {self.url}\n{self.content}"


class MissingRequiredFieldError(TableauError):
    pass


class NotSignedInError(TableauError):
    pass


class FailedSignInError(XMLError, NotSignedInError):
    def __str__(self):
        return f"{split_pascal_case(self.__class__.__name__)}: {super().__str__()}"


class ItemTypeNotAllowed(TableauError):
    pass


class NonXMLResponseError(TableauError):
    pass


class InvalidGraphQLQuery(TableauError):
    pass


class GraphQLError(TableauError):
    def __init__(self, error_payload):
        self.error = error_payload

    def __str__(self):
        from pprint import pformat

        return pformat(self.error)


class JobFailedException(TableauError):
    def __init__(self, job):
        self.notes = job.notes
        self.job = job

    def __str__(self):
        return f"Job {self.job.id} failed with notes {self.notes}"


class JobCancelledException(JobFailedException):
    pass


class FlowRunFailedException(TableauError):
    def __init__(self, flow_run):
        self.background_job_id = flow_run.background_job_id
        self.flow_run = flow_run

    def __str__(self):
        return f"FlowRun {self.flow_run.id} failed with job id {self.background_job_id}"


class FlowRunCancelledException(FlowRunFailedException):
    pass


class UnsupportedAttributeError(TableauError):
    pass
