from office365.directory.protection.threatassessment.requests.request import (
    ThreatAssessmentRequest,
)


class FileAssessmentRequest(ThreatAssessmentRequest):
    """Used to create and retrieve a file threat assessment, derived from threatAssessmentRequest.

    The file can be a text file or Word document or binary file received in an email attachment.
    """
