from office365.directory.protection.threatassessment.requests.request import (
    ThreatAssessmentRequest,
)


class MailAssessmentRequest(ThreatAssessmentRequest):
    """
    Used to create and retrieve a mail threat assessment, derived from threatAssessmentRequest.

    When you create a mail threat assessment request, the mail should be received by the user specified
    in recipientEmail. Delegated Mail permissions (Mail.Read or Mail.Read.Shared) are requried to access the mail
    received by the user or shared by someone else.
    """
