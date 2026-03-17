from typing import Optional

from office365.directory.protection.threatassessment.requests.request import (
    ThreatAssessmentRequest,
)


class EmailFileAssessmentRequest(ThreatAssessmentRequest):
    """
    Represents a resource that creates and retrieves an email file threat assessment.
    The email file can be an .eml file type.
    """

    @property
    def content_data(self):
        # type: () -> Optional[str]
        """
        Base64 encoded .eml email file content. The file content can't fetch back because it isn't stored.
        """
        return self.properties.get("contentData", None)

    @property
    def file_name(self):
        # type: () -> Optional[str]
        """
        Base64 encoded .eml email file content. The file content can't fetch back because it isn't stored.
        """
        return self.properties.get("fileName", None)
