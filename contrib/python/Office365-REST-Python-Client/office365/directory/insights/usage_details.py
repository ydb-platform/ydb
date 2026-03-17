from typing import TYPE_CHECKING

from office365.runtime.client_value import ClientValue

if TYPE_CHECKING:
    from datetime import datetime
    from typing import Optional


class UsageDetails(ClientValue):
    """Complex type containing properties of Used items. Information on when the resource was last accessed (viewed)
    or modified (edited) by the user."""

    def __init__(self, last_accessed_datetime=None, last_modified_datetime=None):
        # type: (Optional[datetime], Optional[datetime]) -> None
        """
        :param last_accessed_datetime: The date and time the resource was last accessed by the user.
        :param last_modified_datetime: The date and time the resource was last modified by the user.
        """
        self.lastAccessedDateTime = last_accessed_datetime
        self.lastModifiedDateTime = last_modified_datetime
