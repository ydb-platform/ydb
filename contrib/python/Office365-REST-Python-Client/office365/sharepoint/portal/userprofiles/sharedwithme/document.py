from datetime import datetime
from typing import Optional

from office365.runtime.client_value_collection import ClientValueCollection
from office365.sharepoint.entity import Entity
from office365.sharepoint.portal.userprofiles.sharedwithme.document_user import (
    SharedWithMeDocumentUser,
)


class SharedWithMeDocument(Entity):
    """Represents a shared document."""

    @property
    def authors(self):
        # type: () -> ClientValueCollection[SharedWithMeDocumentUser]
        """Specifies a list of users that authored the document."""
        return self.properties.get(
            "Authors", ClientValueCollection(SharedWithMeDocumentUser)
        )

    @property
    def caller_stack(self):
        # type: () -> Optional[str]
        """ """
        return self.properties.get("CallerStack", None)

    @property
    def content_type_id(self):
        # type: () -> Optional[str]
        """Specifies the identifier of the content type of the document."""
        return self.properties.get("ContentTypeId", None)

    @property
    def doc_id(self):
        # type: () -> Optional[str]
        """Specifies the document identifier."""
        return self.properties.get("DocId", None)

    @property
    def editors(self):
        """Specifies a list of users that can edit the document."""
        return self.properties.get(
            "Editors", ClientValueCollection(SharedWithMeDocumentUser)
        )

    @property
    def modified(self):
        """Specifies the date and time when the document was last modified."""
        return self.properties.get("Modified", datetime.min)

    @property
    def file_leaf_ref(self):
        # type: () -> Optional[str]
        """Specifies the name of the document."""
        return self.properties.get("FileLeafRef", None)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.UserProfiles.SharedWithMeDocument"
