from typing import Optional

from office365.sharepoint.changes.change import Change


class ChangeWeb(Change):
    """Specifies a change on a site"""

    def __repr__(self):
        return "Web: {0}, Action: {1}".format(self.web_id, self.change_type_name)

    @property
    def web_id(self):
        # type: () -> Optional[str]
        """
        Identifies the site that has changed
        """
        return self.properties.get("WebId", None)
