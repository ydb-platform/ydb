from typing import Optional

from office365.sharepoint.entity import Entity


class EffectiveInformationRightsManagementSettings(Entity):
    """A collection of effective IRM settings on the file."""

    @property
    def allow_print(self):
        # type: () -> Optional[bool]
        """Specifies whether a user can print the downloaded document."""
        return self.properties.get("AllowPrint", None)

    @property
    def template_id(self):
        # type: () -> Optional[str]
        """Gets the template ID of the RMS template that will be applied to the file/library."""
        return self.properties.get("TemplateId", None)
