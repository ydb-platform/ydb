from typing import Optional

from office365.sharepoint.entity import Entity


class UserCustomAction(Entity):
    """Represents a custom action associated with a SharePoint list, Web site, or subsite."""

    @property
    def client_side_component_id(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("ClientSideComponentId", None)

    @property
    def script_block(self):
        # type: () -> Optional[str]
        """Gets the value that specifies the ECMAScript to be executed when the custom action is performed."""
        return self.properties.get("ScriptBlock", None)

    @script_block.setter
    def script_block(self, value):
        # type: (str) -> None
        """
        Sets the value that specifies the ECMAScript to be executed when the custom action is performed.
        """
        self.set_property("ScriptBlock", value)

    @property
    def script_src(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the URI of a file which contains the ECMAScript to execute on the page"""
        return self.properties.get("ScriptSrc", None)

    @script_src.setter
    def script_src(self, value):
        # type: (str) -> None
        """
        Sets a value that specifies the URI of a file which contains the ECMAScript to execute on the page
        """
        self.set_property("ScriptSrc", value)

    @property
    def url(self):
        # type: () -> Optional[str]
        """Gets or sets the URL, URI, or ECMAScript (JScript, JavaScript) function associated with the action."""
        return self.properties.get("Url", None)
