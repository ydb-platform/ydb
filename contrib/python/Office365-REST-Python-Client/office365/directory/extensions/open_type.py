from typing import Optional

from office365.directory.extensions.extension import Extension


class OpenTypeExtension(Extension):
    """
    Represents open extensions (also known as open type extensions, and formerly known as Office 365 data extensions),
    an extensibility option that provides an easy way to directly add untyped properties to a resource
    in Microsoft Graph.
    """

    @property
    def extension_name(self):
        # type: () -> Optional[str]
        """A unique text identifier for an open type data extension."""
        return self.properties.get("extensionName", None)
