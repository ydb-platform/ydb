from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class TileData(ClientValue):
    """Represents a Tile that describes a graphical link the user can click."""

    def __init__(
        self,
        background_collage_image_locations=None,
        background_image_location=None,
        background_image_renders_as_icon=None,
        body_text=None,
        description=None,
        hover_disabled=None,
        id_=None,
    ):
        self.BackgroundCollageImageLocations = StringCollection(
            background_collage_image_locations
        )
        self.BackgroundImageLocation = background_image_location
        self.BackgroundImageRendersAsIcon = background_image_renders_as_icon
        self.BodyText = body_text
        self.Description = description
        self.HoverDisabled = hover_disabled
        self.ID = id_
