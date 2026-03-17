"""This contains the IMAGE property from :rfc:`7986`."""

from __future__ import annotations

import base64
from typing import TYPE_CHECKING

from icalendar.prop.binary import vBinary
from icalendar.prop.uri import vUri

if TYPE_CHECKING:
    from icalendar.prop.text import vText


class Image:
    """An image as URI or BINARY according to :rfc:`7986`.

    Value Type:
        URI or BINARY -- no default.  The value MUST be data
        with a media type of "image" or refer to such data.

    Description:
        This property specifies an image for an iCalendar
        object or a calendar component via a URI or directly with inline
        data that can be used by calendar user agents when presenting the
        calendar data to a user.  Multiple properties MAY be used to
        specify alternative sets of images with, for example, varying
        media subtypes, resolutions, or sizes.  When multiple properties
        are present, calendar user agents SHOULD display only one of them,
        picking one that provides the most appropriate image quality, or
        display none.  The "DISPLAY" parameter is used to indicate the
        intended display mode for the image.  The "ALTREP" parameter,
        defined in :rfc:`5545`, can be used to provide a "clickable" image
        where the URI in the parameter value can be "launched" by a click
        on the image in the calendar user agent.

    Parameters:
        uri: The URI of the image.
        b64data: The data of the image, base64 encoded.
        fmttype: The format type, e.g. ``"image/png"``.
        altrep: Link target of the image.
        display: The display mode, e.g. ``"BADGE"``.

    """

    @classmethod
    def from_property_value(cls, value: vUri | vBinary | vText) -> Image:
        """Create an Image from a property value."""
        params: dict[str, str] = {}
        if not hasattr(value, "params"):
            raise TypeError("Value must be URI or BINARY.")
        try:
            value_type = value.params.get("VALUE", "").upper()
        except (AttributeError, TypeError) as err:
            raise TypeError("Value must have a valid params attribute.") from err
        if value_type == "URI" or isinstance(value, vUri):
            params["uri"] = str(value)
        elif isinstance(value, vBinary):
            params["b64data"] = value.obj
        elif value_type == "BINARY":
            params["b64data"] = str(value)
        else:
            raise TypeError(
                f"The VALUE parameter must be URI or BINARY, not {value_type!r}."
            )
        params.update(
            {
                "fmttype": value.params.get("FMTTYPE"),
                "altrep": value.params.get("ALTREP"),
                "display": value.params.get("DISPLAY"),
            }
        )
        return cls(**params)

    def __init__(
        self,
        uri: str | None = None,
        b64data: str | None = None,
        fmttype: str | None = None,
        altrep: str | None = None,
        display: str | None = None,
    ) -> None:
        """Create a new image according to :rfc:`7986`."""
        if uri is not None and b64data is not None:
            raise ValueError("Image cannot have both URI and binary data (RFC 7986)")
        if uri is None and b64data is None:
            raise ValueError("Image must have either URI or binary data")
        self.uri = uri
        self.b64data = b64data
        self.fmttype = fmttype
        self.altrep = altrep
        self.display = display

    @property
    def data(self) -> bytes | None:
        """Return the binary data, if available."""
        if self.b64data is None:
            return None
        return base64.b64decode(self.b64data)


__all__ = ["Image"]
