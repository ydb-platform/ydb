"""Shapes based on the `p:pic` element, including Picture and Movie."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pptx.dml.line import LineFormat
from pptx.enum.shapes import MSO_SHAPE, MSO_SHAPE_TYPE, PP_MEDIA_TYPE
from pptx.shapes.base import BaseShape
from pptx.shared import ParentedElementProxy
from pptx.util import lazyproperty

if TYPE_CHECKING:
    from pptx.oxml.shapes.picture import CT_Picture
    from pptx.oxml.shapes.shared import CT_LineProperties
    from pptx.types import ProvidesPart


class _BasePicture(BaseShape):
    """Base class for shapes based on a `p:pic` element."""

    def __init__(self, pic: CT_Picture, parent: ProvidesPart):
        super(_BasePicture, self).__init__(pic, parent)
        self._pic = pic

    @property
    def crop_bottom(self) -> float:
        """|float| representing relative portion cropped from shape bottom.

        Read/write. 1.0 represents 100%. For example, 25% is represented by 0.25. Negative values
        are valid as are values greater than 1.0.
        """
        return self._pic.srcRect_b

    @crop_bottom.setter
    def crop_bottom(self, value: float):
        self._pic.srcRect_b = value

    @property
    def crop_left(self) -> float:
        """|float| representing relative portion cropped from left of shape.

        Read/write. 1.0 represents 100%. A negative value extends the side beyond the image
        boundary.
        """
        return self._pic.srcRect_l

    @crop_left.setter
    def crop_left(self, value: float):
        self._pic.srcRect_l = value

    @property
    def crop_right(self) -> float:
        """|float| representing relative portion cropped from right of shape.

        Read/write. 1.0 represents 100%.
        """
        return self._pic.srcRect_r

    @crop_right.setter
    def crop_right(self, value: float):
        self._pic.srcRect_r = value

    @property
    def crop_top(self) -> float:
        """|float| representing relative portion cropped from shape top.

        Read/write. 1.0 represents 100%.
        """
        return self._pic.srcRect_t

    @crop_top.setter
    def crop_top(self, value: float):
        self._pic.srcRect_t = value

    def get_or_add_ln(self):
        """Return the `a:ln` element for this `p:pic`-based image.

        The `a:ln` element contains the line format properties XML.
        """
        return self._pic.get_or_add_ln()

    @lazyproperty
    def line(self) -> LineFormat:
        """Provides access to properties of the picture outline, such as its color and width."""
        return LineFormat(self)

    @property
    def ln(self) -> CT_LineProperties | None:
        """The `a:ln` element for this `p:pic`.

        Contains the line format properties such as line color and width. |None| if no `a:ln`
        element is present.
        """
        return self._pic.ln


class Movie(_BasePicture):
    """A movie shape, one that places a video on a slide.

    Like |Picture|, a movie shape is based on the `p:pic` element. A movie is composed of a video
    and a *poster frame*, the placeholder image that represents the video before it is played.
    """

    @lazyproperty
    def media_format(self) -> _MediaFormat:
        """The |_MediaFormat| object for this movie.

        The |_MediaFormat| object provides access to formatting properties for the movie.
        """
        return _MediaFormat(self._pic, self)

    @property
    def media_type(self) -> PP_MEDIA_TYPE:
        """Member of :ref:`PpMediaType` describing this shape.

        The return value is unconditionally `PP_MEDIA_TYPE.MOVIE` in this case.
        """
        return PP_MEDIA_TYPE.MOVIE

    @property
    def poster_frame(self):
        """Return |Image| object containing poster frame for this movie.

        Returns |None| if this movie has no poster frame (uncommon).
        """
        slide_part, rId = self.part, self._pic.blip_rId
        if rId is None:
            return None
        return slide_part.get_image(rId)

    @property
    def shape_type(self) -> MSO_SHAPE_TYPE:
        """Return member of :ref:`MsoShapeType` describing this shape.

        The return value is unconditionally `MSO_SHAPE_TYPE.MEDIA` in this
        case.
        """
        return MSO_SHAPE_TYPE.MEDIA


class Picture(_BasePicture):
    """A picture shape, one that places an image on a slide.

    Based on the `p:pic` element.
    """

    @property
    def auto_shape_type(self) -> MSO_SHAPE | None:
        """Member of MSO_SHAPE indicating masking shape.

        A picture can be masked by any of the so-called "auto-shapes" available in PowerPoint,
        such as an ellipse or triangle. When a picture is masked by a shape, the shape assumes the
        same dimensions as the picture and the portion of the picture outside the shape boundaries
        does not appear. Note the default value for a newly-inserted picture is
        `MSO_AUTO_SHAPE_TYPE.RECTANGLE`, which performs no cropping because the extents of the
        rectangle exactly correspond to the extents of the picture.

        The available shapes correspond to the members of :ref:`MsoAutoShapeType`.

        The return value can also be |None|, indicating the picture either has no geometry (not
        expected) or has custom geometry, like a freeform shape. A picture with no geometry will
        have no visible representation on the slide, although it can be selected. This is because
        without geometry, there is no "inside-the-shape" for it to appear in.
        """
        prstGeom = self._pic.spPr.prstGeom
        if prstGeom is None:  # ---generally means cropped with freeform---
            return None
        return prstGeom.prst

    @auto_shape_type.setter
    def auto_shape_type(self, member: MSO_SHAPE):
        MSO_SHAPE.validate(member)
        spPr = self._pic.spPr
        prstGeom = spPr.prstGeom
        if prstGeom is None:
            spPr._remove_custGeom()  # pyright: ignore[reportPrivateUsage]
            prstGeom = spPr._add_prstGeom()  # pyright: ignore[reportPrivateUsage]
        prstGeom.prst = member

    @property
    def image(self):
        """The |Image| object for this picture.

        Provides access to the properties and bytes of the image in this picture shape.
        """
        slide_part, rId = self.part, self._pic.blip_rId
        if rId is None:
            raise ValueError("no embedded image")
        return slide_part.get_image(rId)

    @property
    def shape_type(self) -> MSO_SHAPE_TYPE:
        """Unconditionally `MSO_SHAPE_TYPE.PICTURE` in this case."""
        return MSO_SHAPE_TYPE.PICTURE


class _MediaFormat(ParentedElementProxy):
    """Provides access to formatting properties for a Media object.

    Media format properties are things like start point, volume, and
    compression type.
    """
