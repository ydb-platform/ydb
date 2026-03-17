"""lxml custom element classes for picture-related XML elements."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from xml.sax.saxutils import escape

from pptx.oxml import parse_xml
from pptx.oxml.ns import nsdecls
from pptx.oxml.shapes.shared import BaseShapeElement
from pptx.oxml.xmlchemy import BaseOxmlElement, OneAndOnlyOne

if TYPE_CHECKING:
    from pptx.oxml.shapes.shared import CT_ShapeProperties
    from pptx.util import Length


class CT_Picture(BaseShapeElement):
    """`p:pic` element.

    Represents a picture shape (an image placement on a slide).
    """

    nvPicPr = OneAndOnlyOne("p:nvPicPr")
    blipFill = OneAndOnlyOne("p:blipFill")
    spPr: CT_ShapeProperties = OneAndOnlyOne("p:spPr")  # pyright: ignore[reportAssignmentType]

    @property
    def blip_rId(self) -> str | None:
        """Value of `p:blipFill/a:blip/@r:embed`.

        Returns |None| if not present.
        """
        blip = self.blipFill.blip
        if blip is not None and blip.rEmbed is not None:
            return blip.rEmbed
        return None

    def crop_to_fit(self, image_size, view_size):
        """
        Set cropping values in `p:blipFill/a:srcRect` such that an image of
        *image_size* will stretch to exactly fit *view_size* when its aspect
        ratio is preserved.
        """
        self.blipFill.crop(self._fill_cropping(image_size, view_size))

    def get_or_add_ln(self):
        """
        Return the <a:ln> grandchild element, newly added if not present.
        """
        return self.spPr.get_or_add_ln()

    @property
    def ln(self):
        """
        ``<a:ln>`` grand-child element or |None| if not present
        """
        return self.spPr.ln

    @classmethod
    def new_ph_pic(cls, id_, name, desc, rId):
        """
        Return a new `p:pic` placeholder element populated with the supplied
        parameters.
        """
        return parse_xml(cls._pic_ph_tmpl() % (id_, name, desc, rId))

    @classmethod
    def new_pic(cls, shape_id, name, desc, rId, x, y, cx, cy):
        """Return new `<p:pic>` element tree configured with supplied parameters."""
        return parse_xml(cls._pic_tmpl() % (shape_id, name, escape(desc), rId, x, y, cx, cy))

    @classmethod
    def new_video_pic(
        cls,
        shape_id: int,
        shape_name: str,
        video_rId: str,
        media_rId: str,
        poster_frame_rId: str,
        x: Length,
        y: Length,
        cx: Length,
        cy: Length,
    ) -> CT_Picture:
        """Return a new `p:pic` populated with the specified video."""
        return cast(
            CT_Picture,
            parse_xml(
                cls._pic_video_tmpl()
                % (
                    shape_id,
                    shape_name,
                    video_rId,
                    media_rId,
                    poster_frame_rId,
                    x,
                    y,
                    cx,
                    cy,
                )
            ),
        )

    @property
    def srcRect_b(self):
        """Value of `p:blipFill/a:srcRect/@b` or 0.0 if not present."""
        return self._srcRect_x("b")

    @srcRect_b.setter
    def srcRect_b(self, value):
        self.blipFill.get_or_add_srcRect().b = value

    @property
    def srcRect_l(self):
        """Value of `p:blipFill/a:srcRect/@l` or 0.0 if not present."""
        return self._srcRect_x("l")

    @srcRect_l.setter
    def srcRect_l(self, value):
        self.blipFill.get_or_add_srcRect().l = value  # noqa

    @property
    def srcRect_r(self):
        """Value of `p:blipFill/a:srcRect/@r` or 0.0 if not present."""
        return self._srcRect_x("r")

    @srcRect_r.setter
    def srcRect_r(self, value):
        self.blipFill.get_or_add_srcRect().r = value

    @property
    def srcRect_t(self):
        """Value of `p:blipFill/a:srcRect/@t` or 0.0 if not present."""
        return self._srcRect_x("t")

    @srcRect_t.setter
    def srcRect_t(self, value):
        self.blipFill.get_or_add_srcRect().t = value

    def _fill_cropping(self, image_size, view_size):
        """
        Return a (left, top, right, bottom) 4-tuple containing the cropping
        values required to display an image of *image_size* in *view_size*
        when stretched proportionately. Each value is a percentage expressed
        as a fraction of 1.0, e.g. 0.425 represents 42.5%. *image_size* and
        *view_size* are each (width, height) pairs.
        """

        def aspect_ratio(width, height):
            return width / height

        ar_view = aspect_ratio(*view_size)
        ar_image = aspect_ratio(*image_size)

        if ar_view < ar_image:  # image too wide
            crop = (1.0 - (ar_view / ar_image)) / 2.0
            return (crop, 0.0, crop, 0.0)
        if ar_view > ar_image:  # image too tall
            crop = (1.0 - (ar_image / ar_view)) / 2.0
            return (0.0, crop, 0.0, crop)
        return (0.0, 0.0, 0.0, 0.0)

    @classmethod
    def _pic_ph_tmpl(cls):
        return (
            "<p:pic %s>\n"
            "  <p:nvPicPr>\n"
            '    <p:cNvPr id="%%d" name="%%s" descr="%%s"/>\n'
            "    <p:cNvPicPr>\n"
            '      <a:picLocks noGrp="1" noChangeAspect="1"/>\n'
            "    </p:cNvPicPr>\n"
            "    <p:nvPr/>\n"
            "  </p:nvPicPr>\n"
            "  <p:blipFill>\n"
            '    <a:blip r:embed="%%s"/>\n'
            "    <a:stretch>\n"
            "      <a:fillRect/>\n"
            "    </a:stretch>\n"
            "  </p:blipFill>\n"
            "  <p:spPr/>\n"
            "</p:pic>" % nsdecls("p", "a", "r")
        )

    @classmethod
    def _pic_tmpl(cls):
        return (
            "<p:pic %s>\n"
            "  <p:nvPicPr>\n"
            '    <p:cNvPr id="%%d" name="%%s" descr="%%s"/>\n'
            "    <p:cNvPicPr>\n"
            '      <a:picLocks noChangeAspect="1"/>\n'
            "    </p:cNvPicPr>\n"
            "    <p:nvPr/>\n"
            "  </p:nvPicPr>\n"
            "  <p:blipFill>\n"
            '    <a:blip r:embed="%%s"/>\n'
            "    <a:stretch>\n"
            "      <a:fillRect/>\n"
            "    </a:stretch>\n"
            "  </p:blipFill>\n"
            "  <p:spPr>\n"
            "    <a:xfrm>\n"
            '      <a:off x="%%d" y="%%d"/>\n'
            '      <a:ext cx="%%d" cy="%%d"/>\n'
            "    </a:xfrm>\n"
            '    <a:prstGeom prst="rect">\n'
            "      <a:avLst/>\n"
            "    </a:prstGeom>\n"
            "  </p:spPr>\n"
            "</p:pic>" % nsdecls("a", "p", "r")
        )

    @classmethod
    def _pic_video_tmpl(cls):
        return (
            "<p:pic %s>\n"
            "  <p:nvPicPr>\n"
            '    <p:cNvPr id="%%d" name="%%s">\n'
            '      <a:hlinkClick r:id="" action="ppaction://media"/>\n'
            "    </p:cNvPr>\n"
            "    <p:cNvPicPr>\n"
            '      <a:picLocks noChangeAspect="1"/>\n'
            "    </p:cNvPicPr>\n"
            "    <p:nvPr>\n"
            '      <a:videoFile r:link="%%s"/>\n'
            "      <p:extLst>\n"
            '        <p:ext uri="{DAA4B4D4-6D71-4841-9C94-3DE7FCFB9230}">\n'
            '          <p14:media xmlns:p14="http://schemas.microsoft.com/of'
            'fice/powerpoint/2010/main" r:embed="%%s"/>\n'
            "        </p:ext>\n"
            "      </p:extLst>\n"
            "    </p:nvPr>\n"
            "  </p:nvPicPr>\n"
            "  <p:blipFill>\n"
            '    <a:blip r:embed="%%s"/>\n'
            "    <a:stretch>\n"
            "      <a:fillRect/>\n"
            "    </a:stretch>\n"
            "  </p:blipFill>\n"
            "  <p:spPr>\n"
            "    <a:xfrm>\n"
            '      <a:off x="%%d" y="%%d"/>\n'
            '      <a:ext cx="%%d" cy="%%d"/>\n'
            "    </a:xfrm>\n"
            '    <a:prstGeom prst="rect">\n'
            "      <a:avLst/>\n"
            "    </a:prstGeom>\n"
            "  </p:spPr>\n"
            "</p:pic>" % nsdecls("a", "p", "r")
        )

    def _srcRect_x(self, attr_name):
        """
        Value of `p:blipFill/a:srcRect/@{attr_name}` or 0.0 if not present.
        """
        srcRect = self.blipFill.srcRect
        if srcRect is None:
            return 0.0
        return getattr(srcRect, attr_name)


class CT_PictureNonVisual(BaseOxmlElement):
    """
    ``<p:nvPicPr>`` element, containing non-visual properties for a picture
    shape.
    """

    cNvPr = OneAndOnlyOne("p:cNvPr")
    nvPr = OneAndOnlyOne("p:nvPr")
