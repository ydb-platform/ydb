"""lxml custom element classes for theme-related XML elements."""

from __future__ import annotations

from . import parse_from_template
from .xmlchemy import BaseOxmlElement


class CT_OfficeStyleSheet(BaseOxmlElement):
    """
    ``<a:theme>`` element, root of a theme part
    """

    _tag_seq = (
        "a:themeElements",
        "a:objectDefaults",
        "a:extraClrSchemeLst",
        "a:custClrLst",
        "a:extLst",
    )
    del _tag_seq

    @classmethod
    def new_default(cls):
        """
        Return a new ``<a:theme>`` element containing default settings
        suitable for use with a notes master.
        """
        return parse_from_template("theme")
