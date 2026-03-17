# encoding: utf-8

"""
Enumerations that describe click action settings
"""

from __future__ import absolute_import

from .base import alias, Enumeration, EnumMember


@alias("PP_ACTION")
class PP_ACTION_TYPE(Enumeration):
    """
    Specifies the type of a mouse action (click or hover action).

    Alias: ``PP_ACTION``

    Example::

        from pptx.enum.action import PP_ACTION

        assert shape.click_action.action == PP_ACTION.HYPERLINK
    """

    __ms_name__ = "PpActionType"

    __url__ = "https://msdn.microsoft.com/EN-US/library/office/ff744895.aspx"

    __members__ = (
        EnumMember("END_SHOW", 6, "Slide show ends."),
        EnumMember("FIRST_SLIDE", 3, "Returns to the first slide."),
        EnumMember("HYPERLINK", 7, "Hyperlink."),
        EnumMember("LAST_SLIDE", 4, "Moves to the last slide."),
        EnumMember("LAST_SLIDE_VIEWED", 5, "Moves to the last slide viewed."),
        EnumMember("NAMED_SLIDE", 101, "Moves to slide specified by slide number."),
        EnumMember("NAMED_SLIDE_SHOW", 10, "Runs the slideshow."),
        EnumMember("NEXT_SLIDE", 1, "Moves to the next slide."),
        EnumMember("NONE", 0, "No action is performed."),
        EnumMember("OPEN_FILE", 102, "Opens the specified file."),
        EnumMember("OLE_VERB", 11, "OLE Verb."),
        EnumMember("PLAY", 12, "Begins the slideshow."),
        EnumMember("PREVIOUS_SLIDE", 2, "Moves to the previous slide."),
        EnumMember("RUN_MACRO", 8, "Runs a macro."),
        EnumMember("RUN_PROGRAM", 9, "Runs a program."),
    )
