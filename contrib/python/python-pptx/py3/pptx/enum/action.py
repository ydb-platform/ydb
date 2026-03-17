"""Enumerations that describe click-action settings."""

from __future__ import annotations

from pptx.enum.base import BaseEnum


class PP_ACTION_TYPE(BaseEnum):
    """
    Specifies the type of a mouse action (click or hover action).

    Alias: ``PP_ACTION``

    Example::

        from pptx.enum.action import PP_ACTION

        assert shape.click_action.action == PP_ACTION.HYPERLINK

    MS API name: `PpActionType`

    https://msdn.microsoft.com/EN-US/library/office/ff744895.aspx
    """

    END_SHOW = (6, "Slide show ends.")
    """Slide show ends."""

    FIRST_SLIDE = (3, "Returns to the first slide.")
    """Returns to the first slide."""

    HYPERLINK = (7, "Hyperlink.")
    """Hyperlink."""

    LAST_SLIDE = (4, "Moves to the last slide.")
    """Moves to the last slide."""

    LAST_SLIDE_VIEWED = (5, "Moves to the last slide viewed.")
    """Moves to the last slide viewed."""

    NAMED_SLIDE = (101, "Moves to slide specified by slide number.")
    """Moves to slide specified by slide number."""

    NAMED_SLIDE_SHOW = (10, "Runs the slideshow.")
    """Runs the slideshow."""

    NEXT_SLIDE = (1, "Moves to the next slide.")
    """Moves to the next slide."""

    NONE = (0, "No action is performed.")
    """No action is performed."""

    OPEN_FILE = (102, "Opens the specified file.")
    """Opens the specified file."""

    OLE_VERB = (11, "OLE Verb.")
    """OLE Verb."""

    PLAY = (12, "Begins the slideshow.")
    """Begins the slideshow."""

    PREVIOUS_SLIDE = (2, "Moves to the previous slide.")
    """Moves to the previous slide."""

    RUN_MACRO = (8, "Runs a macro.")
    """Runs a macro."""

    RUN_PROGRAM = (9, "Runs a program.")
    """Runs a program."""


PP_ACTION = PP_ACTION_TYPE
