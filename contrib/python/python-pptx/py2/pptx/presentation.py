# encoding: utf-8

"""Main presentation object."""

from pptx.shared import PartElementProxy
from pptx.slide import SlideMasters, Slides
from pptx.util import lazyproperty


class Presentation(PartElementProxy):
    """PresentationML (PML) presentation.

    Not intended to be constructed directly. Use :func:`pptx.Presentation` to open or
    create a presentation.
    """

    @property
    def core_properties(self):
        """
        Instance of |CoreProperties| holding the read/write Dublin Core
        document properties for this presentation.
        """
        return self.part.core_properties

    @property
    def notes_master(self):
        """
        Instance of |NotesMaster| for this presentation. If the presentation
        does not have a notes master, one is created from a default template
        and returned. The same single instance is returned on each call.
        """
        return self.part.notes_master

    def save(self, file):
        """
        Save this presentation to *file*, where *file* can be either a path
        to a file (a string) or a file-like object.
        """
        self.part.save(file)

    @property
    def slide_height(self):
        """
        Height of slides in this presentation, in English Metric Units (EMU).
        Returns |None| if no slide width is defined. Read/write.
        """
        sldSz = self._element.sldSz
        if sldSz is None:
            return None
        return sldSz.cy

    @slide_height.setter
    def slide_height(self, height):
        sldSz = self._element.get_or_add_sldSz()
        sldSz.cy = height

    @property
    def slide_layouts(self):
        """
        Sequence of |SlideLayout| instances belonging to the first
        |SlideMaster| of this presentation. A presentation can have more than
        one slide master and each master will have its own set of layouts.
        This property is a convenience for the common case where the
        presentation has only a single slide master.
        """
        return self.slide_masters[0].slide_layouts

    @property
    def slide_master(self):
        """
        First |SlideMaster| object belonging to this presentation. Typically,
        presentations have only a single slide master. This property provides
        simpler access in that common case.
        """
        return self.slide_masters[0]

    @lazyproperty
    def slide_masters(self):
        """
        Sequence of |SlideMaster| objects belonging to this presentation
        """
        return SlideMasters(self._element.get_or_add_sldMasterIdLst(), self)

    @property
    def slide_width(self):
        """
        Width of slides in this presentation, in English Metric Units (EMU).
        Returns |None| if no slide width is defined. Read/write.
        """
        sldSz = self._element.sldSz
        if sldSz is None:
            return None
        return sldSz.cx

    @slide_width.setter
    def slide_width(self, width):
        sldSz = self._element.get_or_add_sldSz()
        sldSz.cx = width

    @lazyproperty
    def slides(self):
        """
        |Slides| object containing the slides in this presentation.
        """
        sldIdLst = self._element.get_or_add_sldIdLst()
        self.part.rename_slide_parts([sldId.rId for sldId in sldIdLst])
        return Slides(sldIdLst, self)
