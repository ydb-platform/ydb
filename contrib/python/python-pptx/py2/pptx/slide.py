# encoding: utf-8

"""Slide-related objects, including masters, layouts, and notes."""

from pptx.dml.fill import FillFormat
from pptx.enum.shapes import PP_PLACEHOLDER
from pptx.shapes.shapetree import (
    LayoutPlaceholders,
    LayoutShapes,
    MasterPlaceholders,
    MasterShapes,
    NotesSlidePlaceholders,
    NotesSlideShapes,
    SlidePlaceholders,
    SlideShapes,
)
from pptx.shared import ElementProxy, ParentedElementProxy, PartElementProxy
from pptx.util import lazyproperty


class _BaseSlide(PartElementProxy):
    """Base class for slide objects, including masters, layouts and notes."""

    @lazyproperty
    def background(self):
        """|_Background| object providing slide background properties.

        This property returns a |_Background| object whether or not the
        slide, master, or layout has an explicitly defined background.

        The same |_Background| object is returned on every call for the same
        slide object.
        """
        return _Background(self._element.cSld)

    @property
    def name(self):
        """
        String representing the internal name of this slide. Returns an empty
        string (`''`) if no name is assigned. Assigning an empty string or
        |None| to this property causes any name to be removed.
        """
        return self._element.cSld.name

    @name.setter
    def name(self, value):
        new_value = "" if value is None else value
        self._element.cSld.name = new_value


class _BaseMaster(_BaseSlide):
    """
    Base class for master objects such as |SlideMaster| and |NotesMaster|.
    Provides access to placeholders and regular shapes.
    """

    @lazyproperty
    def placeholders(self):
        """
        Instance of |MasterPlaceholders| containing sequence of placeholder
        shapes in this master, sorted in *idx* order.
        """
        return MasterPlaceholders(self._element.spTree, self)

    @lazyproperty
    def shapes(self):
        """
        Instance of |MasterShapes| containing sequence of shape objects
        appearing on this slide.
        """
        return MasterShapes(self._element.spTree, self)


class NotesMaster(_BaseMaster):
    """
    Proxy for the notes master XML document. Provides access to shapes, the
    most commonly used of which are placeholders.
    """


class NotesSlide(_BaseSlide):
    """Notes slide object.

    Provides access to slide notes placeholder and other shapes on the notes handout
    page.
    """

    def clone_master_placeholders(self, notes_master):
        """Selectively add placeholder shape elements from *notes_master*.

        Selected placeholder shape elements from *notes_master* are added to the shapes
        collection of this notes slide. Z-order of placeholders is preserved. Certain
        placeholders (header, date, footer) are not cloned.
        """

        def iter_cloneable_placeholders(notes_master):
            """
            Generate a reference to each placeholder in *notes_master* that
            should be cloned to a notes slide when the a new notes slide is
            created.
            """
            cloneable = (
                PP_PLACEHOLDER.SLIDE_IMAGE,
                PP_PLACEHOLDER.BODY,
                PP_PLACEHOLDER.SLIDE_NUMBER,
            )
            for placeholder in notes_master.placeholders:
                if placeholder.element.ph_type in cloneable:
                    yield placeholder

        shapes = self.shapes
        for placeholder in iter_cloneable_placeholders(notes_master):
            shapes.clone_placeholder(placeholder)

    @property
    def notes_placeholder(self):
        """
        Return the notes placeholder on this notes slide, the shape that
        contains the actual notes text. Return |None| if no notes placeholder
        is present; while this is probably uncommon, it can happen if the
        notes master does not have a body placeholder, or if the notes
        placeholder has been deleted from the notes slide.
        """
        for placeholder in self.placeholders:
            if placeholder.placeholder_format.type == PP_PLACEHOLDER.BODY:
                return placeholder
        return None

    @property
    def notes_text_frame(self):
        """
        Return the text frame of the notes placeholder on this notes slide,
        or |None| if there is no notes placeholder. This is a shortcut to
        accommodate the common case of simply adding "notes" text to the
        notes "page".
        """
        notes_placeholder = self.notes_placeholder
        if notes_placeholder is None:
            return None
        return notes_placeholder.text_frame

    @lazyproperty
    def placeholders(self):
        """
        An instance of |NotesSlidePlaceholders| containing the sequence of
        placeholder shapes in this notes slide.
        """
        return NotesSlidePlaceholders(self.element.spTree, self)

    @lazyproperty
    def shapes(self):
        """
        An instance of |NotesSlideShapes| containing the sequence of shape
        objects appearing on this notes slide.
        """
        return NotesSlideShapes(self._element.spTree, self)


class Slide(_BaseSlide):
    """Slide object. Provides access to shapes and slide-level properties."""

    @property
    def background(self):
        """|_Background| object providing slide background properties.

        This property returns a |_Background| object whether or not the slide
        overrides the default background or inherits it. Determining which of
        those conditions applies for this slide is accomplished using the
        :attr:`follow_master_background` property.

        The same |_Background| object is returned on every call for the same
        slide object.
        """
        return super(Slide, self).background

    @property
    def follow_master_background(self):
        """|True| if this slide inherits the slide master background.

        Assigning |False| causes background inheritance from the master to be
        interrupted; if there is no custom background for this slide,
        a default background is added. If a custom background already exists
        for this slide, assigning |False| has no effect.

        Assigning |True| causes any custom background for this slide to be
        deleted and inheritance from the master restored.
        """
        return self._element.bg is None

    @property
    def has_notes_slide(self):
        """
        Return True if this slide has a notes slide, False otherwise. A notes
        slide is created by :attr:`.notes_slide` when one doesn't exist; use
        this property to test for a notes slide without the possible side
        effect of creating one.
        """
        return self.part.has_notes_slide

    @property
    def notes_slide(self):
        """
        Return the |NotesSlide| instance for this slide. If the slide does
        not have a notes slide, one is created. The same single instance is
        returned on each call.
        """
        return self.part.notes_slide

    @lazyproperty
    def placeholders(self):
        """
        Instance of |SlidePlaceholders| containing sequence of placeholder
        shapes in this slide.
        """
        return SlidePlaceholders(self._element.spTree, self)

    @lazyproperty
    def shapes(self):
        """
        Instance of |SlideShapes| containing sequence of shape objects
        appearing on this slide.
        """
        return SlideShapes(self._element.spTree, self)

    @property
    def slide_id(self):
        """
        The integer value that uniquely identifies this slide within this
        presentation. The slide id does not change if the position of this
        slide in the slide sequence is changed by adding, rearranging, or
        deleting slides.
        """
        return self.part.slide_id

    @property
    def slide_layout(self):
        """
        |SlideLayout| object this slide inherits appearance from.
        """
        return self.part.slide_layout


class Slides(ParentedElementProxy):
    """
    Sequence of slides belonging to an instance of |Presentation|, having
    list semantics for access to individual slides. Supports indexed access,
    len(), and iteration.
    """

    def __init__(self, sldIdLst, prs):
        super(Slides, self).__init__(sldIdLst, prs)
        self._sldIdLst = sldIdLst

    def __getitem__(self, idx):
        """
        Provide indexed access, (e.g. 'slides[0]').
        """
        try:
            sldId = self._sldIdLst[idx]
        except IndexError:
            raise IndexError("slide index out of range")
        return self.part.related_slide(sldId.rId)

    def __iter__(self):
        """
        Support iteration (e.g. 'for slide in slides:').
        """
        for sldId in self._sldIdLst:
            yield self.part.related_slide(sldId.rId)

    def __len__(self):
        """
        Support len() built-in function (e.g. 'len(slides) == 4').
        """
        return len(self._sldIdLst)

    def add_slide(self, slide_layout):
        """
        Return a newly added slide that inherits layout from *slide_layout*.
        """
        rId, slide = self.part.add_slide(slide_layout)
        slide.shapes.clone_layout_placeholders(slide_layout)
        self._sldIdLst.add_sldId(rId)
        return slide

    def get(self, slide_id, default=None):
        """
        Return the slide identified by integer *slide_id* in this
        presentation, or *default* if not found.
        """
        slide = self.part.get_slide(slide_id)
        if slide is None:
            return default
        return slide

    def index(self, slide):
        """
        Map *slide* to an integer representing its zero-based position in
        this slide collection. Raises |ValueError| on *slide* not present.
        """
        for idx, this_slide in enumerate(self):
            if this_slide == slide:
                return idx
        raise ValueError("%s is not in slide collection" % slide)


class SlideLayout(_BaseSlide):
    """
    Slide layout object. Provides access to placeholders, regular shapes, and
    slide layout-level properties.
    """

    def iter_cloneable_placeholders(self):
        """
        Generate a reference to each layout placeholder on this slide layout
        that should be cloned to a slide when the layout is applied to that
        slide.
        """
        latent_ph_types = (
            PP_PLACEHOLDER.DATE,
            PP_PLACEHOLDER.FOOTER,
            PP_PLACEHOLDER.SLIDE_NUMBER,
        )
        for ph in self.placeholders:
            if ph.element.ph_type not in latent_ph_types:
                yield ph

    @lazyproperty
    def placeholders(self):
        """
        Instance of |LayoutPlaceholders| containing sequence of placeholder
        shapes in this slide layout, sorted in *idx* order.
        """
        return LayoutPlaceholders(self._element.spTree, self)

    @lazyproperty
    def shapes(self):
        """
        Instance of |LayoutShapes| containing the sequence of shapes
        appearing on this slide layout.
        """
        return LayoutShapes(self._element.spTree, self)

    @property
    def slide_master(self):
        """
        Slide master from which this slide layout inherits properties.
        """
        return self.part.slide_master

    @property
    def used_by_slides(self):
        """Tuple of slide objects based on this slide layout."""
        # ---getting Slides collection requires going around the horn a bit---
        slides = self.part.package.presentation_part.presentation.slides
        return tuple(s for s in slides if s.slide_layout == self)


class SlideLayouts(ParentedElementProxy):
    """Sequence of slide layouts belonging to a slide-master.

    Supports indexed access, len(), iteration, index() and remove().
    """

    def __init__(self, sldLayoutIdLst, parent):
        super(SlideLayouts, self).__init__(sldLayoutIdLst, parent)
        self._sldLayoutIdLst = sldLayoutIdLst

    def __getitem__(self, idx):
        """
        Provide indexed access, (e.g. ``slide_layouts[2]``).
        """
        try:
            sldLayoutId = self._sldLayoutIdLst[idx]
        except IndexError:
            raise IndexError("slide layout index out of range")
        return self.part.related_slide_layout(sldLayoutId.rId)

    def __iter__(self):
        """
        Generate a reference to each of the |SlideLayout| instances in the
        collection, in sequence.
        """
        for sldLayoutId in self._sldLayoutIdLst:
            yield self.part.related_slide_layout(sldLayoutId.rId)

    def __len__(self):
        """
        Support len() built-in function (e.g. 'len(slides) == 4').
        """
        return len(self._sldLayoutIdLst)

    def get_by_name(self, name, default=None):
        """Return SlideLayout object having *name* or *default* if not found."""
        for slide_layout in self:
            if slide_layout.name == name:
                return slide_layout
        return default

    def index(self, slide_layout):
        """Return zero-based index of *slide_layout* in this collection.

        Raises ValueError if *slide_layout* is not present in this collection.
        """
        for idx, this_layout in enumerate(self):
            if slide_layout == this_layout:
                return idx
        raise ValueError("layout not in this SlideLayouts collection")

    def remove(self, slide_layout):
        """Remove *slide_layout* from the collection.

        Raises ValueError when *slide_layout* is in use; a slide layout which is the
        basis for one or more slides cannot be removed.
        """
        # ---raise if layout is in use---
        if slide_layout.used_by_slides:
            raise ValueError("cannot remove slide-layout in use by one or more slides")

        # ---target layout is identified by its index in this collection---
        target_idx = self.index(slide_layout)

        # --remove layout from p:sldLayoutIds of its master
        # --this stops layout from showing up, but doesn't remove it from package
        target_sldLayoutId = self._sldLayoutIdLst.sldLayoutId_lst[target_idx]
        self._sldLayoutIdLst.remove(target_sldLayoutId)

        # --drop relationship from master to layout
        # --this removes layout from package, along with everything (only) it refers to,
        # --including images (not used elsewhere) and hyperlinks
        slide_layout.slide_master.part.drop_rel(target_sldLayoutId.rId)


class SlideMaster(_BaseMaster):
    """
    Slide master object. Provides access to slide layouts. Access to
    placeholders, regular shapes, and slide master-level properties is
    inherited from |_BaseMaster|.
    """

    @lazyproperty
    def slide_layouts(self):
        """|SlideLayouts| object providing access to this slide-master's layouts."""
        return SlideLayouts(self._element.get_or_add_sldLayoutIdLst(), self)


class SlideMasters(ParentedElementProxy):
    """Sequence of |SlideMaster| objects belonging to a presentation.

    Has list access semantics, supporting indexed access, len(), and iteration.
    """

    def __init__(self, sldMasterIdLst, parent):
        super(SlideMasters, self).__init__(sldMasterIdLst, parent)
        self._sldMasterIdLst = sldMasterIdLst

    def __getitem__(self, idx):
        """
        Provide indexed access, (e.g. ``slide_masters[2]``).
        """
        try:
            sldMasterId = self._sldMasterIdLst[idx]
        except IndexError:
            raise IndexError("slide master index out of range")
        return self.part.related_slide_master(sldMasterId.rId)

    def __iter__(self):
        """
        Generate a reference to each of the |SlideMaster| instances in the
        collection, in sequence.
        """
        for smi in self._sldMasterIdLst:
            yield self.part.related_slide_master(smi.rId)

    def __len__(self):
        """
        Support len() built-in function (e.g. 'len(slide_masters) == 4').
        """
        return len(self._sldMasterIdLst)


class _Background(ElementProxy):
    """Provides access to slide background properties.

    Note that the presence of this object does not by itself imply an
    explicitly-defined background; a slide with an inherited background still
    has a |_Background| object.
    """

    def __init__(self, cSld):
        super(_Background, self).__init__(cSld)
        self._cSld = cSld

    @lazyproperty
    def fill(self):
        """|FillFormat| instance for this background.

        This |FillFormat| object is used to interrogate or specify the fill
        of the slide background.

        Note that accessing this property is potentially destructive. A slide
        background can also be specified by a background style reference and
        accessing this property will remove that reference, if present, and
        replace it with NoFill. This is frequently the case for a slide
        master background.

        This is also the case when there is no explicitly defined background
        (background is inherited); merely accessing this property will cause
        the background to be set to NoFill and the inheritance link will be
        interrupted. This is frequently the case for a slide background.

        Of course, if you are accessing this property in order to set the
        fill, then these changes are of no consequence, but the existing
        background cannot be reliably interrogated using this property unless
        you have already established it is an explicit fill.

        If the background is already a fill, then accessing this property
        makes no changes to the current background.
        """
        bgPr = self._cSld.get_or_add_bgPr()
        return FillFormat.from_fill_parent(bgPr)
