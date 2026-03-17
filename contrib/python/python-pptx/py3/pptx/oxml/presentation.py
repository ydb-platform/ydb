"""Custom element classes for presentation-related XML elements."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, cast

from pptx.oxml.simpletypes import ST_SlideId, ST_SlideSizeCoordinate, XsdString
from pptx.oxml.xmlchemy import BaseOxmlElement, RequiredAttribute, ZeroOrMore, ZeroOrOne

if TYPE_CHECKING:
    from pptx.util import Length


class CT_Presentation(BaseOxmlElement):
    """`p:presentation` element, root of the Presentation part stored as `/ppt/presentation.xml`."""

    get_or_add_sldSz: Callable[[], CT_SlideSize]
    get_or_add_sldIdLst: Callable[[], CT_SlideIdList]
    get_or_add_sldMasterIdLst: Callable[[], CT_SlideMasterIdList]

    sldMasterIdLst: CT_SlideMasterIdList | None = (
        ZeroOrOne(  # pyright: ignore[reportAssignmentType]
            "p:sldMasterIdLst",
            successors=(
                "p:notesMasterIdLst",
                "p:handoutMasterIdLst",
                "p:sldIdLst",
                "p:sldSz",
                "p:notesSz",
            ),
        )
    )
    sldIdLst: CT_SlideIdList | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "p:sldIdLst", successors=("p:sldSz", "p:notesSz")
    )
    sldSz: CT_SlideSize | None = ZeroOrOne(  # pyright: ignore[reportAssignmentType]
        "p:sldSz", successors=("p:notesSz",)
    )


class CT_SlideId(BaseOxmlElement):
    """`p:sldId` element.

    Direct child of `p:sldIdLst` that contains an `rId` reference to a slide in the presentation.
    """

    id: int = RequiredAttribute("id", ST_SlideId)  # pyright: ignore[reportAssignmentType]
    rId: str = RequiredAttribute("r:id", XsdString)  # pyright: ignore[reportAssignmentType]


class CT_SlideIdList(BaseOxmlElement):
    """`p:sldIdLst` element.

    Direct child of <p:presentation> that contains a list of the slide parts in the presentation.
    """

    sldId_lst: list[CT_SlideId]

    _add_sldId: Callable[..., CT_SlideId]
    sldId = ZeroOrMore("p:sldId")

    def add_sldId(self, rId: str) -> CT_SlideId:
        """Create and return a reference to a new `p:sldId` child element.

        The new `p:sldId` element has its r:id attribute set to `rId`.
        """
        return self._add_sldId(id=self._next_id, rId=rId)

    @property
    def _next_id(self) -> int:
        """The next available slide ID as an `int`.

        Valid slide IDs start at 256. The next integer value greater than the max value in use is
        chosen, which minimizes that chance of reusing the id of a deleted slide.
        """
        MIN_SLIDE_ID = 256
        MAX_SLIDE_ID = 2147483647

        used_ids = [int(s) for s in cast("list[str]", self.xpath("./p:sldId/@id"))]
        simple_next = max([MIN_SLIDE_ID - 1] + used_ids) + 1
        if simple_next <= MAX_SLIDE_ID:
            return simple_next

        # -- fall back to search for next unused from bottom --
        valid_used_ids = sorted(id for id in used_ids if (MIN_SLIDE_ID <= id <= MAX_SLIDE_ID))
        return (
            next(
                candidate_id
                for candidate_id, used_id in enumerate(valid_used_ids, start=MIN_SLIDE_ID)
                if candidate_id != used_id
            )
            if valid_used_ids
            else 256
        )


class CT_SlideMasterIdList(BaseOxmlElement):
    """`p:sldMasterIdLst` element.

    Child of `p:presentation` containing references to the slide masters that belong to the
    presentation.
    """

    sldMasterId_lst: list[CT_SlideMasterIdListEntry]

    sldMasterId = ZeroOrMore("p:sldMasterId")


class CT_SlideMasterIdListEntry(BaseOxmlElement):
    """
    ``<p:sldMasterId>`` element, child of ``<p:sldMasterIdLst>`` containing
    a reference to a slide master.
    """

    rId: str = RequiredAttribute("r:id", XsdString)  # pyright: ignore[reportAssignmentType]


class CT_SlideSize(BaseOxmlElement):
    """`p:sldSz` element.

    Direct child of <p:presentation> that contains the width and height of slides in the
    presentation.
    """

    cx: Length = RequiredAttribute(  # pyright: ignore[reportAssignmentType]
        "cx", ST_SlideSizeCoordinate
    )
    cy: Length = RequiredAttribute(  # pyright: ignore[reportAssignmentType]
        "cy", ST_SlideSizeCoordinate
    )
