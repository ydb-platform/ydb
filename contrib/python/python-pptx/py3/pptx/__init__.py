"""Initialization module for python-pptx package."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pptx.exc as exceptions
from pptx.api import Presentation
from pptx.opc.constants import CONTENT_TYPE as CT
from pptx.opc.package import PartFactory
from pptx.parts.chart import ChartPart
from pptx.parts.coreprops import CorePropertiesPart
from pptx.parts.image import ImagePart
from pptx.parts.media import MediaPart
from pptx.parts.presentation import PresentationPart
from pptx.parts.slide import (
    NotesMasterPart,
    NotesSlidePart,
    SlideLayoutPart,
    SlideMasterPart,
    SlidePart,
)

if TYPE_CHECKING:
    from pptx.opc.package import Part

__version__ = "1.0.2"

sys.modules["pptx.exceptions"] = exceptions
del sys

__all__ = ["Presentation"]

content_type_to_part_class_map: dict[str, type[Part]] = {
    CT.PML_PRESENTATION_MAIN: PresentationPart,
    CT.PML_PRES_MACRO_MAIN: PresentationPart,
    CT.PML_TEMPLATE_MAIN: PresentationPart,
    CT.PML_SLIDESHOW_MAIN: PresentationPart,
    CT.OPC_CORE_PROPERTIES: CorePropertiesPart,
    CT.PML_NOTES_MASTER: NotesMasterPart,
    CT.PML_NOTES_SLIDE: NotesSlidePart,
    CT.PML_SLIDE: SlidePart,
    CT.PML_SLIDE_LAYOUT: SlideLayoutPart,
    CT.PML_SLIDE_MASTER: SlideMasterPart,
    CT.DML_CHART: ChartPart,
    CT.BMP: ImagePart,
    CT.GIF: ImagePart,
    CT.JPEG: ImagePart,
    CT.MS_PHOTO: ImagePart,
    CT.PNG: ImagePart,
    CT.TIFF: ImagePart,
    CT.X_EMF: ImagePart,
    CT.X_WMF: ImagePart,
    CT.ASF: MediaPart,
    CT.AVI: MediaPart,
    CT.MOV: MediaPart,
    CT.MP4: MediaPart,
    CT.MPG: MediaPart,
    CT.MS_VIDEO: MediaPart,
    CT.SWF: MediaPart,
    CT.VIDEO: MediaPart,
    CT.WMV: MediaPart,
    CT.X_MS_VIDEO: MediaPart,
    # -- accommodate "image/jpg" as an alias for "image/jpeg" --
    "image/jpg": ImagePart,
}

PartFactory.part_type_for.update(content_type_to_part_class_map)

del (
    ChartPart,
    CorePropertiesPart,
    ImagePart,
    MediaPart,
    SlidePart,
    SlideLayoutPart,
    SlideMasterPart,
    PresentationPart,
    CT,
    PartFactory,
)
