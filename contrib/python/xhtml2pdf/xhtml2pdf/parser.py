# Copyright 2010 Dirk Holtwick, holtwick.it
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import copy
import logging
import re
import xml.dom.minidom
from xml.dom import Node

import html5lib
from html5lib import treebuilders
from reportlab.platypus.doctemplate import FrameBreak, NextPageTemplate
from reportlab.platypus.flowables import KeepInFrame, PageBreak

from xhtml2pdf.default import (
    BOOL,
    BOX,
    COLOR,
    FILE,
    FONT,
    INT,
    MUST,
    POS,
    SIZE,
    STRING,
    TAGS,
)
from xhtml2pdf.files import pisaTempFile

# TODO: Why do we need to import these Tags here? They aren't uses in this file or any other file,
#  but if we don't import them, the tests fail. Very strange (fbernhart)
from xhtml2pdf.tables import (  # noqa: F401
    TableData,
    pisaTagTABLE,
    pisaTagTD,
    pisaTagTH,
    pisaTagTR,
)
from xhtml2pdf.tags import (  # noqa: F401
    pisaTag,
    pisaTagA,
    pisaTagBODY,
    pisaTagBR,
    pisaTagCANVAS,
    pisaTagDIV,
    pisaTagFONT,
    pisaTagH1,
    pisaTagH2,
    pisaTagH3,
    pisaTagH4,
    pisaTagH5,
    pisaTagH6,
    pisaTagHR,
    pisaTagIMG,
    pisaTagINPUT,
    pisaTagLI,
    pisaTagMETA,
    pisaTagOL,
    pisaTagP,
    pisaTagPDFBARCODE,
    pisaTagPDFFONT,
    pisaTagPDFFRAME,
    pisaTagPDFLANGUAGE,
    pisaTagPDFNEXTFRAME,
    pisaTagPDFNEXTPAGE,
    pisaTagPDFNEXTTEMPLATE,
    pisaTagPDFPAGECOUNT,
    pisaTagPDFPAGENUMBER,
    pisaTagPDFSPACER,
    pisaTagPDFTEMPLATE,
    pisaTagPDFTOC,
    pisaTagSTYLE,
    pisaTagSUB,
    pisaTagSUP,
    pisaTagTEXTAREA,
    pisaTagTITLE,
    pisaTagUL,
)
from xhtml2pdf.util import (
    getAlign,
    getBool,
    getBox,
    getColor,
    getPos,
    getSize,
    toList,
    transform_attrs,
)
from xhtml2pdf.w3c import cssDOMElementInterface
from xhtml2pdf.xhtml2pdf_reportlab import PmlLeftPageBreak, PmlRightPageBreak

log = logging.getLogger(__name__)

CSSAttrCache: dict[str, dict] = {}

rxhttpstrip = re.compile("https?://[^/]+(.*)", re.M | re.I)


class AttrContainer(dict):
    def __getattr__(self, name):
        try:
            return dict.__getattr__(self, name)
        except Exception:
            return self[name]


def pisaGetAttributes(c, tag, attributes):
    attrs = {}
    if attributes:
        for k, v in attributes.items():
            try:
                # XXX no Unicode! Reportlab fails with template names
                attrs[str(k)] = str(v)
            except Exception as e:  # noqa: PERF203
                log.debug(
                    "%s during string conversion for %s=%s", e, k, v, exc_info=True
                )
                attrs[k] = v

    nattrs = {}
    if tag in TAGS:
        block, adef = TAGS[tag]
        adef["id"] = STRING

        for k, v in adef.items():
            nattrs[k] = None
            # print k, v
            # defaults, wenn vorhanden
            if isinstance(v, tuple):
                if v[1] == MUST and k not in attrs:
                    log.warning(c.warning("Attribute '%s' must be set!", k))
                    nattrs[k] = None
                    continue
                nv = attrs.get(k, v[1])
                dfl = v[1]
                v = v[0]
            else:
                nv = attrs.get(k, None)
                dfl = None

            if nv is not None:
                if isinstance(v, list):
                    nv = nv.strip().lower()
                    if nv not in v:
                        # ~ raise PML_EXCEPTION, "attribute '%s' of wrong value, allowed is one of: %s" % (k, repr(v))
                        log.warning(
                            c.warning(
                                "Attribute '%s' of wrong value, allowed is one of: %s",
                                k,
                                repr(v),
                            )
                        )
                        nv = dfl

                elif v == BOOL:
                    nv = nv.strip().lower()
                    nv = nv in {"1", "y", "yes", "true", str(k)}

                elif v == SIZE:
                    try:
                        nv = getSize(nv)
                    except Exception:
                        log.warning(c.warning("Attribute '%s' expects a size value", k))

                elif v == BOX:
                    nv = getBox(nv, c.pageSize)

                elif v == POS:
                    nv = getPos(nv, c.pageSize)

                elif v == INT:
                    nv = int(nv)

                elif v == COLOR:
                    nv = getColor(nv)

                elif v == FILE:
                    nv = c.getFile(nv)

                elif v == FONT:
                    nv = c.getFontName(nv)

                nattrs[k] = nv

    return AttrContainer(nattrs)


attrNames = """
    color
    font-family
    font-size
    font-weight
    font-style
    text-decoration
    line-height
    letter-spacing
    background-color
    display
    margin-left
    margin-right
    margin-top
    margin-bottom
    padding-left
    padding-right
    padding-top
    padding-bottom
    border-top-color
    border-top-style
    border-top-width
    border-bottom-color
    border-bottom-style
    border-bottom-width
    border-left-color
    border-left-style
    border-left-width
    border-right-color
    border-right-style
    border-right-width
    text-align
    vertical-align
    width
    height
    zoom
    page-break-after
    page-break-before
    list-style-type
    list-style-image
    white-space
    text-indent
    -pdf-page-break
    -pdf-frame-break
    -pdf-next-page
    -pdf-keep-with-next
    -pdf-outline
    -pdf-outline-level
    -pdf-outline-open
    -pdf-line-spacing
    -pdf-keep-in-frame-mode
    -pdf-word-wrap
    """.strip().split()


def getCSSAttr(self, cssCascade, attrName, default=NotImplemented):
    if attrName in self.cssAttrs:
        return self.cssAttrs[attrName]

    try:
        result = cssCascade.findStyleFor(self.cssElement, attrName, default)
    except LookupError:
        result = None

    # XXX Workaround for inline styles
    try:
        style = self.cssStyle
    except Exception:
        style = self.cssStyle = cssCascade.parser.parseInline(
            self.cssElement.getStyleAttr() or ""
        )[0]
    if attrName in style:
        result = style[attrName]

    if result == "inherit":
        if hasattr(self.parentNode, "getCSSAttr"):
            result = self.parentNode.getCSSAttr(cssCascade, attrName, default)
        elif default is not NotImplemented:
            return default
        msg = f"Could not find inherited CSS attribute value for '{attrName}'"
        raise LookupError(msg)

    if result is not None:
        self.cssAttrs[attrName] = result
    return result


# TODO: Monkeypatching standard lib should go away.
xml.dom.minidom.Element.getCSSAttr = getCSSAttr  # type: ignore[attr-defined]

# Create an aliasing system.  Many sources use non-standard tags, because browsers allow
# them to.  This allows us to map a nonstandard name to the standard one.
nonStandardAttrNames = {"bgcolor": "background-color"}


def mapNonStandardAttrs(c, _node, attrList):
    for attr in nonStandardAttrNames:
        if attr in attrList and nonStandardAttrNames[attr] not in c:
            c[nonStandardAttrNames[attr]] = attrList[attr]
    return c


def getCSSAttrCacheKey(node):
    _cl = _id = _st = ""
    for k, v in node.attributes.items():
        if k == "class":
            _cl = v
        elif k == "id":
            _id = v
        elif k == "style":
            _st = v
    return f"{id(node.parentNode)}#{node.tagName.lower()}#{_cl}#{_id}#{_st}"


def CSSCollect(node, c):
    # node.cssAttrs = {}
    # return node.cssAttrs

    if c.css:
        key = getCSSAttrCacheKey(node)

        if (
            hasattr(node.parentNode, "tagName")
            and node.parentNode.tagName.lower() != "html"
        ):
            CachedCSSAttr = CSSAttrCache.get(key, None)
            if CachedCSSAttr is not None:
                node.cssAttrs = CachedCSSAttr
                return CachedCSSAttr

        node.cssElement = cssDOMElementInterface.CSSDOMElementInterface(node)
        node.cssAttrs = {}
        # node.cssElement.onCSSParserVisit(c.cssCascade.parser)
        cssAttrMap = {}
        for cssAttrName in attrNames:
            try:
                cssAttrMap[cssAttrName] = node.getCSSAttr(c.cssCascade, cssAttrName)
            # except LookupError:
            #    pass
            except Exception as e:  # noqa: PERF203
                log.debug("%r during CSS attr '%s'", e, cssAttrName, exc_info=True)

        CSSAttrCache[key] = node.cssAttrs
    return node.cssAttrs


def lower(sequence):
    if isinstance(sequence, str):
        return sequence.lower()
    return sequence[0].lower()


def CSS2Frag(c, kw, isBlock):
    # COLORS
    if "color" in c.cssAttr:
        c.frag.textColor = getColor(c.cssAttr["color"], "#000000")
    if "background-color" in c.cssAttr:
        c.frag.backColor = getColor(c.cssAttr["background-color"], "#ffffff")
        # FONT SIZE, STYLE, WEIGHT
    if "font-family" in c.cssAttr:
        c.frag.fontName = c.getFontName(c.cssAttr["font-family"])
    if "font-size" in c.cssAttr:
        # XXX inherit
        c.frag.fontSize = max(
            getSize("".join(c.cssAttr["font-size"]), c.frag.fontSize, c.baseFontSize),
            1.0,
        )
    if "line-height" in c.cssAttr:
        leading = "".join(c.cssAttr["line-height"])
        c.frag.leading = getSize(leading, c.frag.fontSize)
        c.frag.leadingSource = leading
    else:
        c.frag.leading = getSize(c.frag.leadingSource, c.frag.fontSize)
    if "letter-spacing" in c.cssAttr:
        c.frag.letterSpacing = c.cssAttr["letter-spacing"]
    if "-pdf-line-spacing" in c.cssAttr:
        c.frag.leadingSpace = getSize("".join(c.cssAttr["-pdf-line-spacing"]))
        # print "line-spacing", c.cssAttr["-pdf-line-spacing"], c.frag.leading
    if "font-weight" in c.cssAttr:
        value = lower(c.cssAttr["font-weight"])
        if value in {"bold", "bolder", "500", "600", "700", "800", "900"}:
            c.frag.bold = 1
        else:
            c.frag.bold = 0
    for value in toList(c.cssAttr.get("text-decoration", "")):
        if "underline" in value:
            c.frag.underline = 1
        if "line-through" in value:
            c.frag.strike = 1
        if "none" in value:
            c.frag.underline = 0
            c.frag.strike = 0
    if "font-style" in c.cssAttr:
        value = lower(c.cssAttr["font-style"])
        if value in {"italic", "oblique"}:
            c.frag.italic = 1
        else:
            c.frag.italic = 0
    if "white-space" in c.cssAttr:
        # normal | pre | nowrap
        c.frag.whiteSpace = str(c.cssAttr["white-space"]).lower()
        # ALIGN & VALIGN
    if "text-align" in c.cssAttr:
        c.frag.alignment = getAlign(c.cssAttr["text-align"])
    if "vertical-align" in c.cssAttr:
        c.frag.vAlign = c.cssAttr["vertical-align"]
        # HEIGHT & WIDTH
    if "height" in c.cssAttr:
        try:
            # XXX Relative is not correct!
            c.frag.height = "".join(toList(c.cssAttr["height"]))
        except TypeError:
            # sequence item 0: expected string, tuple found
            c.frag.height = "".join(toList(c.cssAttr["height"][0]))
        if c.frag.height in {"auto"}:
            c.frag.height = None
    if "width" in c.cssAttr:
        try:
            # XXX Relative is not correct!
            c.frag.width = "".join(toList(c.cssAttr["width"]))
        except TypeError:
            c.frag.width = "".join(toList(c.cssAttr["width"][0]))
        if c.frag.width in {"auto"}:
            c.frag.width = None
        # ZOOM
    if "zoom" in c.cssAttr:
        # XXX Relative is not correct!
        zoom = "".join(toList(c.cssAttr["zoom"]))
        if zoom.endswith("%"):
            zoom = float(zoom[:-1]) / 100.0
        c.frag.zoom = float(zoom)
        # MARGINS & LIST INDENT, STYLE
    if isBlock:
        transform_attrs(
            c.frag,
            (
                ("spaceBefore", "margin-top"),
                ("spaceAfter", "margin-bottom"),
                ("firstLineIndent", "text-indent"),
            ),
            c.cssAttr,
            getSize,
            extras=c.frag.fontSize,
        )

        if "margin-left" in c.cssAttr:
            c.frag.bulletIndent = kw["margin-left"]  # For lists
            kw["margin-left"] += getSize(c.cssAttr["margin-left"], c.frag.fontSize)
            c.frag.leftIndent = kw["margin-left"]
        if "margin-right" in c.cssAttr:
            kw["margin-right"] += getSize(c.cssAttr["margin-right"], c.frag.fontSize)
            c.frag.rightIndent = kw["margin-right"]

        if "list-style-type" in c.cssAttr:
            c.frag.listStyleType = str(c.cssAttr["list-style-type"]).lower()
        if "list-style-image" in c.cssAttr:
            c.frag.listStyleImage = c.getFile(c.cssAttr["list-style-image"])
        # PADDINGS
    if isBlock:
        transform_attrs(
            c.frag,
            (
                ("paddingTop", "padding-top"),
                ("paddingBottom", "padding-bottom"),
                ("paddingLeft", "padding-left"),
                ("paddingRight", "padding-right"),
            ),
            c.cssAttr,
            getSize,
            extras=c.frag.fontSize,
        )

        # BORDERS
    if isBlock:
        transform_attrs(
            c.frag,
            (
                ("borderTopWidth", "border-top-width"),
                ("borderBottomWidth", "border-bottom-width"),
                ("borderLeftWidth", "border-left-width"),
                ("borderRightWidth", "border-right-width"),
            ),
            c.cssAttr,
            getSize,
            extras=c.frag.fontSize,
        )
        transform_attrs(
            c.frag,
            (
                ("borderTopStyle", "border-top-style"),
                ("borderBottomStyle", "border-bottom-style"),
                ("borderLeftStyle", "border-left-style"),
                ("borderRightStyle", "border-right-style"),
            ),
            c.cssAttr,
            lambda x: x,
        )

        transform_attrs(
            c.frag,
            (
                ("borderTopColor", "border-top-color"),
                ("borderBottomColor", "border-bottom-color"),
                ("borderLeftColor", "border-left-color"),
                ("borderRightColor", "border-right-color"),
            ),
            c.cssAttr,
            getColor,
        )


def pisaPreLoop(node, context, *, collect=False):
    """Collect all CSS definitions."""
    data = ""
    if node.nodeType == Node.TEXT_NODE and collect:
        data = node.data

    elif node.nodeType == Node.ELEMENT_NODE:
        name = node.tagName.lower()

        if name in {"style", "link"}:
            attr = pisaGetAttributes(context, name, node.attributes)
            media = [x.strip() for x in attr.media.lower().split(",") if x.strip()]

            if attr.get("type", "").lower() in {"", "text/css"} and (
                not media or "all" in media or "print" in media or "pdf" in media
            ):
                if name == "style":
                    for node in node.childNodes:
                        data += pisaPreLoop(node, context, collect=True)
                    context.addCSS(data)
                    return ""

                if name == "link" and attr.href and attr.rel.lower() == "stylesheet":
                    # print "CSS LINK", attr
                    context.addCSS(
                        '\n@import "{}" {};'.format(attr.href, ",".join(media))
                    )

    for node in node.childNodes:
        result = pisaPreLoop(node, context, collect=collect)
        if collect:
            data += result

    return data


def pisaLoop(node, context, path=None, **kw):
    if path is None:
        path = []

    # Initialize KW
    if not kw:
        kw = {"margin-top": 0, "margin-bottom": 0, "margin-left": 0, "margin-right": 0}
    else:
        kw = copy.copy(kw)

    # indent = len(path) * "  " # only used for debug print statements

    # TEXT
    if node.nodeType == Node.TEXT_NODE:
        # print indent, "#", repr(node.data) #, context.frag
        context.addFrag(node.data)
        # context.text.append(node.value)

    # ELEMENT
    elif node.nodeType == Node.ELEMENT_NODE:
        node.tagName = node.tagName.replace(":", "").lower()

        if node.tagName in {"style", "script"}:
            return

        path = [*copy.copy(path), node.tagName]

        # Prepare attributes
        attr = pisaGetAttributes(context, node.tagName, node.attributes)
        # log.debug(indent + "<%s %s>" % (node.tagName, attr) +
        # repr(node.attributes.items())) #, path

        # Calculate styles
        context.cssAttr = CSSCollect(node, context)
        context.cssAttr = mapNonStandardAttrs(context.cssAttr, node, attr)
        context.node = node

        # Block?
        PAGE_BREAK = 1
        PAGE_BREAK_RIGHT = 2
        PAGE_BREAK_LEFT = 3

        pageBreakAfter = False
        frameBreakAfter = False
        display = lower(context.cssAttr.get("display", "inline"))
        # print indent, node.tagName, display,
        # context.cssAttr.get("background-color", None), attr
        isBlock = display == "block"

        if isBlock:
            context.addPara()

            # Page break by CSS
            if "-pdf-next-page" in context.cssAttr:
                context.addStory(
                    NextPageTemplate(str(context.cssAttr["-pdf-next-page"]))
                )
            if (
                "-pdf-page-break" in context.cssAttr
                and str(context.cssAttr["-pdf-page-break"]).lower() == "before"
            ):
                context.addStory(PageBreak())
            if "-pdf-frame-break" in context.cssAttr:
                if str(context.cssAttr["-pdf-frame-break"]).lower() == "before":
                    context.addStory(FrameBreak())
                if str(context.cssAttr["-pdf-frame-break"]).lower() == "after":
                    frameBreakAfter = True
            if "page-break-before" in context.cssAttr:
                if str(context.cssAttr["page-break-before"]).lower() == "always":
                    context.addStory(PageBreak())
                if str(context.cssAttr["page-break-before"]).lower() == "right":
                    context.addStory(PageBreak())
                    context.addStory(PmlRightPageBreak())
                if str(context.cssAttr["page-break-before"]).lower() == "left":
                    context.addStory(PageBreak())
                    context.addStory(PmlLeftPageBreak())
            if "page-break-after" in context.cssAttr:
                if str(context.cssAttr["page-break-after"]).lower() == "always":
                    pageBreakAfter = PAGE_BREAK
                if str(context.cssAttr["page-break-after"]).lower() == "right":
                    pageBreakAfter = PAGE_BREAK_RIGHT
                if str(context.cssAttr["page-break-after"]).lower() == "left":
                    pageBreakAfter = PAGE_BREAK_LEFT

        if display == "none":
            # print "none!"
            return

        # Translate CSS to frags

        # Save previous frag styles
        context.pushFrag()

        # Map styles to Reportlab fragment properties
        CSS2Frag(context, kw, isBlock=isBlock)

        # EXTRAS
        transform_attrs(
            context.frag,
            (
                ("keepWithNext", "-pdf-keep-with-next"),
                ("outline", "-pdf-outline"),
                # ("borderLeftColor", "-pdf-outline-open"),
            ),
            context.cssAttr,
            getBool,
        )

        if "-pdf-outline-level" in context.cssAttr:
            context.frag.outlineLevel = int(context.cssAttr["-pdf-outline-level"])

        if "-pdf-word-wrap" in context.cssAttr:
            context.frag.wordWrap = context.cssAttr["-pdf-word-wrap"]

        # handle keep-in-frame
        keepInFrameMode = None
        keepInFrameMaxWidth = 0
        keepInFrameMaxHeight = 0
        if "-pdf-keep-in-frame-mode" in context.cssAttr:
            value = str(context.cssAttr["-pdf-keep-in-frame-mode"]).strip().lower()
            if value in {"shrink", "error", "overflow", "truncate"}:
                keepInFrameMode = value
            else:
                keepInFrameMode = "shrink"
            # Added because we need a default value.

        if "-pdf-keep-in-frame-max-width" in context.cssAttr:
            keepInFrameMaxWidth = getSize(
                "".join(context.cssAttr["-pdf-keep-in-frame-max-width"])
            )
        if "-pdf-keep-in-frame-max-height" in context.cssAttr:
            keepInFrameMaxHeight = getSize(
                "".join(context.cssAttr["-pdf-keep-in-frame-max-height"])
            )

        # ignore nested keep-in-frames, tables have their own KIF handling
        keepInFrame = keepInFrameMode is not None and context.keepInFrameIndex is None
        if keepInFrame:
            # keep track of current story index, so we can wrap everythink
            # added after this point in a KeepInFrame
            context.keepInFrameIndex = len(context.story)

        # BEGIN tag
        klass = globals().get("pisaTag%s" % node.tagName.replace(":", "").upper(), None)
        obj = None

        # Static block
        elementId = attr.get("id", None)
        staticFrame = context.frameStatic.get(elementId, None)
        if staticFrame:
            context.frag.insideStaticFrame += 1
            oldStory = context.swapStory()

        # Tag specific operations
        if klass is not None:
            obj = klass(node, attr)
            obj.start(context)

        # Visit child nodes
        context.fragBlock = fragBlock = copy.copy(context.frag)
        for nnode in node.childNodes:
            pisaLoop(nnode, context, path, **kw)
        context.fragBlock = fragBlock

        # END tag
        if obj:
            obj.end(context)

        # Block?
        if isBlock:
            context.addPara()

            # XXX Buggy!

            # Page break by CSS
            if pageBreakAfter:
                context.addStory(PageBreak())
                if pageBreakAfter == PAGE_BREAK_RIGHT:
                    context.addStory(PmlRightPageBreak())
                if pageBreakAfter == PAGE_BREAK_LEFT:
                    context.addStory(PmlLeftPageBreak())
            if frameBreakAfter:
                context.addStory(FrameBreak())

        if keepInFrame:
            # get all content added after start of -pdf-keep-in-frame and wrap
            # it in a KeepInFrame
            substory = context.story[context.keepInFrameIndex :]
            context.story = context.story[: context.keepInFrameIndex]
            context.story.append(
                KeepInFrame(
                    content=substory,
                    maxWidth=keepInFrameMaxWidth,
                    maxHeight=keepInFrameMaxHeight,
                    mode=keepInFrameMode,
                )
            )
            # mode wasn't being used; it is necessary for tables or images at
            # end of page.
            context.keepInFrameIndex = None

        # Static block, END
        if staticFrame:
            context.addPara()
            for frame in staticFrame:
                frame.pisaStaticStory = context.story
            context.swapStory(oldStory)
            context.frag.insideStaticFrame -= 1

        # context.debug(1, indent, "</%s>" % (node.tagName))

        # Reset frag style
        context.pullFrag()

    # Unknown or not handled
    else:
        # context.debug(1, indent, "???", node, node.nodeType, repr(node))
        # Loop over children
        for node in node.childNodes:
            pisaLoop(node, context, path, **kw)


def pisaParser(
    src,
    context,
    default_css="",
    xhtml=False,  # noqa: FBT002
    encoding="utf8",
    xml_output=None,
):
    """
    - Parse HTML and get miniDOM
    - Extract CSS information, add default CSS, parse CSS
    - Handle the document DOM itself and build reportlab story
    - Return Context object.
    """
    global CSSAttrCache  # noqa: PLW0603
    CSSAttrCache = {}

    if xhtml:
        log.warning("xhtml parameter will be removed on next release 0.2.8")
        # TODO: XHTMLParser doesn't seem to exist...
        parser = html5lib.XHTMLParser(tree=treebuilders.getTreeBuilder("dom"))
    else:
        parser = html5lib.HTMLParser(tree=treebuilders.getTreeBuilder("dom"))
    parser_kwargs = {}
    if isinstance(src, str):
        # If an encoding was provided, do not change it.
        if not encoding:
            encoding = "utf-8"
        src = src.encode(encoding)
        src = pisaTempFile(src, capacity=context.capacity)
        # To pass the encoding used to convert the text_type src to binary_type
        # on to html5lib's parser to ensure proper decoding
        parser_kwargs["transport_encoding"] = encoding

    # # Test for the restrictions of html5lib
    # if encoding:
    #     # Workaround for html5lib<0.11.1
    #     if hasattr(inputstream, "isValidEncoding"):
    #         if encoding.strip().lower() == "utf8":
    #             encoding = "utf-8"
    #         if not inputstream.isValidEncoding(encoding):
    #             log.error("%r is not a valid encoding e.g. 'utf8' is not valid but 'utf-8' is!", encoding)
    #     else:
    #         if inputstream.codecName(encoding) is None:
    #             log.error("%r is not a valid encoding", encoding)
    document = parser.parse(src, **parser_kwargs)  # encoding=encoding)

    if xml_output:
        xml_output.write(document.toprettyxml(encoding=encoding))

    if default_css:
        context.addDefaultCSS(default_css)

    pisaPreLoop(document, context)
    context.parseCSS()
    pisaLoop(document, context)
    return context


# Shortcuts

HTML2PDF = pisaParser


def XHTML2PDF(*a, **kw):
    kw["xhtml"] = True
    return HTML2PDF(*a, **kw)


XML2PDF = XHTML2PDF
