import string
from enum import Enum

EOF = None

namespaces = {
    "html": "http://www.w3.org/1999/xhtml",
    "mathml": "http://www.w3.org/1998/Math/MathML",
    "svg": "http://www.w3.org/2000/svg",
    "xlink": "http://www.w3.org/1999/xlink",
    "xml": "http://www.w3.org/XML/1998/namespace",
    "xmlns": "http://www.w3.org/2000/xmlns/"
}

scoping_elements = frozenset([
    (namespaces["html"], "applet"),
    (namespaces["html"], "caption"),
    (namespaces["html"], "html"),
    (namespaces["html"], "marquee"),
    (namespaces["html"], "object"),
    (namespaces["html"], "table"),
    (namespaces["html"], "td"),
    (namespaces["html"], "th"),
    (namespaces["mathml"], "mi"),
    (namespaces["mathml"], "mo"),
    (namespaces["mathml"], "mn"),
    (namespaces["mathml"], "ms"),
    (namespaces["mathml"], "mtext"),
    (namespaces["mathml"], "annotation-xml"),
    (namespaces["svg"], "foreignObject"),
    (namespaces["svg"], "desc"),
    (namespaces["svg"], "title"),
])

special_elements = frozenset([
    (namespaces["html"], "address"),
    (namespaces["html"], "applet"),
    (namespaces["html"], "area"),
    (namespaces["html"], "article"),
    (namespaces["html"], "aside"),
    (namespaces["html"], "base"),
    (namespaces["html"], "basefont"),
    (namespaces["html"], "bgsound"),
    (namespaces["html"], "blockquote"),
    (namespaces["html"], "body"),
    (namespaces["html"], "br"),
    (namespaces["html"], "button"),
    (namespaces["html"], "caption"),
    (namespaces["html"], "center"),
    (namespaces["html"], "col"),
    (namespaces["html"], "colgroup"),
    (namespaces["html"], "command"),
    (namespaces["html"], "dd"),
    (namespaces["html"], "details"),
    (namespaces["html"], "dir"),
    (namespaces["html"], "div"),
    (namespaces["html"], "dl"),
    (namespaces["html"], "dt"),
    (namespaces["html"], "embed"),
    (namespaces["html"], "fieldset"),
    (namespaces["html"], "figure"),
    (namespaces["html"], "footer"),
    (namespaces["html"], "form"),
    (namespaces["html"], "frame"),
    (namespaces["html"], "frameset"),
    (namespaces["html"], "h1"),
    (namespaces["html"], "h2"),
    (namespaces["html"], "h3"),
    (namespaces["html"], "h4"),
    (namespaces["html"], "h5"),
    (namespaces["html"], "h6"),
    (namespaces["html"], "head"),
    (namespaces["html"], "header"),
    (namespaces["html"], "hr"),
    (namespaces["html"], "html"),
    (namespaces["html"], "iframe"),
    # Note that image is commented out in the spec as "this isn't an
    # element that can end up on the stack, so it doesn't matter,"
    (namespaces["html"], "image"),
    (namespaces["html"], "img"),
    (namespaces["html"], "input"),
    (namespaces["html"], "isindex"),
    (namespaces["html"], "li"),
    (namespaces["html"], "link"),
    (namespaces["html"], "listing"),
    (namespaces["html"], "marquee"),
    (namespaces["html"], "menu"),
    (namespaces["html"], "meta"),
    (namespaces["html"], "nav"),
    (namespaces["html"], "noembed"),
    (namespaces["html"], "noframes"),
    (namespaces["html"], "noscript"),
    (namespaces["html"], "object"),
    (namespaces["html"], "ol"),
    (namespaces["html"], "p"),
    (namespaces["html"], "param"),
    (namespaces["html"], "plaintext"),
    (namespaces["html"], "pre"),
    (namespaces["html"], "script"),
    (namespaces["html"], "section"),
    (namespaces["html"], "select"),
    (namespaces["html"], "style"),
    (namespaces["html"], "table"),
    (namespaces["html"], "tbody"),
    (namespaces["html"], "td"),
    (namespaces["html"], "textarea"),
    (namespaces["html"], "tfoot"),
    (namespaces["html"], "th"),
    (namespaces["html"], "thead"),
    (namespaces["html"], "title"),
    (namespaces["html"], "tr"),
    (namespaces["html"], "ul"),
    (namespaces["html"], "wbr"),
    (namespaces["html"], "xmp"),
    (namespaces["svg"], "foreignObject")
])

html_integration_point_elements = frozenset([
    (namespaces["mathml"], "annotation-xml"),
    (namespaces["svg"], "foreignObject"),
    (namespaces["svg"], "desc"),
    (namespaces["svg"], "title")
])

mathml_text_integration_point_elements = frozenset([
    (namespaces["mathml"], "mi"),
    (namespaces["mathml"], "mo"),
    (namespaces["mathml"], "mn"),
    (namespaces["mathml"], "ms"),
    (namespaces["mathml"], "mtext")
])

adjust_svg_attributes = {
    "attributename": "attributeName",
    "attributetype": "attributeType",
    "basefrequency": "baseFrequency",
    "baseprofile": "baseProfile",
    "calcmode": "calcMode",
    "clippathunits": "clipPathUnits",
    "contentscripttype": "contentScriptType",
    "contentstyletype": "contentStyleType",
    "diffuseconstant": "diffuseConstant",
    "edgemode": "edgeMode",
    "externalresourcesrequired": "externalResourcesRequired",
    "filterres": "filterRes",
    "filterunits": "filterUnits",
    "glyphref": "glyphRef",
    "gradienttransform": "gradientTransform",
    "gradientunits": "gradientUnits",
    "kernelmatrix": "kernelMatrix",
    "kernelunitlength": "kernelUnitLength",
    "keypoints": "keyPoints",
    "keysplines": "keySplines",
    "keytimes": "keyTimes",
    "lengthadjust": "lengthAdjust",
    "limitingconeangle": "limitingConeAngle",
    "markerheight": "markerHeight",
    "markerunits": "markerUnits",
    "markerwidth": "markerWidth",
    "maskcontentunits": "maskContentUnits",
    "maskunits": "maskUnits",
    "numoctaves": "numOctaves",
    "pathlength": "pathLength",
    "patterncontentunits": "patternContentUnits",
    "patterntransform": "patternTransform",
    "patternunits": "patternUnits",
    "pointsatx": "pointsAtX",
    "pointsaty": "pointsAtY",
    "pointsatz": "pointsAtZ",
    "preservealpha": "preserveAlpha",
    "preserveaspectratio": "preserveAspectRatio",
    "primitiveunits": "primitiveUnits",
    "refx": "refX",
    "refy": "refY",
    "repeatcount": "repeatCount",
    "repeatdur": "repeatDur",
    "requiredextensions": "requiredExtensions",
    "requiredfeatures": "requiredFeatures",
    "specularconstant": "specularConstant",
    "specularexponent": "specularExponent",
    "spreadmethod": "spreadMethod",
    "startoffset": "startOffset",
    "stddeviation": "stdDeviation",
    "stitchtiles": "stitchTiles",
    "surfacescale": "surfaceScale",
    "systemlanguage": "systemLanguage",
    "tablevalues": "tableValues",
    "targetx": "targetX",
    "targety": "targetY",
    "textlength": "textLength",
    "viewbox": "viewBox",
    "viewtarget": "viewTarget",
    "xchannelselector": "xChannelSelector",
    "ychannelselector": "yChannelSelector",
    "zoomandpan": "zoomAndPan"
}

adjust_mathml_attributes = {"definitionurl": "definitionURL"}

adjust_foreign_attributes = {
    "xlink:actuate": ("xlink", "actuate", namespaces["xlink"]),
    "xlink:arcrole": ("xlink", "arcrole", namespaces["xlink"]),
    "xlink:href": ("xlink", "href", namespaces["xlink"]),
    "xlink:role": ("xlink", "role", namespaces["xlink"]),
    "xlink:show": ("xlink", "show", namespaces["xlink"]),
    "xlink:title": ("xlink", "title", namespaces["xlink"]),
    "xlink:type": ("xlink", "type", namespaces["xlink"]),
    "xml:base": ("xml", "base", namespaces["xml"]),
    "xml:lang": ("xml", "lang", namespaces["xml"]),
    "xml:space": ("xml", "space", namespaces["xml"]),
    "xmlns": (None, "xmlns", namespaces["xmlns"]),
    "xmlns:xlink": ("xmlns", "xlink", namespaces["xmlns"])
}

space_characters = frozenset([
    "\t",
    "\n",
    "\u000C",
    " ",
    "\r"
])

table_insert_mode_elements = frozenset([
    "table",
    "tbody",
    "tfoot",
    "thead",
    "tr"
])

ascii_lowercase = frozenset(string.ascii_lowercase)
ascii_uppercase = frozenset(string.ascii_uppercase)
ascii_letters = frozenset(string.ascii_letters)
digits = frozenset(string.digits)
hexdigits = frozenset(string.hexdigits)

ascii_upper_to_lower = {ord(c): ord(c.lower()) for c in string.ascii_uppercase}

# Heading elements need to be ordered
heading_elements = (
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6"
)

void_elements = frozenset([
    "area",
    "base",
    "br",
    "col",
    "command",  # removed ^1
    "embed",
    "event-source",  # renamed and later removed ^2
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",  # deprecated ^3
    "source",
    "track",
    "wbr",
])

# Removals and deprecations in the HTML 5 spec:
# ^1: command
#     http://lists.whatwg.org/pipermail/whatwg-whatwg.org/2012-December/038472.html
#     https://github.com/whatwg/html/commit/9e2e25f4ae90969a7c64e0763c98548a35b50af8
# ^2: event-source
#     renamed to eventsource in 7/2008:
#     https://github.com/whatwg/html/commit/d157945d0285b4463a04b57318da0c4b300a99e7
#     removed entirely in 2/2009:
#     https://github.com/whatwg/html/commit/43cbdbfbb7eb74b0d65e0f4caab2020c0b2a16ff
# ^3: param
#     https://developer.mozilla.org/en-US/docs/Web/HTML/Element/param

cdata_elements = frozenset(['title', 'textarea'])

rcdata_elements = frozenset([
    'style',
    'script',
    'xmp',
    'iframe',
    'noembed',
    'noframes',
    'noscript'
])

replacement_characters = {
    0x0: "\uFFFD",
    0x0d: "\u000D",
    0x80: "\u20AC",
    0x81: "\u0081",
    0x82: "\u201A",
    0x83: "\u0192",
    0x84: "\u201E",
    0x85: "\u2026",
    0x86: "\u2020",
    0x87: "\u2021",
    0x88: "\u02C6",
    0x89: "\u2030",
    0x8A: "\u0160",
    0x8B: "\u2039",
    0x8C: "\u0152",
    0x8D: "\u008D",
    0x8E: "\u017D",
    0x8F: "\u008F",
    0x90: "\u0090",
    0x91: "\u2018",
    0x92: "\u2019",
    0x93: "\u201C",
    0x94: "\u201D",
    0x95: "\u2022",
    0x96: "\u2013",
    0x97: "\u2014",
    0x98: "\u02DC",
    0x99: "\u2122",
    0x9A: "\u0161",
    0x9B: "\u203A",
    0x9C: "\u0153",
    0x9D: "\u009D",
    0x9E: "\u017E",
    0x9F: "\u0178",
}

class Token(Enum):
    DOCTYPE = 0
    CHARACTERS = 1
    SPACE_CHARACTERS = 2
    START_TAG = 3
    END_TAG = 4
    EMPTY_TAG = 5
    COMMENT = 6
    PARSE_ERROR = 7

tag_token_types = frozenset([Token.START_TAG, Token.END_TAG, Token.EMPTY_TAG])

prefixes = {url: name for name, url in namespaces.items()}
prefixes["http://www.w3.org/1998/Math/MathML"] = "math"

class ReparseError(Exception):
    pass
