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

from reportlab.lib.pagesizes import (
    A0,
    A1,
    A2,
    A3,
    A4,
    A5,
    A6,
    A7,
    A8,
    A9,
    A10,
    B0,
    B1,
    B2,
    B3,
    B4,
    B5,
    B6,
    B7,
    B8,
    B9,
    B10,
    C0,
    C1,
    C2,
    C3,
    C4,
    C5,
    C6,
    C7,
    C8,
    C9,
    C10,
    ELEVENSEVENTEEN,
    GOV_LEGAL,
    GOV_LETTER,
    HALF_LETTER,
    JUNIOR_LEGAL,
    LEDGER,
    LEGAL,
    LETTER,
    TABLOID,
)

BOOL: int
MUST: int
TAGS: dict
DEFAULT_CSS: str
PML_WARNING: str = "warning"
PML_ERROR: str = "error"
PML_EXCEPTION: str = "PML Exception"
PML_PREFIX: str = "pdf:"

# CLASS   = 1
BOOL = 2
FONT: int = 3
COLOR: int = 4
FILE: int = 5
SIZE: int = 6
INT: int = 7
STRING: int = 8
BOX: int = 9
POS: int = 10
# STYLE   = 11
MUST = 23

#: Definition of all known tags. Also used for building the reference
TAGS = {
    # FORMAT
    # "document": (1, {
    #    "format":               (["a0", "a1", "a2", "a3", "a4", "a5", "a6",
    #                              "b0", "b1", "b2", "b3", "b4", "b5", "b6",
    #                              "letter", "legal", "elevenseventeen"], "a4"),
    #    "orientation":          ["portrait", "landscape"],
    #    "fullscreen":           (BOOL, "0"),
    #    "author":               (STRING, ""),
    #    "subject":              (STRING, ""),
    #    "title":                (STRING, ""),
    #    "duration":             INT,
    #    "showoutline":          (BOOL, "0"),
    #    "outline":              INT,
    #    }),
    "pdftemplate": (
        1,
        {
            "name": (STRING, "body"),
            "format": (
                [
                    "a0",
                    "a1",
                    "a2",
                    "a3",
                    "a4",
                    "a5",
                    "a6",
                    "b0",
                    "b1",
                    "b2",
                    "b3",
                    "b4",
                    "b5",
                    "b6",
                    "letter",
                    "legal",
                    "elevenseventeen",
                ],
                "a4",
            ),
            "orientation": ["portrait", "landscape"],
            "background": FILE,
        },
    ),
    "pdfframe": (
        0,
        {
            "name": (STRING, ""),
            "box": (BOX, MUST),
            "border": (BOOL, "0"),
            "static": (BOOL, "0"),
        },
    ),
    # "static": (1, {
    #    "name":                 STRING,
    #    "box":                  (BOX, MUST),
    #    "border":               (BOOL, "0"),
    #    }),
    "pdfnexttemplate": (0, {"name": (STRING, "body")}),
    "pdfnextpage": (
        0,
        {
            "name": (STRING, ""),
            # "background":           FILE,
        },
    ),
    "pdfnextframe": (0, {}),
    "pdffont": (
        0,
        {
            "src": (FILE, MUST),
            "name": (STRING, MUST),
            # "print":                (BOOL, "0"),
            "encoding": (STRING, "WinAnsiEncoding"),
        },
    ),
    "pdflanguage": (0, {"name": (STRING, "")}),
    "pdfdrawline": (
        0,
        {
            "from": (POS, MUST),
            "to": (POS, MUST),
            "color": (COLOR, "#000000"),
            "width": (SIZE, 1),
        },
    ),
    "drawpoint": (
        0,
        {"pos": (POS, MUST), "color": (COLOR, "#000000"), "width": (SIZE, 1)},
    ),
    "pdfdrawlines": (
        0,
        {"coords": (STRING, MUST), "color": (COLOR, "#000000"), "width": (SIZE, 1)},
    ),
    "pdfdrawstring": (
        0,
        {
            "pos": (POS, MUST),
            "text": (STRING, MUST),
            "color": (COLOR, "#000000"),
            "align": (["left", "center", "right"], "right"),
            "valign": (["top", "middle", "bottom"], "bottom"),
            # "class":                CLASS,
            "rotate": (INT, "0"),
        },
    ),
    "pdfdrawimg": (
        0,
        {
            "pos": (POS, MUST),
            "src": (FILE, MUST),
            "width": SIZE,
            "height": SIZE,
            "align": (["left", "center", "right"], "right"),
            "valign": (["top", "middle", "bottom"], "bottom"),
        },
    ),
    "pdfspacer": (0, {"height": (SIZE, MUST)}),
    "pdfpagenumber": (0, {"example": (STRING, "0")}),
    "pdfpagecount": (0, {}),
    "pdftoc": (0, {}),
    "pdfversion": (0, {}),
    "pdfkeeptogether": (1, {}),
    "pdfkeepinframe": (
        1,
        {
            "maxwidth": SIZE,
            "maxheight": SIZE,
            "mergespace": (INT, 1),
            "mode": (["error", "overflow", "shrink", "truncate"], "shrink"),
            "name": (STRING, ""),
        },
    ),
    # The chart example, see pml_charts
    "pdfchart": (
        1,
        {
            "type": (["spider", "bar"], "bar"),
            "strokecolor": (COLOR, "#000000"),
            "width": (SIZE, MUST),
            "height": (SIZE, MUST),
        },
    ),
    "pdfchartdata": (
        0,
        {
            "set": (STRING, MUST),
            "value": STRING,
            # "label":                (STRING),
            "strokecolor": COLOR,
            "fillcolor": COLOR,
            "strokewidth": SIZE,
        },
    ),
    "pdfchartlabel": (0, {"value": (STRING, MUST)}),
    "pdfbarcode": (
        0,
        {
            "value": (STRING, MUST),
            "type": (
                [
                    "i2of5",
                    "itf",
                    "code39",
                    "extendedcode39",
                    "code93",
                    "extendedcode93",
                    "msi",
                    "codabar",
                    "nw7",
                    "code11",
                    "fim",
                    "postnet",
                    "usps4s",
                    "code128",
                    "ean13",
                    "ean8",
                    "qr",
                ],
                "code128",
            ),
            "humanreadable": (STRING, "0"),
            "vertical": (STRING, "0"),
            "checksum": (STRING, "1"),
            "barwidth": SIZE,
            "barheight": SIZE,
            "fontsize": SIZE,
            "align": (["baseline", "top", "middle", "bottom"], "baseline"),
        },
    ),
    # ========================================================
    "link": (
        0,
        {
            "href": (STRING, MUST),
            "rel": (STRING, ""),
            "type": (STRING, ""),
            "media": (STRING, "all"),
            "charset": (STRING, "latin1"),  # XXX Must be something else...
        },
    ),
    "meta": (0, {"name": (STRING, ""), "content": (STRING, "")}),
    "style": (0, {"type": (STRING, ""), "media": (STRING, "all")}),
    "img": (
        0,
        {
            "src": (FILE, MUST),
            "width": SIZE,
            "height": SIZE,
            "align": [
                "top",
                "middle",
                "bottom",
                "left",
                "right",
                "texttop",
                "absmiddle",
                "absbottom",
                "baseline",
            ],
        },
    ),
    "table": (
        1,
        {
            "align": (["left", "center", "right"], "left"),
            "valign": (["top", "bottom", "middle"], "middle"),
            "border": (SIZE, "0"),
            "bordercolor": (COLOR, "#000000"),
            "bgcolor": COLOR,
            "cellpadding": (SIZE, "0"),
            "cellspacing": (SIZE, "0"),
            "repeat": (INT, "0"),  # XXX Remove this! Set to 0
            "width": STRING,
            # "keepmaxwidth":         SIZE,
            # "keepmaxheight":        SIZE,
            # "keepmergespace":       (INT, 1),
            # "keepmode":             (["error", "overflow", "shrink", "truncate"], "shrink"),
        },
    ),
    "tr": (
        1,
        {
            "bgcolor": COLOR,
            "valign": ["top", "bottom", "middle"],
            "border": SIZE,
            "bordercolor": (COLOR, "#000000"),
        },
    ),
    "td": (
        1,
        {
            "align": ["left", "center", "right", "justify"],
            "valign": ["top", "bottom", "middle"],
            "width": STRING,
            "bgcolor": COLOR,
            "border": SIZE,
            "bordercolor": (COLOR, "#000000"),
            "colspan": INT,
            "rowspan": INT,
        },
    ),
    "th": (
        1,
        {
            "align": ["left", "center", "right", "justify"],
            "valign": ["top", "bottom", "middle"],
            "width": STRING,
            "bgcolor": COLOR,
            "border": SIZE,
            "bordercolor": (COLOR, "#000000"),
            "colspan": INT,
            "rowspan": INT,
        },
    ),
    "dl": (1, {}),
    "dd": (1, {}),
    "dt": (1, {}),
    "ol": (1, {"type": (["1", "a", "A", "i", "I"], "1"), "start": INT}),
    "ul": (1, {"type": (["circle", "disk", "square"], "disk")}),
    "li": (1, {}),
    "hr": (
        0,
        {
            "color": (COLOR, "#000000"),
            "size": (SIZE, "1"),
            "width": STRING,
            "align": ["left", "center", "right", "justify"],
        },
    ),
    "div": (
        1,
        {"align": ["left", "center", "right", "justify"], "dir": ["ltr", "rtl"]},
    ),
    "p": (1, {"align": ["left", "center", "right", "justify"], "dir": ["ltr", "rtl"]}),
    "body": (1, {"dir": ["ltr", "rtl"]}),
    "br": (0, {}),
    "h1": (
        1,
        {
            "outline": STRING,
            "closed": (INT, 0),
            "align": ["left", "center", "right", "justify"],
        },
    ),
    "h2": (
        1,
        {
            "outline": STRING,
            "closed": (INT, 0),
            "align": ["left", "center", "right", "justify"],
        },
    ),
    "h3": (
        1,
        {
            "outline": STRING,
            "closed": (INT, 0),
            "align": ["left", "center", "right", "justify"],
        },
    ),
    "h4": (
        1,
        {
            "outline": STRING,
            "closed": (INT, 0),
            "align": ["left", "center", "right", "justify"],
        },
    ),
    "h5": (
        1,
        {
            "outline": STRING,
            "closed": (INT, 0),
            "align": ["left", "center", "right", "justify"],
        },
    ),
    "h6": (
        1,
        {
            "outline": STRING,
            "closed": (INT, 0),
            "align": ["left", "center", "right", "justify"],
        },
    ),
    "font": (1, {"face": FONT, "color": COLOR, "size": STRING}),
    "a": (1, {"href": STRING, "name": STRING}),
    "input": (
        0,
        {
            "name": STRING,
            "value": STRING,
            "type": (["text", "hidden", "checkbox"], "text"),
        },
    ),
    "textarea": (1, {"name": STRING, "cols": (SIZE, 40), "rows": (SIZE, 1)}),
    "select": (1, {"name": STRING, "value": STRING}),
    "option": (0, {"value": STRING}),
}

# XXX use "html" not "*" as default!
DEFAULT_CSS = """
html {
    font-family: Helvetica;
    font-size: 10px;
    font-weight: normal;
    color: #000000;
    margin: 0;
    padding: 0;
    line-height: 150%;
    border: 1px none;
    display: inline;
    width: auto;
    height: auto;
    white-space: normal;
}

b,
strong {
    font-weight: bold;
}

i,
em {
    font-style: italic;
}

u {
    text-decoration: underline;
}

s,
strike {
    text-decoration: line-through;
}

a {
    text-decoration: underline;
    color: blue;
}

ins {
    color: green;
    text-decoration: underline;
}
del {
    color: red;
    text-decoration: line-through;
}

pre,
code,
kbd,
samp,
tt {
    font-family: "Courier New";
}

h1,
h2,
h3,
h4,
h5,
h6 {
    font-weight:bold;
    -pdf-outline: true;
    -pdf-outline-open: false;
}

h1 {
    /*18px via YUI Fonts CSS foundation*/
    font-size:138.5%;
    -pdf-outline-level: 0;
}

h2 {
    /*16px via YUI Fonts CSS foundation*/
    font-size:123.1%;
    -pdf-outline-level: 1;
}

h3 {
    /*14px via YUI Fonts CSS foundation*/
    font-size:108%;
    -pdf-outline-level: 2;
}

h4 {
    -pdf-outline-level: 3;
}

h5 {
    -pdf-outline-level: 4;
}

h6 {
    -pdf-outline-level: 5;
}

h1,
h2,
h3,
h4,
h5,
h6,
p,
pre,
hr {
    margin:1em 0;
}

address,
blockquote,
body,
center,
dl,
dir,
div,
fieldset,
form,
h1,
h2,
h3,
h4,
h5,
h6,
hr,
isindex,
menu,
noframes,
noscript,
ol,
p,
pre,
table,
th,
tr,
td,
ul,
li,
dd,
dt,
pdftoc {
    display: block;
}

table {
}

tr,
th,
td {

    vertical-align: middle;
    width: auto;
}

th {
    text-align: center;
    font-weight: bold;
}

center {
    text-align: center;
}

big {
    font-size: 125%;
}

small {
    font-size: 75%;
}


ul {
    margin-left: 1.5em;
    list-style-type: disc;
}

ul ul {
    list-style-type: circle;
}

ul ul ul {
    list-style-type: square;
}

ol {
    list-style-type: decimal;
    margin-left: 1.5em;
}

ul li div:first-child {
    display: inline-block;
}

pre {
    white-space: pre;
}

blockquote {
    margin-left: 1.5em;
    margin-right: 1.5em;
}

noscript {
    display: none;
}
"""

DEFAULT_LANGUAGE_LIST = {
    "arabic": "arabic",
    "hebrew": "hebrew",
    "persian": "persian",
    "urdu": "urdu",
    "pashto": "pashto",
    "sindhi": "sindhi",
}

DEFAULT_FONT = {
    "courier": "Courier",
    "courier-bold": "Courier-Bold",
    "courier-boldoblique": "Courier-BoldOblique",
    "courier-oblique": "Courier-Oblique",
    "helvetica": "Helvetica",
    "helvetica-bold": "Helvetica-Bold",
    "helvetica-boldoblique": "Helvetica-BoldOblique",
    "helvetica-oblique": "Helvetica-Oblique",
    "times": "Times-Roman",
    "times-roman": "Times-Roman",
    "times-bold": "Times-Bold",
    "times-boldoblique": "Times-BoldOblique",
    "times-oblique": "Times-Oblique",
    "symbol": "Symbol",
    "zapfdingbats": "ZapfDingbats",
    "zapf-dingbats": "ZapfDingbats",
    # Alias
    "arial": "Helvetica",
    "times new roman": "Times-Roman",
    "georgia": "Times-Roman",
    "serif": "Times-Roman",
    "sansserif": "Helvetica",
    "sans": "Helvetica",
    "monospaced": "Courier",
    "monospace": "Courier",
    "mono": "Courier",
    "courier new": "Courier",
    "verdana": "Helvetica",
    "geneva": "Helvetica",
}

PML_PAGESIZES = {
    "a0": A0,
    "a1": A1,
    "a2": A2,
    "a3": A3,
    "a4": A4,
    "a5": A5,
    "a6": A6,
    "a7": A7,
    "a8": A8,
    "a9": A9,
    "a10": A10,
    "b0": B0,
    "b1": B1,
    "b2": B2,
    "b3": B3,
    "b4": B4,
    "b5": B5,
    "b6": B6,
    "b7": B7,
    "b8": B8,
    "b9": B9,
    "b10": B10,
    "c0": C0,
    "c1": C1,
    "c2": C2,
    "c3": C3,
    "c4": C4,
    "c5": C5,
    "c6": C6,
    "c7": C7,
    "c8": C8,
    "c9": C9,
    "c10": C10,
    "letter": LETTER,
    "legal": LEGAL,
    "ledger": LEDGER,
    "elevenseventeen": ELEVENSEVENTEEN,
    "juniorlegal": JUNIOR_LEGAL,
    "halfletter": HALF_LETTER,
    "govletter": GOV_LETTER,
    "govlegal": GOV_LEGAL,
    "tabloid": TABLOID,
}
