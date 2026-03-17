# Purpose: compiler for line type definitions
# Copyright (c) 2018-2021, Manfred Moitzi
# License: MIT License

# Auszug acadlt.lin
#
# *RAND,Rand __ __ . __ __ . __ __ . __ __ . __ __ .
# A,.5,-.25,.5,-.25,0,-.25
# *RAND2,Rand (.5x) __.__.__.__.__.__.__.__.__.__.__.
# A,.25,-.125,.25,-.125,0,-.125
# *RANDX2,Rand (2x) ____  ____  .  ____  ____  .  ___
# A,1.0,-.5,1.0,-.5,0,-.5
#
# *MITTE,Mitte ____ _ ____ _ ____ _ ____ _ ____ _ ____
# A,1.25,-.25,.25,-.25
# *CENTER2,Mitte (.5x) ___ _ ___ _ ___ _ ___ _ ___ _ ___
# A,.75,-.125,.125,-.125
# *MITTEX2,Mitte (2x) ________  __  ________  __  _____
# A,2.5,-.5,.5,-.5
#
# ;;  Komplexe Linientypen
# ;;
# ;;  Dieser Datei sind komplexe Linientypen hinzugefügt worden.
# ;;  Diese Linientypen wurden in LTYPESHP.LIN in
# ;;  Release 13 definiert und wurden in ACAD.LIN in
# ;;  Release 14 aufgenommen.
# ;;
# ;;  Diese Linientypdefinitionen verwenden LTYPESHP.SHX.
# ;;
# *GRENZE1,Grenze rund ----0-----0----0-----0----0-----0--
# A,.25,-.1,[CIRC1,ltypeshp.shx,x=-.1,s=.1],-.1,1
# *GRENZE2,Grenze eckig ----[]-----[]----[]-----[]----[]---
# A,.25,-.1,[BOX,ltypeshp.shx,x=-.1,s=.1],-.1,1
# *EISENBAHN,Eisenbahn -|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-
# A,.15,[TRACK1,ltypeshp.shx,s=.25],.15
# *ISOLATION,Isolation SSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS
# A,.0001,-.1,[BAT,ltypeshp.shx,x=-.1,s=.1],-.2,[BAT,ltypeshp.shx,r=180,x=.1,s=.1],-.1
# *HEISSWASSERLEITUNG,Heißwasserleitung ---- HW ---- HW ---- HW ----
# A,.5,-.2,["HW",STANDARD,S=.1,U=0.0,X=-0.1,Y=-.05],-.2
# *GASLEITUNG,Gasleitung ----GAS----GAS----GAS----GAS----GAS----GAS--
# A,.5,-.2,["GAS",STANDARD,S=.1,U=0.0,X=-0.1,Y=-.05],-.25
# *ZICKZACK,Zickzack /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
# A,.0001,-.2,[ZIG,ltypeshp.shx,x=-.2,s=.2],-.4,[ZIG,ltypeshp.shx,r=180,x=.2,s=.2],-.2
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Sequence, Union, Any
from ezdxf.lldxf.const import DXFValueError, DXFTableEntryError
from ezdxf.lldxf.tags import DXFTag, Tags

if TYPE_CHECKING:  # import forward references
    from ezdxf.document import Drawing

Token = Union[str, float, list]


def lin_compiler(definition: str) -> Sequence[DXFTag]:
    """
    Compiles line type definitions like 'A,.5,-.25,.5,-.25,0,-.25' or
    'A,.5,-.2,["GAS",STANDARD,S=.1,U=0.0,X=-0.1,Y=-.05],-.25' into DXFTags().

    Args:
        definition: definition string

    Returns:
        list of DXFTag()
    """
    # 'A,.5,-.2,["GAS",STANDARD,S=.1,U=0.0,X=-0.1,Y=-.05],-.25'
    # ['A', .5, -.2, ['TEXT', 'GAS', 'STANDARD', 's', .1, 'u', 0.0, 'x', -.1, 'y', -.05], -.25]
    tags = []
    for token in lin_parser(definition):
        if token == "A":
            continue
        elif isinstance(token, float):
            tags.append(
                DXFTag(49, token)
            )  # Dash, dot or space length (one entry per element)
        elif isinstance(token, list):  # yield from
            tags.append(compile_complex_definition(token))  # type: ignore
    return tags


class ComplexLineTypePart:
    def __init__(self, type_: str, value, font: str = "STANDARD"):
        self.type = type_
        self.value = value
        self.font = font
        self.tags = Tags()

    def complex_ltype_tags(self, doc: "Drawing") -> Sequence[DXFTag]:
        def get_font_handle() -> str:
            if self.type == "SHAPE":
                # Create new shx or returns existing entry:
                font = doc.styles.get_shx(self.font)
            else:
                try:
                    # Case insensitive search for text style:
                    font = doc.styles.get(self.font)
                except DXFTableEntryError:
                    font = doc.styles.new(self.font)
            return font.dxf.handle

        # Note: AutoCAD/BricsCAD do NOT report an error or even crash, if the
        # text style handle is invalid!
        if doc is not None:
            handle = get_font_handle()
        else:
            handle = "0"
        tags = []
        if self.type == "TEXT":
            tags.append(DXFTag(74, 2))
            tags.append(DXFTag(75, 0))
        else:  # SHAPE
            tags.append(DXFTag(74, 4))
            tags.append(DXFTag(75, self.value))
        tags.append(DXFTag(340, handle))
        tags.extend(self.tags)
        if self.type == "TEXT":
            tags.append(DXFTag(9, self.value))
        return tags


CMD_CODES = {
    "s": 46,  # scaling factor
    "r": 50,  # rotation angle, r == u
    "u": 50,  # rotation angle
    "x": 44,  # shift x units = parallel to line direction
    "y": 45,  # shift y units = normal to line direction
}


def compile_complex_definition(tokens: Sequence) -> ComplexLineTypePart:
    part = ComplexLineTypePart(tokens[0], tokens[1], tokens[2])
    commands = list(reversed(tokens[3:]))
    params = {}
    while len(commands):
        cmd = commands.pop()
        value = commands.pop()
        code = CMD_CODES.get(cmd, 0)
        params[code] = DXFTag(code, value)

    for code in (46, 50, 44, 45):
        tag = params.get(code, DXFTag(code, 0.0))
        part.tags.append(tag)
    return part


def lin_parser(definition: str) -> Sequence[Token]:
    bag: list[Any] = []
    sublist = None
    first = True
    for token in lin_tokenizer(definition):
        if token == "A" and first:
            bag.append(token)
            first = False
            continue

        try:
            value = float(token)  # only outside of TEXT or SHAPE definition
            bag.append(value)
            continue
        except ValueError:
            pass

        if token.startswith("["):
            if sublist is not None:
                raise DXFValueError(
                    "Complex line type error. {}".format(definition)
                )
            sublist = []
            if token.startswith('["'):
                sublist.append("TEXT")
                sublist.append(
                    token[2:-1]
                )  # text without surrounding '["' and '"'
            else:
                sublist.append("SHAPE")
                try:
                    sublist.append(int(token[1:]))  # type: ignore # shape index! required
                except ValueError:
                    raise DXFValueError(
                        "Complex line type with shapes requires shape index not shape name!"
                    )
        else:
            _token = token.rstrip("]")
            subtokens = _token.split("=")
            if len(subtokens) == 2:
                sublist.append(subtokens[0].lower())
                sublist.append(float(subtokens[1]))  # type: ignore
            else:
                sublist.append(_token)
        if token.endswith("]"):
            if sublist is None:
                raise DXFValueError(
                    "Complex line type error. {}".format(definition)
                )
            bag.append(sublist)
            sublist = None  # type: ignore
    return bag


def lin_tokenizer(definition: str) -> Iterable[str]:
    token = ""
    escape = False
    for char in definition:
        if char == "," and not escape:
            yield token.strip()
            token = ""
            continue
        token += char
        if char == '"':
            escape = not escape
    if escape:
        raise DXFValueError("Line type parsing error: '{}'".format(definition))
    if token:
        yield token.strip()
