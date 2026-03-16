# Copyright ReportLab Europe Ltd. 2000-2008
# see license.txt for license details
# history http://www.reportlab.co.uk/cgi-bin/viewcvs.cgi/public/reportlab/trunk/reportlab/platypus/paragraph.py
# Modifications by Dirk Holtwick, 2008
from __future__ import annotations

import re
import sys
from copy import deepcopy
from operator import truth
from string import whitespace
from typing import Callable

from reportlab.graphics import renderPDF
from reportlab.lib.abag import ABag
from reportlab.lib.colors import Color
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.lib.textsplit import ALL_CANNOT_START
from reportlab.pdfbase.pdfmetrics import getAscentDescent, stringWidth
from reportlab.platypus.flowables import Flowable
from reportlab.platypus.paraparser import ParaParser
from reportlab.rl_settings import _FUZZ

from xhtml2pdf.util import getSize

PARAGRAPH_DEBUG = False
LEADING_FACTOR = 1.0

_wsc_re_split = re.compile(
    "[%s]+"
    % re.escape(
        "\t\n\x0b\x0c\r\x1c\x1d\x1e\x1f"
        " \x85\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u200b\u2028\u2029\u202f\u205f\u3000"
    )
).split


def split(text, delim=None):
    if isinstance(text, bytes):
        text = text.decode("utf8")
    if isinstance(delim, bytes):
        delim = delim.decode("utf8")
    elif delim is None and "\xa0" in text:
        return [uword.encode("utf8") for uword in _wsc_re_split(text)]
    return [uword.encode("utf8") for uword in text.split(delim)]


def strip(text):
    if isinstance(text, bytes):
        text = text.decode("utf8")
    return text.strip().encode("utf8")


class ParaLines(ABag):
    """
    class ParaLines contains the broken into lines representation of Paragraphs
    kind=0  Simple
    fontName, fontSize, textColor apply to whole Paragraph
    lines   [(extraSpace1,words1),....,(extraspaceN,wordsN)].

    kind==1 Complex
    lines   [FragLine1,...,FragLineN]
    """


class FragLine(ABag):
    """
    class FragLine contains a styled line (ie a line with more than one style)::

    extraSpace  unused space for justification only
    wordCount   1+spaces in line for justification purposes
    words       [ParaFrags] style text lumps to be concatenated together
    fontSize    maximum fontSize seen on the line; not used at present,
                but could be used for line spacing.
    """


# our one and only parser
# XXXXX if the parser has any internal state using only one is probably a BAD idea!
_parser = ParaParser()


def _lineClean(L):
    return b" ".join(filter(truth, split(strip(L))))


def cleanBlockQuotedText(text, joiner=b" "):
    """
    This is an internal utility which takes triple-
    quoted text form within the document and returns
    (hopefully) the paragraph the user intended originally.
    """
    L = filter(truth, map(_lineClean, split(text, "\n")))
    return joiner.join(L)


def setXPos(tx, dx):
    if dx > 1e-6 or dx < -1e-6:
        tx.setXPos(dx)


def _leftDrawParaLine(tx, offset, _extraspace, words, _last=0):
    setXPos(tx, offset)
    tx._textOut(b" ".join(words), 1)
    setXPos(tx, -offset)
    return offset


def _centerDrawParaLine(tx, offset, extraspace, words, _last=0):
    m = offset + 0.5 * extraspace
    setXPos(tx, m)
    tx._textOut(b" ".join(words), 1)
    setXPos(tx, -m)
    return m


def _rightDrawParaLine(tx, offset, extraspace, words, _last=0):
    m = offset + extraspace
    setXPos(tx, m)
    tx._textOut(b" ".join(words), 1)
    setXPos(tx, -m)
    return m


def _justifyDrawParaLine(tx, offset, extraspace, words, last=0):
    setXPos(tx, offset)
    text = b" ".join(words)
    if last:
        # last one, left align
        tx._textOut(text, 1)
    else:
        nSpaces = len(words) - 1
        if nSpaces:
            tx.setWordSpace(extraspace / float(nSpaces))
            tx._textOut(text, 1)
            tx.setWordSpace(0)
        else:
            tx._textOut(text, 1)
    setXPos(tx, -offset)
    return offset


def imgVRange(h, va, fontSize):
    """Return bottom,top offsets relative to baseline(0)."""
    if va == "baseline":
        iyo = 0
    elif va in {"text-top", "top"}:
        iyo = fontSize - h
    elif va == "middle":
        iyo = fontSize - (1.2 * fontSize + h) * 0.5
    elif va in {"text-bottom", "bottom"}:
        iyo = fontSize - 1.2 * fontSize
    elif va == "super":
        iyo = 0.5 * fontSize
    elif va == "sub":
        iyo = -0.5 * fontSize
    elif hasattr(va, "normalizedValue"):
        iyo = va.normalizedValue(fontSize)
    else:
        iyo = va
    return iyo, iyo + h


_56 = 5.0 / 6
_16 = 1.0 / 6


def _putFragLine(cur_x, tx, line):
    xs = tx.XtraState
    cur_y = xs.cur_y
    x0 = tx._x0
    autoLeading = xs.autoLeading
    leading = xs.leading
    cur_x += xs.leftIndent
    dal = autoLeading in {"min", "max"}
    if dal:
        if autoLeading == "max":
            ascent = max(_56 * leading, line.ascent)
            descent = max(_16 * leading, -line.descent)
        else:
            ascent = line.ascent
            descent = -line.descent
        leading = ascent + descent
    if tx._leading != leading:
        tx.setLeading(leading)
    if dal:
        olb = tx._olb
        if olb is not None:
            xcy = olb - ascent
            if tx._oleading != leading:
                cur_y += leading - tx._oleading
            if abs(xcy - cur_y) > 1e-8:
                cur_y = xcy
                tx.setTextOrigin(x0, cur_y)
                xs.cur_y = cur_y
        tx._olb = cur_y - descent
        tx._oleading = leading

    # Letter spacing
    if xs.style.letterSpacing != "normal":
        tx.setCharSpace(getSize("".join(xs.style.letterSpacing)))

    ws = getattr(tx, "_wordSpace", 0)
    nSpaces = 0
    words = line.words
    for f in words:
        if hasattr(f, "cbDefn"):
            cbDefn = f.cbDefn
            kind = cbDefn.kind
            if kind == "img":
                # draw image cbDefn,cur_y,cur_x
                w = cbDefn.width
                h = cbDefn.height
                txfs = tx._fontsize
                if txfs is None:
                    txfs = xs.style.fontSize
                iy0, iy1 = imgVRange(h, cbDefn.valign, txfs)
                cur_x_s = cur_x + nSpaces * ws
                drawing = cbDefn.image.getDrawing(w, h)
                if drawing:
                    renderPDF.draw(drawing, tx._canvas, cur_x_s, cur_y + iy0)
                else:
                    tx._canvas.drawImage(
                        cbDefn.image.getImage(), cur_x_s, cur_y + iy0, w, h, mask="auto"
                    )
                cur_x += w
                cur_x_s += w
                setXPos(tx, cur_x_s - tx._x0)
            elif kind == "barcode":
                barcode = cbDefn.barcode
                w = cbDefn.width
                h = cbDefn.height
                txfs = tx._fontsize
                if txfs is None:
                    txfs = xs.style.fontSize
                iy0, iy1 = imgVRange(h, cbDefn.valign, txfs)
                cur_x_s = cur_x + nSpaces * ws
                barcode.draw(canvas=tx._canvas, xoffset=cur_x_s)
                cur_x += w
                cur_x_s += w
                setXPos(tx, cur_x_s - tx._x0)
            else:
                name = cbDefn.name
                if kind == "anchor":
                    tx._canvas.bookmarkHorizontal(name, cur_x, cur_y + leading)
                else:
                    func = getattr(tx._canvas, name, None)
                    if not func:
                        msg = f"Missing {kind} callback attribute '{name}'"
                        raise AttributeError(msg)
                    func(tx._canvas, kind, cbDefn.label)
            if f is words[-1]:
                if not tx._fontname:
                    tx.setFont(xs.style.fontName, xs.style.fontSize)
                    tx._textOut("", 1)
                elif kind == "img":
                    tx._textOut("", 1)
        else:
            cur_x_s = cur_x + nSpaces * ws
            if (tx._fontname, tx._fontsize) != (f.fontName, f.fontSize):
                tx._setFont(f.fontName, f.fontSize)
            if xs.textColor != f.textColor:
                xs.textColor = f.textColor
                tx.setFillColor(f.textColor)
            if xs.rise != f.rise:
                xs.rise = f.rise
                tx.setRise(f.rise)
            text = f.text
            tx._textOut(text, f is words[-1])  # cheap textOut

            # XXX Modified for XHTML2PDF
            # Background colors (done like underline)
            if hasattr(f, "backColor") and (
                xs.backgroundColor != f.backColor or xs.backgroundFontSize != f.fontSize
            ):
                if xs.backgroundColor is not None:
                    xs.backgrounds.append(
                        (
                            xs.background_x,
                            cur_x_s,
                            xs.backgroundColor,
                            xs.backgroundFontSize,
                        )
                    )
                xs.background_x = cur_x_s
                xs.backgroundColor = f.backColor
                xs.backgroundFontSize = f.fontSize

            # Underline
            if not (hasattr(xs, "underline") and xs.underline) and (
                hasattr(f, "underline") and f.underline
            ):
                xs.underline = 1
                xs.underline_x = cur_x_s
                xs.underlineColor = f.textColor
            elif xs.underline:
                if not f.underline:
                    xs.underline = 0
                    xs.underlines.append((xs.underline_x, cur_x_s, xs.underlineColor))
                    xs.underlineColor = None
                elif xs.textColor != xs.underlineColor:
                    xs.underlines.append((xs.underline_x, cur_x_s, xs.underlineColor))
                    xs.underlineColor = xs.textColor
                    xs.underline_x = cur_x_s

            # Strike
            if not (hasattr(xs, "strike") and xs.strike) and (
                hasattr(f, "strike") and f.strike
            ):
                xs.strike = 1
                xs.strike_x = cur_x_s
                xs.strikeColor = f.textColor
                # XXX Modified for XHTML2PDF
                xs.strikeFontSize = f.fontSize
            elif xs.strike:
                if not f.strike:
                    xs.strike = 0
                    # XXX Modified for XHTML2PDF
                    xs.strikes.append(
                        (xs.strike_x, cur_x_s, xs.strikeColor, xs.strikeFontSize)
                    )
                    xs.strikeColor = None
                    xs.strikeFontSize = None
                elif xs.textColor != xs.strikeColor:
                    xs.strikes.append(
                        (xs.strike_x, cur_x_s, xs.strikeColor, xs.strikeFontSize)
                    )
                    xs.strikeColor = xs.textColor
                    xs.strikeFontSize = f.fontSize
                    xs.strike_x = cur_x_s
            if f.link and not xs.link:
                if not xs.link:
                    xs.link = f.link
                    xs.link_x = cur_x_s
                    xs.linkColor = xs.textColor
            elif xs.link:
                if not f.link:
                    xs.links.append((xs.link_x, cur_x_s, xs.link, xs.linkColor))
                    xs.link = None
                    xs.linkColor = None
                elif f.link != xs.link or xs.textColor != xs.linkColor:
                    xs.links.append((xs.link_x, cur_x_s, xs.link, xs.linkColor))
                    xs.link = f.link
                    xs.link_x = cur_x_s
                    xs.linkColor = xs.textColor
            txtlen = tx._canvas.stringWidth(text, tx._fontname, tx._fontsize)
            cur_x += txtlen
            try:
                nSpaces += text.count(" ")
            except Exception:
                nSpaces += text.decode("utf8").count(" ")
    cur_x_s = cur_x + (nSpaces - 1) * ws

    # XXX Modified for XHTML2PDF
    # Underline
    if xs.underline:
        xs.underlines.append((xs.underline_x, cur_x_s, xs.underlineColor))

    # XXX Modified for XHTML2PDF
    # Backcolor
    if words and hasattr(f, "backColor") and xs.backgroundColor is not None:
        xs.backgrounds.append(
            (xs.background_x, cur_x_s, xs.backgroundColor, xs.backgroundFontSize)
        )

    # XXX Modified for XHTML2PDF
    # Strike
    if xs.strike:
        xs.strikes.append((xs.strike_x, cur_x_s, xs.strikeColor, xs.strikeFontSize))

    if xs.link:
        xs.links.append((xs.link_x, cur_x_s, xs.link, xs.linkColor))
    if tx._x0 != x0:
        setXPos(tx, x0 - tx._x0)


def _leftDrawParaLineX(tx, offset, line, _last=0):
    setXPos(tx, offset)
    _putFragLine(offset, tx, line)
    setXPos(tx, -offset)


def _centerDrawParaLineX(tx, offset, line, _last=0):
    m = offset + 0.5 * line.extraSpace
    setXPos(tx, m)
    _putFragLine(m, tx, line)
    setXPos(tx, -m)


def _rightDrawParaLineX(tx, offset, line, _last=0):
    m = offset + line.extraSpace
    setXPos(tx, m)
    _putFragLine(m, tx, line)
    setXPos(tx, -m)


def _justifyDrawParaLineX(tx, offset, line, last=0):
    setXPos(tx, offset)
    extraSpace = line.extraSpace
    nSpaces = line.wordCount - 1
    if (
        last
        or not nSpaces
        or abs(extraSpace) <= 1e-8
        or (hasattr(line, "lineBreak") and line.lineBreak)
    ):
        _putFragLine(offset, tx, line)  # no space modification
    else:
        tx.setWordSpace(extraSpace / float(nSpaces))
        _putFragLine(offset, tx, line)
        tx.setWordSpace(0)
    setXPos(tx, -offset)


def _sameFrag(f, g):
    """Returns 1 if two ParaFrags map out the same."""
    if (
        hasattr(f, "cbDefn")
        or hasattr(g, "cbDefn")
        or hasattr(f, "lineBreak")
        or hasattr(g, "lineBreak")
    ):
        return 0
    for a in (
        "fontName",
        "fontSize",
        "textColor",
        "backColor",
        "rise",
        "underline",
        "strike",
        "link",
    ):
        if getattr(f, a, None) != getattr(g, a, None):
            return 0
    return 1


def reverse_sentence(sentence):
    words = str(sentence).split(" ")
    reverse_sentence = " ".join(reversed(words))
    return reverse_sentence[::-1]


def _getFragWords(frags, *, reverse=False):
    """
    Given a Parafrag list return a list of fragwords
    [[size, (f00,w00), ..., (f0n,w0n)],....,[size, (fm0,wm0), ..., (f0n,wmn)]]
    each pair f,w represents a style and some string
    each sublist represents a word.
    """
    R = []
    W = []
    n = 0
    hangingStrip = False
    for f in frags:
        text = f.text
        if isinstance(text, bytes):
            text = text.decode("utf8")
        if reverse:
            text = reverse_sentence(text)

        # of paragraphs
        if text:
            if hangingStrip:
                hangingStrip = False
                text = text.lstrip()
            S = split(text)
            if reverse:
                S.reverse()
            if [] == S:
                S = [""]
            if [] != W and text[0] in whitespace:
                W.insert(0, n)
                R.append(W)
                W = []
                n = 0

            for w in S[:-1]:
                W.append((f, w))
                n += stringWidth(w, f.fontName, f.fontSize)
                W.insert(0, n)
                R.append(W)
                W = []
                n = 0

            w = S[-1]
            W.append((f, w))
            n += stringWidth(w, f.fontName, f.fontSize)
            if text and text[-1] in whitespace:
                W.insert(0, n)
                R.append(W)
                W = []
                n = 0
        elif hasattr(f, "cbDefn"):
            w = getattr(f.cbDefn, "width", 0)
            if w:
                if [] != W:
                    W.insert(0, n)
                    R.append(W)
                    W = []
                    n = 0
                R.append([w, (f, "")])
            else:
                W.append((f, ""))
        elif hasattr(f, "lineBreak"):
            # pass the frag through.  The line breaker will scan for it.
            if [] != W:
                W.insert(0, n)
                R.append(W)
                W = []
                n = 0
            R.append([0, (f, "")])
            hangingStrip = True

    if [] != W:
        W.insert(0, n)
        R.append(W)

    return R


def _split_blParaSimple(blPara, start: int, stop: int) -> list:
    f = blPara.clone()
    for a in ("lines", "kind", "text"):
        if hasattr(f, a):
            delattr(f, a)

    f.words = []
    for line in blPara.lines[start:stop]:
        for w in line[1]:
            f.words.append(w)
    return [f]


def _split_blParaHard(blPara, start: int, stop: int) -> list:
    f = []
    lines = blPara.lines[start:stop]
    for line in lines:
        for w in line.words:
            f.append(w)  # noqa: PERF402
        if line is not lines[-1]:
            i = len(f) - 1
            while (
                i >= 0
                and hasattr(f[i], "cbDefn")
                and not getattr(f[i].cbDefn, "width", 0)
            ):
                i -= 1
            if i >= 0:
                g = f[i]
                if not g.text:
                    g.text = " "
                else:
                    if isinstance(g.text, bytes):
                        g.text = g.text.decode("utf8")
                    if g.text[-1] != " ":
                        g.text += " "
    return f


def _drawBullet(canvas, offset, cur_y, bulletText, style):
    """Draw a bullet text could be a simple string or a frag list."""
    tx2 = canvas.beginText(
        style.bulletIndent, cur_y + getattr(style, "bulletOffsetY", 0)
    )
    tx2.setFont(style.bulletFontName, style.bulletFontSize)
    tx2.setFillColor(
        hasattr(style, "bulletColor") and style.bulletColor or style.textColor
    )
    if isinstance(bulletText, str):
        tx2.textOut(bulletText)
    else:
        for f in bulletText:
            if hasattr(f, "image"):
                image = f.image
                width = image.drawWidth
                height = image.drawHeight
                gap = style.bulletFontSize * 0.25
                img = image.getImage()
                # print style.bulletIndent, offset, width
                canvas.drawImage(
                    img,
                    style.leftIndent - width - gap,
                    cur_y + getattr(style, "bulletOffsetY", 0),
                    width,
                    height,
                )
            else:
                tx2.setFont(f.fontName, f.fontSize)
                tx2.setFillColor(f.textColor)
                tx2.textOut(f.text)
    canvas.drawText(tx2)
    # AR making definition lists a bit less ugly
    # bulletEnd = tx2.getX()
    bulletEnd = tx2.getX() + style.bulletFontSize * 0.6
    return max(offset, bulletEnd - style.leftIndent)


def _handleBulletWidth(bulletText, style, maxWidths):
    """Work out bullet width and adjust maxWidths[0] if neccessary."""
    if bulletText:
        if isinstance(bulletText, str):
            bulletWidth = stringWidth(
                bulletText, style.bulletFontName, style.bulletFontSize
            )
        else:
            # it's a list of fragments
            bulletWidth = 0
            for f in bulletText:
                bulletWidth += stringWidth(f.text, f.fontName, f.fontSize)
        bulletRight = style.bulletIndent + bulletWidth + 0.6 * style.bulletFontSize
        indent = style.leftIndent + style.firstLineIndent
        if bulletRight > indent:
            # ..then it overruns, and we have less space available on line 1
            maxWidths[0] -= bulletRight - indent


def splitLines0(frags, widths):
    """
    Given a list of ParaFrags we return a list of ParaLines.

    each ParaLine has
    1)  ExtraSpace
    2)  blankCount
    3)  [textDefns....]
        each text definition is a (ParaFrag, start, limit) triplet
    """
    # initialise the algorithm
    lineNum = 0
    maxW = widths[lineNum]
    i = -1
    line = len(frags)
    lim = start = 0
    text = frags[0]
    while 1:
        # find a non whitespace character
        while i < line:
            while start < lim and text[start] == " ":
                start += 1
            if start == lim:
                i += 1
                if i == line:
                    break
                start = 0
                f = frags[i]
                text = f.text
                lim = len(text)
            else:
                break  # we found one

        if start == lim:
            break  # if we didn't find one we are done

        # start of a line
        g = (None, None, None)
        line = []
        cLen = 0
        nSpaces = 0
        while cLen < maxW:
            j = text.find(" ", start)
            if j < 0:
                j = lim
            w = stringWidth(text[start:j], f.fontName, f.fontSize)
            cLen += w
            if cLen > maxW and line != []:
                cLen -= w
                # this is the end of the line
                while g.text[lim] == " ":
                    lim -= 1
                    nSpaces -= 1
                break
            if j < 0:
                j = lim
            if g[0] is f:
                g[2] = j  # extend
            else:
                g = (f, start, j)
                line.append(g)
            if j == lim:
                i += 1


def _do_under_line(i, t_off, ws, tx, lm=-0.125):
    y = (
        tx.XtraState.cur_y
        - i * tx.XtraState.style.leading
        + lm * tx.XtraState.f.fontSize
    )
    textlen = tx._canvas.stringWidth(
        b" ".join(tx.XtraState.lines[i][1]), tx._fontname, tx._fontsize
    )
    tx._canvas.line(t_off, y, t_off + textlen + ws, y)


_scheme_re = re.compile("^[a-zA-Z][-+a-zA-Z0-9]+$")


def _doLink(tx, link, rect):
    parts = link.split(":", 1)
    scheme = len(parts) == 2 and parts[0].lower() or ""
    if _scheme_re.match(scheme) and scheme != "document":
        kind = scheme.lower() == "pdf" and "GoToR" or "URI"
        if kind == "GoToR":
            link = parts[1]
        tx._canvas.linkURL(link, rect, relative=1, kind=kind)
    else:
        if link[0] == "#":
            link = link[1:]
            scheme = ""
        tx._canvas.linkRect(
            "", scheme != "document" and link or parts[1], rect, relative=1
        )


def _do_link_line(i, t_off, ws, tx):
    xs = tx.XtraState
    leading = xs.style.leading
    y = xs.cur_y - i * leading - xs.f.fontSize / 8.0  # 8.0 factor copied from para.py
    text = b" ".join(xs.lines[i][1])
    textlen = tx._canvas.stringWidth(text, tx._fontname, tx._fontsize)
    _doLink(tx, xs.link, (t_off, y, t_off + textlen + ws, y + leading))


# XXX Modified for XHTML2PDF
def _do_post_text(tx):
    """
    Try to find out what the variables mean:

    tx         A structure containing more information about paragraph ???

    leading    Height of lines
    ff         1/8 of the font size
    y0         The "baseline" postion ???
    y          1/8 below the baseline
    """
    xs = tx.XtraState
    leading = xs.style.leading
    autoLeading = xs.autoLeading
    f = xs.f
    if autoLeading == "max":
        # leading = max(leading, f.fontSize)
        leading = max(leading, LEADING_FACTOR * f.fontSize)
    elif autoLeading == "min":
        leading = LEADING_FACTOR * f.fontSize
    ff = 0.125 * f.fontSize
    y0 = xs.cur_y
    y = y0 - ff

    # Background
    for x1, x2, c, fs in xs.backgrounds:
        inlineFF = fs * 0.125
        gap = inlineFF * 1.25
        tx._canvas.setFillColor(c)
        tx._canvas.rect(x1, y - gap, x2 - x1, fs + 1, fill=1, stroke=0)
    xs.backgrounds = []
    xs.background = 0
    xs.backgroundColor = None
    xs.backgroundFontSize = None

    # Underline
    yUnderline = y0 - 1.5 * ff
    tx._canvas.setLineWidth(ff * 0.75)
    csc = None
    for x1, x2, c in xs.underlines:
        if c != csc:
            tx._canvas.setStrokeColor(c)
            csc = c
        tx._canvas.line(x1, yUnderline, x2, yUnderline)
    xs.underlines = []
    xs.underline = 0
    xs.underlineColor = None

    # Strike
    for x1, x2, c, fs in xs.strikes:
        inlineFF = fs * 0.125
        ys = y0 + 2 * inlineFF
        if c != csc:
            tx._canvas.setStrokeColor(c)
            csc = c
        tx._canvas.setLineWidth(inlineFF * 0.75)
        tx._canvas.line(x1, ys, x2, ys)
    xs.strikes = []
    xs.strike = 0
    xs.strikeColor = None

    yl = y + leading
    for x1, x2, link, _c in xs.links:
        # No automatic underlining for links, never!
        _doLink(tx, link, (x1, y, x2, yl))
    xs.links = []
    xs.link = None
    xs.linkColor = None
    xs.cur_y -= leading


def textTransformFrags(frags, style):
    tt = style.textTransform
    if tt:
        tt = tt.lower()
        if tt == "lowercase":
            tt = str.lower
        elif tt == "uppercase":
            tt = str.upper
        elif tt == "capitalize":
            tt = str.title
        elif tt == "none":
            return
        else:
            raise ValueError(
                "ParaStyle.textTransform value %r is invalid" % style.textTransform
            )
        n = len(frags)
        if n == 1:
            # single fragment the easy case
            frags[0].text = tt(frags[0].text.decode("utf8")).encode("utf8")
        elif tt is str.title:
            pb = True
            for f in frags:
                t = f.text
                if not t:
                    continue
                u = t.decode("utf8")
                if u.startswith(" ") or pb:
                    u = tt(u)
                else:
                    i = u.find(" ")
                    if i >= 0:
                        u = u[:i] + tt(u[i:])
                pb = u.endswith(" ")
                f.text = u.encode("utf8")
        else:
            for f in frags:
                t = f.text
                if not t:
                    continue
                f.text = tt(t.decode("utf8")).encode("utf8")


class cjkU(str):  # noqa: SLOT000
    """simple class to hold the frag corresponding to a str."""

    def __new__(cls, value, frag, _encoding):
        self = str.__new__(cls, value)
        self._frag = frag
        if hasattr(frag, "cbDefn"):
            w = getattr(frag.cbDefn, "width", 0)
            self._width = w
        else:
            self._width = stringWidth(value, frag.fontName, frag.fontSize)
        return self

    frag = property(lambda self: self._frag)
    width = property(lambda self: self._width)


def makeCJKParaLine(U, extraSpace, calcBounds):
    words = []
    CW = []
    f0 = FragLine()
    maxSize = maxAscent = minDescent = 0
    for u in U:
        f = u.frag
        fontSize = f.fontSize
        if calcBounds:
            cbDefn = getattr(f, "cbDefn", None)
            if getattr(cbDefn, "width", 0):
                descent, ascent = imgVRange(cbDefn.height, cbDefn.valign, fontSize)
            else:
                ascent, descent = getAscentDescent(f.fontName, fontSize)
        else:
            ascent, descent = getAscentDescent(f.fontName, fontSize)
        maxSize = max(maxSize, fontSize)
        maxAscent = max(maxAscent, ascent)
        minDescent = min(minDescent, descent)
        if not _sameFrag(f0, f):
            f0 = f0.clone()
            f0.text = "".join(CW)
            words.append(f0)
            CW = []
            f0 = f
        CW.append(u)
    if CW:
        f0 = f0.clone()
        f0.text = "".join(CW)
        words.append(f0)
    return FragLine(
        kind=1,
        extraSpace=extraSpace,
        wordCount=1,
        words=words[1:],
        fontSize=maxSize,
        ascent=maxAscent,
        descent=minDescent,
    )


def cjkFragSplit(frags, maxWidths, calcBounds, encoding="utf8"):
    """This attempts to be wordSplit for frags using the dumb algorithm."""
    U = []  # get a list of single glyphs with their widths etc etc
    for f in frags:
        text = f.text
        if not isinstance(text, str):
            text = text.decode(encoding)
        if text:
            U.extend([cjkU(t, f, encoding) for t in text])
        else:
            U.append(cjkU(text, f, encoding))

    lines = []
    widthUsed = lineStartPos = 0
    maxWidth = maxWidths[0]

    for i, u in enumerate(U):
        w = u.width
        widthUsed += w
        lineBreak = hasattr(u.frag, "lineBreak")
        endLine = (widthUsed > maxWidth + _FUZZ and widthUsed > 0) or lineBreak
        if endLine:
            if lineBreak:
                continue
            extraSpace = maxWidth - widthUsed + w
            # This is the most important of the Japanese typography rules.
            # if next character cannot start a line, wrap it up to this line so it hangs
            # in the right margin. We won't do two or more though - that's unlikely and
            # would result in growing ugliness.
            nextChar = U[i]
            if nextChar in ALL_CANNOT_START:
                extraSpace -= w
                i += 1
            lines.append(makeCJKParaLine(U[lineStartPos:i], extraSpace, calcBounds))
            try:
                maxWidth = maxWidths[len(lines)]
            except IndexError:
                maxWidth = maxWidths[-1]  # use the last one

            lineStartPos = i
            widthUsed = w
            i -= 1
        # any characters left?
    if widthUsed > 0:
        lines.append(
            makeCJKParaLine(U[lineStartPos:], maxWidth - widthUsed, calcBounds)
        )

    return ParaLines(kind=1, lines=lines)


class Paragraph(Flowable):
    """
    Paragraph(text, style, bulletText=None, caseSensitive=1)
    text a string of stuff to go into the paragraph.
    style is a style definition as in reportlab.lib.styles.
    bulletText is an optional bullet defintion.
    caseSensitive set this to 0 if you want the markup tags and their attributes to be case-insensitive.

    This class is a flowable that can format a block of text
    into a paragraph with a given style.

    The paragraph Text can contain XML-like markup including the tags:
    <b> ... </b> - bold
    <i> ... </i> - italics
    <u> ... </u> - underline
    <strike> ... </strike> - strike through
    <super> ... </super> - superscript
    <sub> ... </sub> - subscript
    <font name=fontfamily/fontname color=colorname size=float>
    <onDraw name=callable label="a label">
    <link>link text</link>
    attributes of links
    size/fontSize=num
    name/face/fontName=name
    fg/textColor/color=color
    backcolor/backColor/bgcolor=color
    dest/destination/target/href/link=target
    <a>anchor text</a>
    attributes of anchors
    fontSize=num
    fontName=name
    fg/textColor/color=color
    backcolor/backColor/bgcolor=color
    href=href
    <a name="anchorpoint"/>
    <unichar name="unicode character name"/>
    <unichar value="unicode code point"/>
    <img src="path" width="1in" height="1in" valign="bottom"/>

    The whole may be surrounded by <para> </para> tags

    The <b> and <i> tags will work for the built-in fonts (Helvetica
    /Times / Courier).  For other fonts you need to register a family
    of 4 fonts using reportlab.pdfbase.pdfmetrics.registerFont; then
    use the addMapping function to tell the library that these 4 fonts
    form a family e.g.
    from reportlab.lib.fonts import addMapping
    addMapping('Vera', 0, 0, 'Vera')    #normal
    addMapping('Vera', 0, 1, 'Vera-Italic')    #italic
    addMapping('Vera', 1, 0, 'Vera-Bold')    #bold
    addMapping('Vera', 1, 1, 'Vera-BoldItalic')    #italic and bold

    It will also be able to handle any MathML specified Greek characters.
    """

    def __init__(
        self,
        text,
        style,
        bulletText=None,
        frags=None,
        caseSensitive=1,
        encoding="utf8",
        dir="ltr",  # noqa: A002
    ) -> None:
        self.dir = dir
        self.caseSensitive = caseSensitive
        self.encoding = encoding
        self._setup(text, style, bulletText, frags, cleanBlockQuotedText)

    def __repr__(self) -> str:
        n = type(self).__name__
        L = [n + "("]
        keys = self.__dict__.keys()
        for k in keys:
            v = getattr(self, k)
            rk = repr(k)
            rv = repr(v)
            rk = "  " + rk.replace("\n", "\n  ")
            rv = "    " + rk.replace("\n", "\n    ")
            L.extend((rk, rv))
        L.append(") #" + n)
        return "\n".join(L)

    def _setup(self, text, style, bulletText, frags, cleaner):
        if frags is None:
            text = cleaner(text)
            _parser.caseSensitive = self.caseSensitive
            style, frags, bulletTextFrags = _parser.parse(text, style)
            if frags is None:
                msg = "xml parser error ({}) in paragraph beginning\n'{}'".format(
                    _parser.errors[0], text[: min(30, len(text))]
                )
                raise ValueError(msg)
            textTransformFrags(frags, style)
            if bulletTextFrags:
                bulletText = bulletTextFrags

        # AR hack
        self.text = text
        self.frags = frags
        self.style = style
        self.bulletText = bulletText
        self.debug = PARAGRAPH_DEBUG  # turn this on to see a pretty one with all the margins etc.

    def wrap(self, availWidth, availHeight):
        if self.debug:
            print(id(self), "wrap")
            try:
                print(repr(self.getPlainText()[:80]))
            except Exception:
                print("???")

        # work out widths array for breaking
        self.width = availWidth
        style = self.style
        leftIndent = style.leftIndent
        first_line_width = (
            availWidth - (leftIndent + style.firstLineIndent) - style.rightIndent
        )
        later_widths = availWidth - leftIndent - style.rightIndent

        if style.wordWrap == "CJK":
            # use Asian text wrap algorithm to break characters
            blPara = self.breakLinesCJK([first_line_width, later_widths])
        else:
            blPara = self.breakLines([first_line_width, later_widths])
        self.blPara = blPara
        autoLeading = getattr(self, "autoLeading", getattr(style, "autoLeading", ""))
        leading = style.leading
        if blPara.kind == 1 and autoLeading not in {"", "off"}:
            height = 0
            if autoLeading == "max":
                for line in blPara.lines:
                    height += max(line.ascent - line.descent, leading)
            elif autoLeading == "min":
                for line in blPara.lines:
                    height += line.ascent - line.descent
            else:
                raise ValueError("invalid autoLeading value %r" % autoLeading)
        else:
            if autoLeading == "max":
                leading = max(leading, LEADING_FACTOR * style.fontSize)
            elif autoLeading == "min":
                leading = LEADING_FACTOR * style.fontSize
            height = len(blPara.lines) * leading
        self.height = height

        return self.width, height

    def minWidth(self):
        """Attempt to determine a minimum sensible width."""
        frags = self.frags
        nFrags = len(frags)
        if not nFrags:
            return 0
        if nFrags == 1:
            f = frags[0]
            fS = f.fontSize
            fN = f.fontName
            words = hasattr(f, "text") and split(f.text, " ") or f.words

            def func(w: list, fS=fS, fN=fN) -> int:
                return stringWidth(w, fN, fS)

        else:
            words = _getFragWords(frags)

            def func(w: list, fS=None, fN=None) -> int:  # noqa: ARG001
                return w[0]

        return max(map(func, words))

    def _get_split_blParaFunc(self) -> Callable:
        return _split_blParaSimple if self.blPara.kind == 0 else _split_blParaHard

    def split(self, availWidth, availHeight):
        if self.debug:
            print(id(self), "split")

        if len(self.frags) <= 0:
            return []

        # the split information is all inside self.blPara
        if not hasattr(self, "blPara"):
            self.wrap(availWidth, availHeight)

        blPara = self.blPara
        style = self.style
        autoLeading = getattr(self, "autoLeading", getattr(style, "autoLeading", ""))
        leading = style.leading
        lines = blPara.lines
        if blPara.kind == 1 and autoLeading not in {"", "off"}:
            s = height = 0
            if autoLeading == "max":
                for i, line in enumerate(blPara.lines):
                    h = max(line.ascent - line.descent, leading)
                    n = height + h
                    if n > availHeight + 1e-8:
                        break
                    height = n
                    s = i + 1
            elif autoLeading == "min":
                for i, line in enumerate(blPara.lines):
                    n = height + line.ascent - line.descent
                    if n > availHeight + 1e-8:
                        break
                    height = n
                    s = i + 1
            else:
                raise ValueError("invalid autoLeading value %r" % autoLeading)
        else:
            line = leading
            if autoLeading == "max":
                line = max(leading, LEADING_FACTOR * style.fontSize)
            elif autoLeading == "min":
                line = LEADING_FACTOR * style.fontSize
            s = int(availHeight / line)
            height = s * line

        n = len(lines)
        allowWidows = getattr(self, "allowWidows", getattr(self, "allowWidows", 1))
        allowOrphans = getattr(self, "allowOrphans", getattr(self, "allowOrphans", 0))
        if not allowOrphans and s <= 1:  # orphan?
            del self.blPara
            return []
        if n <= s:
            return [self]
        if not allowWidows and n == s + 1:  # widow?
            if (allowOrphans and n == 3) or n > 3:
                s -= 1  # give the widow some company
            else:
                del self.blPara  # no room for adjustment; force the whole para onwards
                return []
        func = self._get_split_blParaFunc()

        P1 = type(self)(
            None, style, bulletText=self.bulletText, frags=func(blPara, 0, s)
        )
        # this is a major hack
        P1.blPara = ParaLines(
            kind=1, lines=blPara.lines[0:s], aH=availHeight, aW=availWidth
        )
        P1._JustifyLast = 1
        P1._splitpara = 1
        P1.height = height
        P1.width = availWidth
        if style.firstLineIndent != 0:
            style = deepcopy(style)
            style.firstLineIndent = 0
        P2 = type(self)(None, style, bulletText=None, frags=func(blPara, s, n))
        for a in (
            "autoLeading",  # possible attributes that might be directly on self.
        ):
            if hasattr(self, a):
                setattr(P1, a, getattr(self, a))
                setattr(P2, a, getattr(self, a))
        return [P1, P2]

    def draw(self):
        # call another method for historical reasons.  Besides, I
        # suspect I will be playing with alternate drawing routines
        # so not doing it here makes it easier to switch.
        self.drawPara(self.debug)

    def breakLines(self, width):
        """
        Returns a broken line structure. There are two cases.

        A) For the simple case of a single formatting input fragment the output is
            A fragment specifier with
                - kind = 0
                - fontName, fontSize, leading, textColor
                - lines=  A list of lines

                        Each line has two items.

                        1. unused width in points
                        2. word list

        B) When there is more than one input formatting fragment the output is
            A fragment specifier with
               - kind = 1
               - lines=  A list of fragments each having fields
                            - extraspace (needed for justified)
                            - fontSize
                            - words=word list
                                each word is itself a fragment with
                                various settings

        This structure can be used to easily draw paragraphs with the various alignments.
        You can supply either a single width or a list of widths; the latter will have its
        last item repeated until necessary. A 2-element list is useful when there is a
        different first line indent; a longer list could be created to facilitate custom wraps
        around irregular objects.
        """
        if self.debug:
            print(id(self), "breakLines")

        maxWidths = [width] if not isinstance(width, (tuple, list)) else width
        lines = []
        lineno = 0
        style = self.style

        # for bullets, work out width and ensure we wrap the right amount onto line one
        _handleBulletWidth(self.bulletText, style, maxWidths)

        maxWidth = maxWidths[0]

        self.height = 0
        autoLeading = getattr(self, "autoLeading", getattr(style, "autoLeading", ""))
        calcBounds = autoLeading not in {"", "off"}
        frags = self.frags
        nFrags = len(frags)
        if nFrags == 1 and not hasattr(frags[0], "cbDefn"):
            f = frags[0]
            fontSize = f.fontSize
            fontName = f.fontName
            ascent, descent = getAscentDescent(fontName, fontSize)
            words = hasattr(f, "text") and split(f.text, " ") or f.words
            spaceWidth = stringWidth(" ", fontName, fontSize, self.encoding)
            cLine = []
            currentWidth = -spaceWidth  # hack to get around extra space for word 1
            for word in words:
                # this underscores my feeling that Unicode throughout would be easier!
                wordWidth = stringWidth(word, fontName, fontSize, self.encoding)
                newWidth = currentWidth + spaceWidth + wordWidth
                if newWidth <= maxWidth or not len(cLine):
                    # fit one more on this line
                    cLine.append(word)
                    currentWidth = newWidth
                else:
                    self.width = max(currentWidth, self.width)
                    # end of line
                    lines.append((maxWidth - currentWidth, cLine))
                    cLine = [word]
                    currentWidth = wordWidth
                    lineno += 1
                    try:
                        maxWidth = maxWidths[lineno]
                    except IndexError:
                        maxWidth = maxWidths[-1]  # use the last one

            # deal with any leftovers on the final line
            if cLine != []:
                self.width = max(currentWidth, self.width)
                lines.append((maxWidth - currentWidth, cLine))

            return f.clone(
                kind=0, lines=lines, ascent=ascent, descent=descent, fontSize=fontSize
            )
        if nFrags <= 0:
            return ParaLines(
                kind=0,
                fontSize=style.fontSize,
                fontName=style.fontName,
                textColor=style.textColor,
                ascent=style.fontSize,
                descent=-0.2 * style.fontSize,
                lines=[],
            )
        if hasattr(self, "blPara") and getattr(self, "_splitpara", 0):
            # NB this is an utter hack that awaits the proper information
            # preserving splitting algorithm
            return self.blPara
        n = 0
        words = []
        frag_words = _getFragWords(frags, reverse=self.dir == "rtl")
        if self.dir == "rtl":
            frag_words.reverse()
        for w in frag_words:
            f = w[-1][0]
            fontName = f.fontName
            fontSize = f.fontSize
            spaceWidth = stringWidth(" ", fontName, fontSize)

            if not words:
                currentWidth = -spaceWidth  # hack to get around extra space for word 1
                maxSize = fontSize
                maxAscent, minDescent = getAscentDescent(fontName, fontSize)

            wordWidth = w[0]
            f = w[1][0]
            if wordWidth > 0:
                newWidth = currentWidth + spaceWidth + wordWidth
            else:
                newWidth = currentWidth

            # test to see if this frag is a line break. If it is we will only act on it
            # if the current width is non-negative or the previous thing was a deliberate lineBreak
            lineBreak = hasattr(f, "lineBreak")
            endLine = (newWidth > maxWidth and n > 0) or lineBreak
            if not endLine:
                if lineBreak:
                    continue  # throw it away
                nText = w[1][1] if isinstance(w[1][1], str) else str(w[1][1], "utf-8")

                if nText:
                    n += 1
                fontSize = f.fontSize
                if calcBounds:
                    cbDefn = getattr(f, "cbDefn", None)
                    if getattr(cbDefn, "width", 0):
                        descent, ascent = imgVRange(
                            cbDefn.height, cbDefn.valign, fontSize
                        )
                    else:
                        ascent, descent = getAscentDescent(f.fontName, fontSize)
                else:
                    ascent, descent = getAscentDescent(f.fontName, fontSize)
                maxSize = max(maxSize, fontSize)
                maxAscent = max(maxAscent, ascent)
                minDescent = min(minDescent, descent)
                if not words:
                    g = f.clone()
                    words = [g]
                    g.text = nText
                elif not _sameFrag(g, f):
                    if currentWidth > 0 and (
                        (nText and not nText.startswith(" ")) or hasattr(f, "cbDefn")
                    ):
                        if hasattr(g, "cbDefn"):
                            i = len(words) - 1
                            while i >= 0:
                                wi = words[i]
                                cbDefn = getattr(wi, "cbDefn", None)
                                if cbDefn and not getattr(cbDefn, "width", 0):
                                    i -= 1
                                    continue
                                if not wi.text.endswith(" "):
                                    wi.text += " "
                                break
                        else:
                            space = " " if isinstance(g.text, str) else b" "
                            if not g.text.endswith(space):
                                g.text += space
                    g = f.clone()
                    words.append(g)
                    g.text = nText
                else:
                    if isinstance(g.text, bytes):
                        g.text = g.text.decode("utf8")
                    if isinstance(nText, bytes):
                        nText = nText.decode("utf8")
                    if nText and not nText.startswith(" "):
                        g.text += " " + nText

                for i in w[2:]:
                    g = i[0].clone()
                    g.text = i[1]
                    words.append(g)
                    fontSize = g.fontSize
                    if calcBounds:
                        cbDefn = getattr(g, "cbDefn", None)
                        if getattr(cbDefn, "width", 0):
                            descent, ascent = imgVRange(
                                cbDefn.height, cbDefn.valign, fontSize
                            )
                        else:
                            ascent, descent = getAscentDescent(g.fontName, fontSize)
                    else:
                        ascent, descent = getAscentDescent(g.fontName, fontSize)
                    maxSize = max(maxSize, fontSize)
                    maxAscent = max(maxAscent, ascent)
                    minDescent = min(minDescent, descent)

                currentWidth = newWidth
            else:  # either it won't fit, or it's a lineBreak tag
                if lineBreak:
                    g = f.clone()
                    words.append(g)

                self.width = max(currentWidth, self.width)
                # end of line
                lines.append(
                    FragLine(
                        extraSpace=maxWidth - currentWidth,
                        wordCount=n,
                        lineBreak=lineBreak,
                        words=words,
                        fontSize=maxSize,
                        ascent=maxAscent,
                        descent=minDescent,
                    )
                )

                # start new line
                lineno += 1
                try:
                    maxWidth = maxWidths[lineno]
                except IndexError:
                    maxWidth = maxWidths[-1]  # use the last one

                if lineBreak:
                    n = 0
                    words = []
                    continue

                currentWidth = wordWidth
                n = 1
                g = f.clone()
                maxSize = g.fontSize
                if calcBounds:
                    cbDefn = getattr(g, "cbDefn", None)
                    if getattr(cbDefn, "width", 0):
                        minDescent, maxAscent = imgVRange(
                            cbDefn.height, cbDefn.valign, maxSize
                        )
                    else:
                        maxAscent, minDescent = getAscentDescent(g.fontName, maxSize)
                else:
                    maxAscent, minDescent = getAscentDescent(g.fontName, maxSize)
                words = [g]
                g.text = w[1][1]

                for i in w[2:]:
                    g = i[0].clone()
                    g.text = i[1]
                    words.append(g)
                    fontSize = g.fontSize
                    if calcBounds:
                        cbDefn = getattr(g, "cbDefn", None)
                        if getattr(cbDefn, "width", 0):
                            descent, ascent = imgVRange(
                                cbDefn.height, cbDefn.valign, fontSize
                            )
                        else:
                            ascent, descent = getAscentDescent(g.fontName, fontSize)
                    else:
                        ascent, descent = getAscentDescent(g.fontName, fontSize)
                    maxSize = max(maxSize, fontSize)
                    maxAscent = max(maxAscent, ascent)
                    minDescent = min(minDescent, descent)

        # deal with any leftovers on the final line
        if words != []:
            self.width = max(currentWidth, self.width)
            lines.append(
                ParaLines(
                    extraSpace=(maxWidth - currentWidth),
                    wordCount=n,
                    words=words,
                    fontSize=maxSize,
                    ascent=maxAscent,
                    descent=minDescent,
                )
            )
        return ParaLines(kind=1, lines=lines)

    def breakLinesCJK(self, width):
        """
        Initially, the dumbest possible wrapping algorithm.
        Cannot handle font variations.
        """
        if self.debug:
            print(id(self), "breakLinesCJK")

        maxWidths = width if isinstance(width, (list, tuple)) else [width]
        style = self.style

        # for bullets, work out width and ensure we wrap the right amount onto line one
        _handleBulletWidth(self.bulletText, style, maxWidths)
        if len(self.frags) > 1:
            autoLeading = getattr(
                self, "autoLeading", getattr(style, "autoLeading", "")
            )
            calcBounds = autoLeading not in {"", "off"}
            return cjkFragSplit(self.frags, maxWidths, calcBounds, self.encoding)

        if not len(self.frags):
            return ParaLines(
                kind=0,
                fontSize=style.fontSize,
                fontName=style.fontName,
                textColor=style.textColor,
                lines=[],
                ascent=style.fontSize,
                descent=-0.2 * style.fontSize,
            )
        f = self.frags[0]
        if 1 and hasattr(self, "blPara") and getattr(self, "_splitpara", 0):
            # NB this is an utter hack that awaits the proper information
            # preserving splitting algorithm
            return f.clone(kind=0, lines=self.blPara.lines)
        lines = []

        self.height = 0

        f = self.frags[0]

        text = f.text if hasattr(f, "text") else "".join(getattr(f, "words", []))

        from reportlab.lib.textsplit import wordSplit

        lines = wordSplit(text, maxWidths[0], f.fontName, f.fontSize)
        # the paragraph drawing routine assumes multiple frags per line, so we need an
        # extra list like this
        #  [space, [text]]
        #
        wrappedLines = [(sp, [line]) for (sp, line) in lines]
        return f.clone(
            kind=0, lines=wrappedLines, ascent=f.fontSize, descent=-0.2 * f.fontSize
        )

    def beginText(self, x, y):
        return self.canv.beginText(x, y)

    def drawPara(self, debug=0):
        """
        Draws a paragraph according to the given style.
        Returns the final y position at the bottom. Not safe for
        paragraphs without spaces e.g. Japanese; wrapping
        algorithm will go infinite.
        """
        if self.debug:
            print(id(self), "drawPara", self.blPara.kind)

        # stash the key facts locally for speed
        canvas = self.canv
        style = self.style
        blPara = self.blPara
        lines = blPara.lines
        leading = style.leading
        autoLeading = getattr(self, "autoLeading", getattr(style, "autoLeading", ""))

        # work out the origin for line 1
        leftIndent = style.leftIndent
        cur_x = leftIndent

        if debug:
            bw = 0.5
            bc = Color(1, 1, 0)
            bg = Color(0.9, 0.9, 0.9)
        else:
            bw = getattr(style, "borderWidth", None)
            bc = getattr(style, "borderColor", None)
            bg = style.backColor

        # if has a background or border, draw it
        if bg or (bc and bw):
            canvas.saveState()
            op = canvas.rect
            kwds = {"fill": 0, "stroke": 0}
            if bc and bw:
                canvas.setStrokeColor(bc)
                canvas.setLineWidth(bw)
                kwds["stroke"] = 1
                br = getattr(style, "borderRadius", 0)
                if br and not debug:
                    op = canvas.roundRect
                    kwds["radius"] = br
            if bg:
                canvas.setFillColor(bg)
                kwds["fill"] = 1
            bp = getattr(style, "borderPadding", 0)
            op(
                leftIndent - bp,
                -bp,
                self.width - (leftIndent + style.rightIndent) + 2 * bp,
                self.height + 2 * bp,
                **kwds,
            )
            canvas.restoreState()

        nLines = len(lines)
        bulletText = self.bulletText
        if nLines > 0:
            _offsets = getattr(self, "_offsets", [0])
            _offsets += (nLines - len(_offsets)) * [_offsets[-1]]
            canvas.saveState()
            alignment = style.alignment
            offset = style.firstLineIndent + _offsets[0]
            lim = nLines - 1
            noJustifyLast = not (hasattr(self, "_JustifyLast") and self._JustifyLast)

            if blPara.kind == 0:
                if alignment == TA_LEFT:
                    dpl = _leftDrawParaLine
                elif alignment == TA_CENTER:
                    dpl = _centerDrawParaLine
                elif self.style.alignment == TA_RIGHT:
                    dpl = _rightDrawParaLine
                elif self.style.alignment == TA_JUSTIFY:
                    dpl = _justifyDrawParaLine
                f = blPara
                cur_y = self.height - getattr(
                    f, "ascent", f.fontSize
                )  # TODO fix XPreformatted to remove this hack
                if bulletText:
                    offset = _drawBullet(canvas, offset, cur_y, bulletText, style)

                # set up the font etc.
                canvas.setFillColor(f.textColor)

                tx = self.beginText(cur_x, cur_y)
                if autoLeading == "max":
                    leading = max(leading, LEADING_FACTOR * f.fontSize)
                elif autoLeading == "min":
                    leading = LEADING_FACTOR * f.fontSize

                # now the font for the rest of the paragraph
                tx.setFont(f.fontName, f.fontSize, leading)
                ws = getattr(tx, "_wordSpace", 0)
                t_off = dpl(tx, offset, ws, lines[0][1], noJustifyLast and nLines == 1)
                if (
                    (hasattr(f, "underline") and f.underline)
                    or f.link
                    or (hasattr(f, "strike") and f.strike)
                ):
                    xs = tx.XtraState = ABag()
                    xs.cur_y = cur_y
                    xs.f = f
                    xs.style = style
                    xs.lines = lines
                    xs.underlines = []
                    xs.underlineColor = None
                    # XXX Modified for XHTML2PDF
                    xs.backgrounds = []
                    xs.backgroundColor = None
                    xs.backgroundFontSize = None
                    xs.strikes = []
                    xs.strikeColor = None
                    # XXX Modified for XHTML2PDF
                    xs.strikeFontSize = None
                    xs.links = []
                    xs.link = f.link
                    canvas.setStrokeColor(f.textColor)
                    dx = t_off + leftIndent
                    if dpl != _justifyDrawParaLine:
                        ws = 0
                    # XXX Never underline!
                    underline = f.underline
                    strike = f.strike
                    link = f.link
                    if underline:
                        _do_under_line(0, dx, ws, tx)
                    if strike:
                        _do_under_line(0, dx, ws, tx, lm=0.125)
                    if link:
                        _do_link_line(0, dx, ws, tx)

                    # now the middle of the paragraph, aligned with the left margin which is our origin.
                    for i in range(1, nLines):
                        ws = lines[i][0]
                        t_off = dpl(
                            tx, _offsets[i], ws, lines[i][1], noJustifyLast and i == lim
                        )
                        if dpl != _justifyDrawParaLine:
                            ws = 0
                        if underline:
                            _do_under_line(i, t_off + leftIndent, ws, tx)
                        if strike:
                            _do_under_line(i, t_off + leftIndent, ws, tx, lm=0.125)
                        if link:
                            _do_link_line(i, t_off + leftIndent, ws, tx)
                else:
                    for i in range(1, nLines):
                        dpl(
                            tx,
                            _offsets[i],
                            lines[i][0],
                            lines[i][1],
                            noJustifyLast and i == lim,
                        )
            else:
                f = lines[0]
                cur_y = self.height - getattr(
                    f, "ascent", f.fontSize
                )  # TODO fix XPreformatted to remove this hack
                # default?
                dpl = _leftDrawParaLineX
                if bulletText:
                    offset = _drawBullet(canvas, offset, cur_y, bulletText, style)
                if alignment == TA_LEFT:
                    dpl = _leftDrawParaLineX
                elif alignment == TA_CENTER:
                    dpl = _centerDrawParaLineX
                elif self.style.alignment == TA_RIGHT:
                    dpl = _rightDrawParaLineX
                elif self.style.alignment == TA_JUSTIFY:
                    dpl = _justifyDrawParaLineX
                else:
                    raise ValueError("bad align %s" % repr(alignment))

                # set up the font etc.
                tx = self.beginText(cur_x, cur_y)
                xs = tx.XtraState = ABag()
                xs.textColor = None
                # XXX Modified for XHTML2PDF
                xs.backColor = None
                xs.rise = 0
                xs.underline = 0
                xs.underlines = []
                xs.underlineColor = None
                # XXX Modified for XHTML2PDF
                xs.background = 0
                xs.backgrounds = []
                xs.backgroundColor = None
                xs.backgroundFontSize = None
                xs.strike = 0
                xs.strikes = []
                xs.strikeColor = None
                # XXX Modified for XHTML2PDF
                xs.strikeFontSize = None
                xs.links = []
                xs.link = None
                xs.leading = style.leading
                xs.leftIndent = leftIndent
                tx._leading = None
                tx._olb = None
                xs.cur_y = cur_y
                xs.f = f
                xs.style = style
                xs.autoLeading = autoLeading

                tx._fontname, tx._fontsize = None, None
                dpl(tx, offset, lines[0], noJustifyLast and nLines == 1)
                _do_post_text(tx)

                # now the middle of the paragraph, aligned with the left margin which is our origin.
                for i in range(1, nLines):
                    f = lines[i]
                    dpl(tx, _offsets[i], f, noJustifyLast and i == lim)
                    _do_post_text(tx)

            canvas.drawText(tx)
            canvas.restoreState()

    def getPlainText(self, identify=None):
        """
        Convenience function for templates which want access
        to the raw text, without XML tags.
        """
        frags = getattr(self, "frags", None)
        if frags:
            return "".join([frag.text] for frag in frags if hasattr(frag, "text"))
        if identify:
            text = getattr(self, "text", None)
            if text is None:
                text = repr(self)
            return text
        return ""

    def getActualLineWidths0(self):
        """
        Convenience function; tells you how wide each line
        actually is.  For justified styles, this will be
        the same as the wrap width; for others it might be
        useful for seeing if paragraphs will fit in spaces.
        """
        assert hasattr(self, "width"), "Cannot call this method before wrap()"
        if self.blPara.kind:

            def func(frag, w=self.width):
                return w - frag.extraSpace

        else:

            def func(frag, w=self.width):
                return w - frag[0]

        return map(func, self.blPara.lines)


if __name__ == "__main__":  # NORUNTESTS

    def dumpParagraphLines(P):
        print(f"dumpParagraphLines(<Paragraph @ {id(P)}>)")
        lines = P.blPara.lines
        for i, line in enumerate(lines):
            line = lines[i]
            words = line.words if hasattr(line, "words") else line[1]
            nwords = len(words)
            wordcount = getattr(line, "wordCount", "Unknown")
            print(f"line{i}: {nwords}({wordcount})\n  ")
            for w in range(nwords):
                text = getattr(words[w], "text", words[w])
                print(f"{w}:'{text}'")
            print()

    def fragDump(w):
        attrs = (
            "fontName",
            "fontSize",
            "textColor",
            "rise",
            "underline",
            "strike",
            "link",
            "cbDefn",
            "lineBreak",
        )
        attr_list = [f"'{w[1]}'"] + [
            f"{a}={getattr(w[0], a)!r}" for a in attrs if hasattr(w[0], a)
        ]
        return ", ".join(attr_list)

    def dumpParagraphFrags(P):
        print(
            f"dumpParagraphFrags(<Paragraph @ {id(P)}>) minWidth() = {P.minWidth():.2f}"
        )
        frags = P.frags
        n = len(frags)
        for frag in range(n):
            detail = " ".join(
                [
                    f"{k}={getattr(frags[frag], k)}"
                    for k in frags[frag].__dict__
                    if k != text
                ]
            )
            print(f"frag{frag}: '{frags[frag].text}' {detail}")

        fragword = 0
        cum = 0
        for W in _getFragWords(frags):
            cum += W[0]
            print(f"fragword{fragword}: cum={cum:3d} size={W[0]}")
            for w in W[1:]:
                print(f"({fragDump(w)})")
            print()
            fragword += 1

    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm

    TESTS = sys.argv[1:]
    if [] == TESTS:
        TESTS = ["4"]

    def flagged(i, TESTS=TESTS):
        return "all" in TESTS or "*" in TESTS or str(i) in TESTS

    styleSheet = getSampleStyleSheet()
    B = styleSheet["BodyText"]
    style = ParagraphStyle("discussiontext", parent=B)
    style.fontName = "Helvetica"
    if flagged(1):
        text = """The <font name=courier color=green>CMYK</font> or subtractive method follows the way a printer
mixes three pigments (cyan, magenta, and yellow) to form colors.
Because mixing chemicals is more difficult than combining light there
is a fourth parameter for darkness.  For example a chemical
combination of the <font name=courier color=green>CMY</font> pigments generally never makes a perfect
black -- instead producing a muddy color -- so, to get black printers
don't use the <font name=courier color=green>CMY</font> pigments but use a direct black ink.  Because
<font name=courier color=green>CMYK</font> maps more directly to the way printer hardware works it may
be the case that &amp;| &amp; | colors specified in <font name=courier color=green>CMYK</font> will provide better fidelity
and better control when printed.
"""
        P = Paragraph(text, style)
        dumpParagraphFrags(P)
        aW, aH = 456.0, 42.8
        w, h = P.wrap(aW, aH)
        dumpParagraphLines(P)
        S = P.split(aW, aH)
        for s in S:
            s.wrap(aW, aH)
            dumpParagraphLines(s)
            aH = 500

    if flagged(2):
        P = Paragraph(
            """Price<super><font color="red">*</font></super>""", styleSheet["Normal"]
        )
        dumpParagraphFrags(P)
        w, h = P.wrap(24, 200)
        dumpParagraphLines(P)

    if flagged(3):
        text = """Dieses Kapitel bietet eine schnelle <b><font color=red>Programme :: starten</font></b>
<onDraw name=myIndex label="Programme :: starten">
<b><font color=red>Eingabeaufforderung :: (&gt;&gt;&gt;)</font></b>
<onDraw name=myIndex label="Eingabeaufforderung :: (&gt;&gt;&gt;)">
<b><font color=red>&gt;&gt;&gt; (Eingabeaufforderung)</font></b>
<onDraw name=myIndex label="&gt;&gt;&gt; (Eingabeaufforderung)">
Einf&#xfc;hrung in Python <b><font color=red>Python :: Einf&#xfc;hrung</font></b>
<onDraw name=myIndex label="Python :: Einf&#xfc;hrung">.
Das Ziel ist, die grundlegenden Eigenschaften von Python darzustellen, ohne
sich zu sehr in speziellen Regeln oder Details zu verstricken. Dazu behandelt
dieses Kapitel kurz die wesentlichen Konzepte wie Variablen, Ausdr&#xfc;cke,
Kontrollfluss, Funktionen sowie Ein- und Ausgabe. Es erhebt nicht den Anspruch,
umfassend zu sein."""
        P = Paragraph(text, styleSheet["Code"])
        dumpParagraphFrags(P)
        w, h = P.wrap(6 * 72, 9.7 * 72)
        dumpParagraphLines(P)

    if flagged(4):
        text = """Die eingebaute Funktion <font name=Courier>range(i, j [, stride])</font><onDraw name=myIndex label="eingebaute Funktionen::range()"><onDraw name=myIndex label="range() (Funktion)"><onDraw name=myIndex label="Funktionen::range()"> erzeugt eine Liste von Ganzzahlen und f&#xfc;llt sie mit Werten <font name=Courier>k</font>, f&#xfc;r die gilt: <font name=Courier>i &lt;= k &lt; j</font>. Man kann auch eine optionale Schrittweite angeben. Die eingebaute Funktion <font name=Courier>xrange()</font><onDraw name=myIndex label="eingebaute Funktionen::xrange()"><onDraw name=myIndex label="xrange() (Funktion)"><onDraw name=myIndex label="Funktionen::xrange()"> erf&#xfc;llt einen &#xe4;hnlichen Zweck, gibt aber eine unver&#xe4;nderliche Sequenz vom Typ <font name=Courier>XRangeType</font><onDraw name=myIndex label="XRangeType"> zur&#xfc;ck. Anstatt alle Werte in der Liste abzuspeichern, berechnet diese Liste ihre Werte, wann immer sie angefordert werden. Das ist sehr viel speicherschonender, wenn mit sehr langen Listen von Ganzzahlen gearbeitet wird. <font name=Courier>XRangeType</font> kennt eine einzige Methode, <font name=Courier>s.tolist()</font><onDraw name=myIndex label="XRangeType::tolist() (Methode)"><onDraw name=myIndex label="s.tolist() (Methode)"><onDraw name=myIndex label="Methoden::s.tolist()">, die seine Werte in eine Liste umwandelt."""
        aW = 420
        aH = 64.4
        P = Paragraph(text, B)
        dumpParagraphFrags(P)
        w, h = P.wrap(aW, aH)
        print("After initial wrap", w, h)
        dumpParagraphLines(P)
        S = P.split(aW, aH)
        dumpParagraphFrags(S[0])
        w0, h0 = S[0].wrap(aW, aH)
        print("After split wrap", w0, h0)
        dumpParagraphLines(S[0])

    if flagged(5):
        text = f"<para> {chr(163)} <![CDATA[</font></b>& {chr(163)} < >]]></para>"
        P = Paragraph(text, styleSheet["Code"])
        dumpParagraphFrags(P)
        w, h = P.wrap(6 * 72, 9.7 * 72)
        dumpParagraphLines(P)

    if flagged(6):
        for text in [
            """Here comes <FONT FACE="Helvetica" SIZE="14pt">Helvetica 14</FONT> with <STRONG>strong</STRONG> <EM>emphasis</EM>.""",
            """Here comes <font face="Helvetica" size="14pt">Helvetica 14</font> with <Strong>strong</Strong> <em>emphasis</em>.""",
            """Here comes <font face="Courier" size="3cm">Courier 3cm</font> and normal again.""",
        ]:
            P = Paragraph(text, styleSheet["Normal"], caseSensitive=0)
            dumpParagraphFrags(P)
            w, h = P.wrap(6 * 72, 9.7 * 72)
            dumpParagraphLines(P)

    if flagged(7):
        text = """<para align="CENTER" fontSize="24" leading="30"><b>Generated by:</b>Dilbert</para>"""
        P = Paragraph(text, styleSheet["Code"])
        dumpParagraphFrags(P)
        w, h = P.wrap(6 * 72, 9.7 * 72)
        dumpParagraphLines(P)

    if flagged(8):
        text = """- bullet 0<br/>- bullet 1<br/>- bullet 2<br/>- bullet 3<br/>- bullet 4<br/>- bullet 5"""
        P = Paragraph(text, styleSheet["Normal"])
        dumpParagraphFrags(P)
        w, h = P.wrap(6 * 72, 9.7 * 72)
        dumpParagraphLines(P)
        S = P.split(6 * 72, h / 2.0)
        print(len(S))
        dumpParagraphLines(S[0])
        dumpParagraphLines(S[1])

    if flagged(9):
        text = """Furthermore, the fundamental error of
regarding <img src="../docs/images/testimg.gif" width="3" height="7"/> functional notions as
categorial delimits a general
convention regarding the forms of the<br/>
grammar. I suggested that these results
would follow from the assumption that"""
        P = Paragraph(
            text, ParagraphStyle("aaa", parent=styleSheet["Normal"], align=TA_JUSTIFY)
        )
        dumpParagraphFrags(P)
        w, h = P.wrap(6 * cm - 12, 9.7 * 72)
        dumpParagraphLines(P)

    if flagged(10):
        text = """a b c\xc2\xa0d e f"""
        P = Paragraph(
            text, ParagraphStyle("aaa", parent=styleSheet["Normal"], align=TA_JUSTIFY)
        )
        dumpParagraphFrags(P)
        w, h = P.wrap(6 * cm - 12, 9.7 * 72)
        dumpParagraphLines(P)
