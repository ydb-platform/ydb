"""
CSS-2.1 parser.

Copyright (C) 2002-2004 TechGame Networks, LLC.

This library is free software; you can redistribute it and/or
modify it under the terms of the BSD style License as found in the
LICENSE file included with this distribution.

Modified by Dirk Holtwick <holtwick@web.de>, 2007-2008

The CSS 2.1 Specification this parser was derived from can be found at http://www.w3.org/TR/CSS21/

Primary Classes:
    * CSSParser
        Parses CSS source forms into results using a Builder Pattern.  Must
        provide concrete implementation of CSSBuilderAbstract.

    * CSSBuilderAbstract
        Outlines the interface between CSSParser and it's rule-builder.
        Compose CSSParser with a concrete implementation of the builder to get
        usable results from the CSS parser.

Dependencies:
    re
"""

# ruff: noqa: N802, N803, N815, N816, N999
from __future__ import annotations

import re
from abc import abstractmethod
from typing import ClassVar

from reportlab.lib.pagesizes import landscape

import xhtml2pdf.default
from xhtml2pdf.util import getSize
from xhtml2pdf.w3c import cssSpecial

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ Definitions
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def isAtRuleIdent(src, ident):
    return re.match(r"^@" + ident + r"\s*", src)


def stripAtRuleIdent(src):
    return re.sub(r"^@[a-z\-]+\s*", "", src)


class CSSSelectorAbstract:
    """
    Outlines the interface between CSSParser and it's rule-builder for selectors.

    CSSBuilderAbstract.selector and CSSBuilderAbstract.combineSelectors must
    return concrete implementations of this abstract.

    See css.CSSMutableSelector for an example implementation.
    """

    @abstractmethod
    def addHashId(self, hashId):
        raise NotImplementedError

    @abstractmethod
    def addClass(self, class_):
        raise NotImplementedError

    @abstractmethod
    def addAttribute(self, attrName):
        raise NotImplementedError

    @abstractmethod
    def addAttributeOperation(self, attrName, op, attr_value):
        raise NotImplementedError

    @abstractmethod
    def addPseudo(self, name):
        raise NotImplementedError

    @abstractmethod
    def addPseudoFunction(self, name, value):
        raise NotImplementedError


class CSSBuilderAbstract:
    """
    Outlines the interface between CSSParser and it's rule-builder.  Compose
    CSSParser with a concrete implementation of the builder to get usable
    results from the CSS parser.

    See css.CSSBuilder for an example implementation
    """

    # ~ css results ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def beginStylesheet(self):
        raise NotImplementedError

    @abstractmethod
    def stylesheet(self, elements):
        raise NotImplementedError

    @abstractmethod
    def endStylesheet(self):
        raise NotImplementedError

    @abstractmethod
    def beginInline(self):
        raise NotImplementedError

    @abstractmethod
    def inline(self, declarations):
        raise NotImplementedError

    @abstractmethod
    def endInline(self):
        raise NotImplementedError

    @abstractmethod
    def ruleset(self, selectors, declarations):
        raise NotImplementedError

    # ~ css namespaces ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def resolveNamespacePrefix(self, nsPrefix, name):
        raise NotImplementedError

    # ~ css @ directives ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def atCharset(self, charset):
        raise NotImplementedError

    @abstractmethod
    def atImport(self, import_, mediums, cssParser):
        raise NotImplementedError

    @abstractmethod
    def atNamespace(self, nsPrefix, uri):
        raise NotImplementedError

    @abstractmethod
    def atMedia(self, mediums, ruleset):
        raise NotImplementedError

    @abstractmethod
    def atPage(
        self,
        name: str,
        pseudopage: str | None,
        data: dict,
        *,
        isLandscape: bool,
        pageBorder,
    ):
        raise NotImplementedError

    @abstractmethod
    def atFontFace(self, declarations):
        raise NotImplementedError

    @abstractmethod
    def atIdent(self, atIdent, cssParser, src):
        return src, NotImplemented

    # ~ css selectors ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def combineSelectors(self, selectorA, combiner, selectorB):
        """Return value must implement CSSSelectorAbstract."""
        raise NotImplementedError

    @abstractmethod
    def selector(self, name):
        """Return value must implement CSSSelectorAbstract."""
        raise NotImplementedError

    # ~ css declarations ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @abstractmethod
    def property(self, name, value, *, important=False):  # noqa: A003
        raise NotImplementedError

    @abstractmethod
    def combineTerms(self, termA, combiner, termB):
        raise NotImplementedError

    @abstractmethod
    def termIdent(self, value):
        raise NotImplementedError

    @abstractmethod
    def termNumber(self, value, units=None):
        raise NotImplementedError

    @abstractmethod
    def termRGB(self, value):
        raise NotImplementedError

    @abstractmethod
    def termURI(self, value):
        raise NotImplementedError

    @abstractmethod
    def termString(self, value):
        raise NotImplementedError

    @abstractmethod
    def termUnicodeRange(self, value):
        raise NotImplementedError

    @abstractmethod
    def termFunction(self, name, value):
        raise NotImplementedError

    @abstractmethod
    def termUnknown(self, _src):
        raise NotImplementedError


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ CSS Parser
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSParseError(Exception):
    src: str = ""
    ctxsrc: str = ""
    fullsrc: bytes | str = ""
    inline: bool = False
    srcCtxIdx: int | None = None
    srcFullIdx: int | None = None
    ctxsrcFullIdx: int | None = None

    def __init__(self, msg, src, ctxsrc=None) -> None:
        super().__init__(msg)
        self.src = src
        self.ctxsrc = ctxsrc or src
        if self.ctxsrc:
            self.srcCtxIdx = self.ctxsrc.find(self.src)
            if self.srcCtxIdx < 0:
                del self.srcCtxIdx

    def __str__(self) -> str:
        if self.ctxsrc and self.srcCtxIdx:
            return (
                super().__str__()
                + ":: ("
                + str(repr(self.ctxsrc[: self.srcCtxIdx]))
                + ", "
                + str(repr(self.ctxsrc[self.srcCtxIdx : self.srcCtxIdx + 20]))
                + ")"
            )
        return super().__str__() + ":: " + repr(self.src[:40])

    def setFullCSSSource(self, fullsrc, *, inline=False):
        self.fullsrc = fullsrc
        if isinstance(self.fullsrc, bytes):
            self.fullsrc = str(self.fullsrc, "utf-8")
        if inline:
            self.inline = inline
        if self.fullsrc:
            self.srcFullIdx = self.fullsrc.find(self.src)
            if self.srcFullIdx < 0:
                del self.srcFullIdx
            self.ctxsrcFullIdx = self.fullsrc.find(self.ctxsrc)
            if self.ctxsrcFullIdx < 0:
                del self.ctxsrcFullIdx


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def regex_or(*args):
    """Small helper to join regex expressions"""
    return "|".join(args)


class CSSParser:
    """
    CSS-2.1 parser dependent only upon the re module.

    Implemented directly from http://www.w3.org/TR/CSS21/grammar.html
    Tested with some existing CSS stylesheets for portability.

    CSS Parsing API:
        * setCSSBuilder()
            To set your concrete implementation of CSSBuilderAbstract

        * parseFile()
            Use to parse external stylesheets using a file-like object

            >>> cssFile = open('test.css', 'r')
            >>> stylesheets = myCSSParser.parseFile(cssFile)

        * parse()
            Use to parse embedded stylesheets using source string

            >>> cssSrc = '''
                body,body.body {
                    font: 110%, "Times New Roman", Arial, Verdana, Helvetica, serif;
                    background: White;
                    color: Black;
                }
                a {text-decoration: underline;}
            '''
            >>> stylesheets = myCSSParser.parse(cssSrc)

        * parseInline()
            Use to parse inline stylesheets using attribute source string

            >>> style = 'font: 110%, "Times New Roman", Arial, Verdana, Helvetica, serif; background: White; color: Black'
            >>> stylesheets = myCSSParser.parseInline(style)

        * parseAttributes()
            Use to parse attribute string values into inline stylesheets

            >>> stylesheets = myCSSParser.parseAttributes(
                    font='110%, "Times New Roman", Arial, Verdana, Helvetica, serif',
                    background='White',
                    color='Black')

        * parseSingleAttr()
            Use to parse a single string value into a CSS expression

            >>> fontValue = myCSSParser.parseSingleAttr('110%, "Times New Roman", Arial, Verdana, Helvetica, serif')
    """

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Constants / Variables / Etc.
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    ParseError = CSSParseError

    AttributeOperators: ClassVar[list[str]] = ["=", "~=", "|=", "&=", "^=", "!=", "<>"]
    SelectorQualifiers: ClassVar[tuple[str, ...]] = ("#", ".", "[", ":")
    SelectorCombiners: ClassVar[list[str]] = ["+", ">"]
    ExpressionOperators: ClassVar[tuple[str, ...]] = ("/", "+", ",")

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Regular expressions
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    _reflags = re.I | re.M | re.U
    i_hex = "[0-9a-fA-F]"
    i_nonascii = "[\200-\377]"
    i_unicode = r"\\(?:%s){1,6}\s?" % i_hex
    i_escape = regex_or(i_unicode, r"\\[ -~\200-\377]")
    # i_nmstart = regex_or('[A-Za-z_]', i_nonascii, i_escape)
    i_nmstart = regex_or(
        r"\-[^0-9]|[A-Za-z_]", i_nonascii, i_escape
    )  # XXX Added hyphen, http://www.w3.org/TR/CSS21/syndata.html#value-def-identifier
    i_nmchar = regex_or("[-0-9A-Za-z_]", i_nonascii, i_escape)
    i_ident = f"((?:{i_nmstart})(?:{i_nmchar})*)"
    re_ident = re.compile(i_ident, _reflags)
    # Caution: treats all characters above 0x7f as legal for an identifier.
    i_unicodeid = r"([^\u0000-\u007f]+)"
    re_unicodeid = re.compile(i_unicodeid, _reflags)
    i_unicodestr1 = r"(\'[^\u0000-\u007f]+\')"
    i_unicodestr2 = r"(\"[^\u0000-\u007f]+\")"
    i_unicodestr = regex_or(i_unicodestr1, i_unicodestr2)
    re_unicodestr = re.compile(i_unicodestr, _reflags)
    i_element_name = rf"((?:{i_ident[1:-1]})|\*)"
    re_element_name = re.compile(i_element_name, _reflags)
    i_namespace_selector = rf"((?:{i_ident[1:-1]})|\*|)\|(?!=)"
    re_namespace_selector = re.compile(i_namespace_selector, _reflags)
    i_class = r"\." + i_ident
    re_class = re.compile(i_class, _reflags)
    i_hash = "#((?:%s)+)" % i_nmchar
    re_hash = re.compile(i_hash, _reflags)
    i_rgbcolor = f"(#{i_hex}{{8}}|#{i_hex}{{6}}|#{i_hex}{{3}})"
    re_rgbcolor = re.compile(i_rgbcolor, _reflags)
    i_nl = "\n|\r\n|\r|\f"
    i_escape_nl = r"\\(?:%s)" % i_nl
    i_string_content = regex_or("[\t !#$%&(-~]", i_escape_nl, i_nonascii, i_escape)
    i_string1 = '"((?:%s|\')*)"' % i_string_content
    i_string2 = "'((?:%s|\")*)'" % i_string_content
    i_string = regex_or(i_string1, i_string2)
    re_string = re.compile(i_string, _reflags)
    i_uri = r"url\(\s*(?:(?:{})|((?:{})+))\s*\)".format(
        i_string, regex_or("[!#$%&*-~]", i_nonascii, i_escape)
    )
    # XXX For now
    # i_uri = '(url\\(.*?\\))'
    re_uri = re.compile(i_uri, _reflags)
    i_num = (  # XXX Added out parenthesis, because e.g. .5em was not parsed correctly
        r"(([-+]?[0-9]+(?:\.[0-9]+)?)|([-+]?\.[0-9]+))"
    )
    re_num = re.compile(i_num, _reflags)
    i_unit = "(%%|%s)?" % i_ident
    re_unit = re.compile(i_unit, _reflags)
    i_function = i_ident + r"\("
    re_function = re.compile(i_function, _reflags)
    i_functionterm = "[-+]?" + i_function
    re_functionterm = re.compile(i_functionterm, _reflags)
    i_unicoderange1 = rf"(?:U\+{i_hex}{{1,6}}-{i_hex}{{1,6}})"
    i_unicoderange2 = r"(?:U\+\?{1,6}|{h}(\?{0,5}|{h}(\?{0,4}|{h}(\?{0,3}|{h}(\?{0,2}|{h}(\??|{h}))))))"
    i_unicoderange = i_unicoderange1  # '(%s|%s)' % (i_unicoderange1, i_unicoderange2)
    re_unicoderange = re.compile(i_unicoderange, _reflags)

    # i_comment = '(?:\/\*[^*]*\*+([^/*][^*]*\*+)*\/)|(?://.*)'
    # gabriel: only C convention for comments is allowed in CSS
    i_comment = r"(?:\/\*[^*]*\*+([^/*][^*]*\*+)*\/)"
    re_comment = re.compile(i_comment, _reflags)
    i_important = r"!\s*(important)"
    re_important = re.compile(i_important, _reflags)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Public
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def __init__(self, cssBuilder=None) -> None:
        self.setCSSBuilder(cssBuilder)

    # ~ CSS Builder to delegate to ~~~~~~~~~~~~~~~~~~~~~~~~

    def getCSSBuilder(self):
        """A concrete instance implementing CSSBuilderAbstract."""
        return self._cssBuilder

    def setCSSBuilder(self, cssBuilder):
        """A concrete instance implementing CSSBuilderAbstract."""
        self._cssBuilder = cssBuilder

    cssBuilder = property(getCSSBuilder, setCSSBuilder)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Public CSS Parsing API
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def parseFile(self, srcFile):
        """
        Parses CSS file-like objects using the current cssBuilder.
        Use for external stylesheets.
        """
        with open(srcFile, encoding="utf-8") as file_handler:
            file_content = file_handler.read()

        return self.parse(file_content)

    def parse(self, src):
        """
        Parses CSS string source using the current cssBuilder.
        Use for embedded stylesheets.
        """
        self.cssBuilder.beginStylesheet()
        try:
            # XXX Some simple preprocessing
            src = cssSpecial.cleanupCSS(src)

            try:
                src, stylesheet = self._parseStylesheet(src)
            except self.ParseError as err:
                err.setFullCSSSource(src)
                raise
        finally:
            self.cssBuilder.endStylesheet()
        return stylesheet

    def parseInline(self, src):
        """
        Parses CSS inline source string using the current cssBuilder.
        Use to parse a tag's 'style'-like attribute.
        """
        self.cssBuilder.beginInline()
        try:
            try:
                src, properties = self._parseDeclarationGroup(src.strip(), braces=False)
            except self.ParseError as err:
                err.setFullCSSSource(src, inline=True)
                raise

            result = self.cssBuilder.inline(properties)
        finally:
            self.cssBuilder.endInline()
        return result

    def parseAttributes(self, attributes=None, **kwAttributes):
        """
        Parses CSS attribute source strings, and return as an inline stylesheet.
        Use to parse a tag's highly CSS-based attributes like 'font'.

        See also: parseSingleAttr
        """
        attributes = attributes if attributes is not None else {}
        if attributes:
            kwAttributes.update(attributes)

        self.cssBuilder.beginInline()
        try:
            properties = []
            try:
                for property_name, src in kwAttributes.items():
                    src, single_property = self._parseDeclarationProperty(
                        src.strip(), property_name
                    )
                    properties.append(single_property)

            except self.ParseError as err:
                err.setFullCSSSource(src, inline=True)
                raise

            result = self.cssBuilder.inline(properties)
        finally:
            self.cssBuilder.endInline()
        return result

    def parseSingleAttr(self, attr_value):
        """
        Parse a single CSS attribute source string, and returns the built CSS expression.
        Use to parse a tag's highly CSS-based attributes like 'font'.

        See also: parseAttributes
        """
        results = self.parseAttributes(temp=attr_value)
        if "temp" in results[1]:
            return results[1]["temp"]
        return results[0]["temp"]

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # ~ Internal _parse methods
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _parseStylesheet(self, src):
        """
        stylesheet
        : [ CHARSET_SYM S* STRING S* ';' ]?
            [S|CDO|CDC]* [ import [S|CDO|CDC]* ]*
            [ [ ruleset | media | page | font_face ] [S|CDO|CDC]* ]*
        ;
        """
        # FIXME: BYTES to STR
        if isinstance(src, bytes):
            src = src.decode()
        # Get rid of the comments
        src = self.re_comment.sub("", src)

        # [ CHARSET_SYM S* STRING S* ';' ]?
        src = self._parseAtCharset(src)

        # [S|CDO|CDC]*
        src = self._parseSCDOCDC(src)
        #  [ import [S|CDO|CDC]* ]*
        src, stylesheetImports = self._parseAtImports(src)

        # [ namespace [S|CDO|CDC]* ]*
        src = self._parseAtNamespace(src)

        stylesheetElements = []

        # [ [ ruleset | atkeywords ] [S|CDO|CDC]* ]*
        while src:  # due to ending with ]*
            if src.startswith("@"):
                # @media, @page, @font-face
                src, atResults = self._parseAtKeyword(src)
                if atResults is not None and atResults != NotImplemented:
                    stylesheetElements.extend(atResults)
            else:
                # ruleset
                src, ruleset = self._parseRuleset(src)
                stylesheetElements.append(ruleset)

            # [S|CDO|CDC]*
            src = self._parseSCDOCDC(src)

        stylesheet = self.cssBuilder.stylesheet(stylesheetElements, stylesheetImports)
        return src, stylesheet

    @staticmethod
    def _parseSCDOCDC(src):
        """[S|CDO|CDC]*."""
        while 1:
            src = src.lstrip()
            if src.startswith("<!--"):
                src = src[4:]
            elif src.startswith("-->"):
                src = src[3:]
            else:
                break
        return src

    # ~ CSS @ directives ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _parseAtCharset(self, src):
        """[ CHARSET_SYM S* STRING S* ';' ]?."""
        if isAtRuleIdent(src, "charset"):
            src = stripAtRuleIdent(src)
            charset, src = self._getString(src)
            src = src.lstrip()
            if src[:1] != ";":
                msg = "@charset expected a terminating ';'"
                raise self.ParseError(msg, src, self.ctxsrc)
            src = src[1:].lstrip()

            self.cssBuilder.atCharset(charset)
        return src

    def _parseAtImports(self, src):
        """[ import [S|CDO|CDC]* ]*."""
        result = []
        while isAtRuleIdent(src, "import"):
            ctxsrc = src
            src = stripAtRuleIdent(src)

            import_, src = self._getStringOrURI(src)
            if import_ is None:
                msg = "Import expecting string or url"
                raise self.ParseError(msg, src, ctxsrc)

            mediums = []
            medium, src = self._getIdent(src.lstrip())
            while medium is not None:
                mediums.append(medium)
                if src[:1] == ",":
                    src = src[1:].lstrip()
                    medium, src = self._getIdent(src)
                else:
                    break

            # XXX No medium inherits and then "all" is appropriate
            if not mediums:
                mediums = ["all"]

            if src[:1] != ";":
                msg = "@import expected a terminating ';'"
                raise self.ParseError(msg, src, ctxsrc)
            src = src[1:].lstrip()

            stylesheet = self.cssBuilder.atImport(import_, mediums, self)
            if stylesheet is not None:
                result.append(stylesheet)

            src = self._parseSCDOCDC(src)
        return src, result

    def _parseAtNamespace(self, src):
        """
        Namespace :

        @namespace S* [IDENT S*]? [STRING|URI] S* ';' S*
        """
        src = self._parseSCDOCDC(src)
        while isAtRuleIdent(src, "namespace"):
            ctxsrc = src
            src = stripAtRuleIdent(src)

            namespace, src = self._getStringOrURI(src)
            if namespace is None:
                nsPrefix, src = self._getIdent(src)
                if nsPrefix is None:
                    msg = "@namespace expected an identifier or a URI"
                    raise self.ParseError(msg, src, ctxsrc)
                namespace, src = self._getStringOrURI(src.lstrip())
                if namespace is None:
                    msg = "@namespace expected a URI"
                    raise self.ParseError(msg, src, ctxsrc)
            else:
                nsPrefix = None

            src = src.lstrip()
            if src[:1] != ";":
                msg = "@namespace expected a terminating ';'"
                raise self.ParseError(msg, src, ctxsrc)
            src = src[1:].lstrip()

            self.cssBuilder.atNamespace(nsPrefix, namespace)

            src = self._parseSCDOCDC(src)
        return src

    def _parseAtKeyword(self, src):
        """[media | page | font_face | unknown_keyword]."""
        ctxsrc = src
        if isAtRuleIdent(src, "media"):
            src, result = self._parseAtMedia(src)
        elif isAtRuleIdent(src, "page"):
            src, result = self._parseAtPage(src)
        elif isAtRuleIdent(src, "font-face"):
            src, result = self._parseAtFontFace(src)
        # XXX added @import, was missing!
        elif isAtRuleIdent(src, "import"):
            src, result = self._parseAtImports(src)
        elif isAtRuleIdent(src, "frame"):
            src, result = self._parseAtFrame(src)
        elif src.startswith("@"):
            src, result = self._parseAtIdent(src)
        else:
            msg = "Unknown state in atKeyword"
            raise self.ParseError(msg, src, ctxsrc)
        return src, result

    def _parseAtMedia(self, src):
        """
        media
        : MEDIA_SYM S* medium [ ',' S* medium ]* '{' S* ruleset* '}' S*
        ;
        """
        ctxsrc = src
        src = src[len("@media ") :].lstrip()
        mediums = []
        while src and src[0] != "{":
            medium, src = self._getIdent(src)
            # make "and ... {" work
            if medium in {None, "and"}:
                # default to mediatype "all"
                if medium is None:
                    mediums.append("all")
                # strip up to curly bracket
                pattern = re.compile(".*?[{]", re.DOTALL)

                match = re.match(pattern, src)
                src = src[match.end() - 1 :]
                break
            mediums.append(medium)
            src = src[1:].lstrip() if src[0] == "," else src.lstrip()

        if not src.startswith("{"):
            msg = "Ruleset opening '{' not found"
            raise self.ParseError(msg, src, ctxsrc)
        src = src[1:].lstrip()

        stylesheetElements = []
        # while src and not src.startswith('}'):
        #    src, ruleset = self._parseRuleset(src)
        #    stylesheetElements.append(ruleset)
        #    src = src.lstrip()

        # Containing @ where not found and parsed
        while src and not src.startswith("}"):
            if src.startswith("@"):
                # @media, @page, @font-face
                src, atResults = self._parseAtKeyword(src)
                if atResults is not None:
                    stylesheetElements.extend(atResults)
            else:
                # ruleset
                src, ruleset = self._parseRuleset(src)
                stylesheetElements.append(ruleset)
            src = src.lstrip()

        if not src.startswith("}"):
            msg = "Ruleset closing '}' not found"
            raise self.ParseError(msg, src, ctxsrc)
        src = src[1:].lstrip()

        result = self.cssBuilder.atMedia(mediums, stylesheetElements)
        return src, result

    def _parseAtPage(self, src):
        """
        page
        : PAGE_SYM S* IDENT? pseudo_page? S*
            '{' S* declaration [ ';' S* declaration ]* '}' S*
        ;
        """
        data = {}
        pageBorder = None
        isLandscape = False

        ctxsrc = src
        src = src[len("@page") :].lstrip()
        page, src = self._getIdent(src)
        if src[:1] == ":":
            pseudopage, src = self._getIdent(src[1:])
            page = page + "_" + pseudopage
        else:
            pseudopage = None

        # src, properties = self._parseDeclarationGroup(src.lstrip())

        # Containing @ where not found and parsed
        stylesheetElements = []
        src = src.lstrip()
        properties = []

        # XXX Extended for PDF use
        if not src.startswith("{"):
            msg = "Ruleset opening '{' not found"
            raise self.ParseError(msg, src, ctxsrc)
        src = src[1:].lstrip()

        while src and not src.startswith("}"):
            if src.startswith("@"):
                # @media, @page, @font-face
                src, atResults = self._parseAtKeyword(src)
                if atResults is not None:
                    stylesheetElements.extend(atResults)
            else:
                src, nproperties = self._parseDeclarationGroup(
                    src.lstrip(), braces=False
                )
                properties += nproperties

                # Set pagesize, orientation (landscape, portrait)
                data = {}
                pageBorder = None

                if properties:
                    result = self.cssBuilder.ruleset(
                        [self.cssBuilder.selector("*")], properties
                    )
                    try:
                        data = result[0].values()[0]
                    except Exception:
                        data = result[0].popitem()[1]
                    pageBorder = data.get("-pdf-frame-border", None)

                if "-pdf-page-size" in data:
                    self.c.pageSize = xhtml2pdf.default.PML_PAGESIZES.get(
                        str(data["-pdf-page-size"]).lower(), self.c.pageSize
                    )

                isLandscape = False
                if "size" in data:
                    size = data["size"]
                    if not isinstance(size, list):
                        size = [size]
                    sizeList = []
                    for value in size:
                        valueStr = str(value).lower()
                        if isinstance(value, tuple):
                            sizeList.append(getSize(value))
                        elif valueStr == "landscape":
                            isLandscape = True
                        elif valueStr == "portrait":
                            isLandscape = False
                        elif valueStr in xhtml2pdf.default.PML_PAGESIZES:
                            self.c.pageSize = xhtml2pdf.default.PML_PAGESIZES[valueStr]
                        else:
                            msg = "Unknown size value for @page"
                            raise RuntimeError(msg)

                    if len(sizeList) == 2:
                        self.c.pageSize = tuple(sizeList)

                    if isLandscape:
                        self.c.pageSize = landscape(self.c.pageSize)

            src = src.lstrip()

        result = [
            self.cssBuilder.atPage(
                page, pseudopage, data, isLandscape=isLandscape, pageBorder=pageBorder
            )
        ]

        return src[1:].lstrip(), result

    def _parseAtFrame(self, src):
        """XXX Proprietary for PDF."""
        src = src[len("@frame ") :].lstrip()
        box, src = self._getIdent(src)
        src, properties = self._parseDeclarationGroup(src.lstrip())
        result = [self.cssBuilder.atFrame(box, properties)]
        return src.lstrip(), result

    def _parseAtFontFace(self, src):
        src = src[len("@font-face") :].lstrip()
        src, properties = self._parseDeclarationGroup(src)
        result = [self.cssBuilder.atFontFace(properties)]
        return src, result

    def _parseAtIdent(self, src):
        ctxsrc = src
        atIdent, src = self._getIdent(src[1:])
        if atIdent is None:
            msg = "At-rule expected an identifier for the rule"
            raise self.ParseError(msg, src, ctxsrc)

        src, result = self.cssBuilder.atIdent(atIdent, self, src)

        if result is NotImplemented:
            # An at-rule consists of everything up to and including the next semicolon (;)
            # or the next block, whichever comes first

            semiIdx = src.find(";")
            if semiIdx < 0:
                semiIdx = None
            blockIdx = src[:semiIdx].find("{")
            if blockIdx < 0:
                blockIdx = None

            if semiIdx is not None and semiIdx < blockIdx:
                src = src[semiIdx + 1 :].lstrip()
            elif blockIdx is None:
                # consume the rest of the content since we didn't find a block or a semicolon
                src = src[-1:-1]
            elif blockIdx is not None:
                # expecing a block...
                src = src[blockIdx:]
                try:
                    # try to parse it as a declarations block
                    src, declarations = self._parseDeclarationGroup(src)
                except self.ParseError:
                    # try to parse it as a stylesheet block
                    src, stylesheet = self._parseStylesheet(src)
            else:
                msg = "Unable to ignore @-rule block"
                raise self.ParserError(msg, src, ctxsrc)

        return src.lstrip(), result

    # ~ ruleset - see selector and declaration groups ~~~~

    def _parseRuleset(self, src):
        """
        ruleset
        : selector [ ',' S* selector ]*
            '{' S* declaration [ ';' S* declaration ]* '}' S*
        ;
        """
        src, selectors = self._parseSelectorGroup(src)
        src, properties = self._parseDeclarationGroup(src.lstrip())
        result = self.cssBuilder.ruleset(selectors, properties)
        return src, result

    # ~ selector parsing ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _parseSelectorGroup(self, src):
        selectors = []
        while src[:1] not in {"{", "}", "]", "(", ")", ";", ""}:
            src, selector = self._parseSelector(src)
            if selector is None:
                break
            selectors.append(selector)
            if src.startswith(","):
                src = src[1:].lstrip()
        return src, selectors

    def _parseSelector(self, src):
        """
        selector
        : simple_selector [ combinator simple_selector ]*
        ;
        """
        src, selector = self._parseSimpleSelector(src)
        srcLen = len(src)  # XXX
        while src[:1] not in {"", ",", ";", "{", "}", "[", "]", "(", ")"}:
            for combiner in self.SelectorCombiners:
                if src.startswith(combiner):
                    src = src[len(combiner) :].lstrip()
                    break
            else:
                combiner = " "
            src, selectorB = self._parseSimpleSelector(src)

            # XXX Fix a bug that occurred here e.g. : .1 {...}
            if len(src) >= srcLen:
                src = src[1:]
                while src and (
                    src[:1] not in {"", ",", ";", "{", "}", "[", "]", "(", ")"}
                ):
                    src = src[1:]
                return src.lstrip(), None

            selector = self.cssBuilder.combineSelectors(selector, combiner, selectorB)

        return src.lstrip(), selector

    def _parseSimpleSelector(self, src):
        """
        simple_selector
        : [ namespace_selector ]? element_name? [ HASH | class | attrib | pseudo ]* S*
        ;
        """
        ctxsrc = src.lstrip()
        nsPrefix, src = self._getMatchResult(self.re_namespace_selector, src)
        name, src = self._getMatchResult(self.re_element_name, src)
        if name:
            pass  # already *successfully* assigned
        elif src[:1] in self.SelectorQualifiers:
            name = "*"
        else:
            msg = "Selector name or qualifier expected"
            raise self.ParseError(msg, src, ctxsrc)

        name = self.cssBuilder.resolveNamespacePrefix(nsPrefix, name)
        selector = self.cssBuilder.selector(name)
        while src and src[:1] in self.SelectorQualifiers:
            hash_, src = self._getMatchResult(self.re_hash, src)
            if hash_ is not None:
                selector.addHashId(hash_)
                continue

            class_, src = self._getMatchResult(self.re_class, src)
            if class_ is not None:
                selector.addClass(class_)
                continue

            if src.startswith("["):
                src, selector = self._parseSelectorAttribute(src, selector)
            elif src.startswith(":"):
                src, selector = self._parseSelectorPseudo(src, selector)
            else:
                break

        return src.lstrip(), selector

    def _parseSelectorAttribute(self, src, selector):
        """
        attrib
        : '[' S* [ namespace_selector ]? IDENT S* [ [ '=' | INCLUDES | DASHMATCH ] S*
            [ IDENT | STRING ] S* ]? ']'
        ;
        """
        ctxsrc = src
        if not src.startswith("["):
            msg = "Selector Attribute opening '[' not found"
            raise self.ParseError(msg, src, ctxsrc)
        src = src[1:].lstrip()

        nsPrefix, src = self._getMatchResult(self.re_namespace_selector, src)
        attrName, src = self._getIdent(src)

        src = src.lstrip()

        if attrName is None:
            msg = "Expected a selector attribute name"
            raise self.ParseError(msg, src, ctxsrc)
        if nsPrefix is not None:
            attrName = self.cssBuilder.resolveNamespacePrefix(nsPrefix, attrName)

        for op in self.AttributeOperators:
            if src.startswith(op):
                break
        else:
            op = ""
        src = src[len(op) :].lstrip()

        if op:
            attr_value, src = self._getIdent(src)
            if attr_value is None:
                attr_value, src = self._getString(src)
                if attr_value is None:
                    msg = "Expected a selector attribute value"
                    raise self.ParseError(msg, src, ctxsrc)
        else:
            attr_value = None

        if not src.startswith("]"):
            msg = "Selector Attribute closing ']' not found"
            raise self.ParseError(msg, src, ctxsrc)
        src = src[1:]

        if op:
            selector.addAttributeOperation(attrName, op, attr_value)
        else:
            selector.addAttribute(attrName)
        return src, selector

    def _parseSelectorPseudo(self, src, selector):
        """
        pseudo
        : ':' [ IDENT | function ]
        ;
        """
        ctxsrc = src
        if not src.startswith(":"):
            msg = "Selector Pseudo ':' not found"
            raise self.ParseError(msg, src, ctxsrc)
        src = re.search("^:{1,2}(.*)", src, re.M | re.S).group(1)

        name, src = self._getIdent(src)
        if not name:
            msg = "Selector Pseudo identifier not found"
            raise self.ParseError(msg, src, ctxsrc)

        if src.startswith("("):
            # function
            src = src[1:].lstrip()
            src, term = self._parseExpression(src, return_list=True)
            if not src.startswith(")"):
                msg = "Selector Pseudo Function closing ')' not found"
                raise self.ParseError(msg, src, ctxsrc)
            src = src[1:]
            selector.addPseudoFunction(name, term)
        else:
            selector.addPseudo(name)

        return src, selector

    # ~ declaration and expression parsing ~~~~~~~~~~~~~~~

    def _parseDeclarationGroup(self, src, *, braces=True):
        ctxsrc = src
        if src.startswith("{"):
            src, braces = src[1:], True
        elif braces:
            msg = "Declaration group opening '{' not found"
            raise self.ParseError(msg, src, ctxsrc)

        properties = []
        src = src.lstrip()
        while src[:1] not in {"", ",", "{", "}", "[", "]", "(", ")", "@"}:  # XXX @?
            src, single_property = self._parseDeclaration(src)

            # XXX Workaround for styles like "*font: smaller"
            if src.startswith("*"):
                src = "-nothing-" + src[1:]
                continue

            if single_property is None:
                src = src[1:].lstrip()
                break
            properties.append(single_property)
            if src.startswith(";"):
                src = src[1:].lstrip()
            else:
                break

        if braces:
            if not src.startswith("}"):
                msg = "Declaration group closing '}' not found"
                raise self.ParseError(msg, src, ctxsrc)
            src = src[1:]

        return src.lstrip(), properties

    def _parseDeclaration(self, src):
        """
        declaration
        : ident S* ':' S* expr prio?
        | /* empty */
        ;
        """
        # property
        property_name, src = self._getIdent(src)

        if property_name is not None:
            src = src.lstrip()
            # S* : S*
            if src[:1] in {":", "="}:
                # Note: we are being fairly flexible here...  technically, the
                # ":" is *required*, but in the name of flexibility we
                # support a null transition, as well as an "=" transition
                src = src[1:].lstrip()

            src, single_property = self._parseDeclarationProperty(src, property_name)
        else:
            single_property = None

        return src, single_property

    def _parseDeclarationProperty(self, src, property_name):
        # expr
        src, expr = self._parseExpression(src)

        # prio?
        important, src = self._getMatchResult(self.re_important, src)
        src = src.lstrip()

        single_property = self.cssBuilder.property(
            property_name, expr, important=important
        )
        return src, single_property

    def _parseExpression(self, src, *, return_list=False):
        """
        expr
        : term [ operator term ]*
        ;
        """
        src, term = self._parseExpressionTerm(src)
        operator = None
        while src[:1] not in {"", ";", "{", "}", "[", "]", ")"}:
            for operator in self.ExpressionOperators:
                if src.startswith(operator):
                    src = src[len(operator) :]
                    break
            else:
                operator = " "
            src, term2 = self._parseExpressionTerm(src.lstrip())
            if term2 is NotImplemented:
                break
            term = self.cssBuilder.combineTerms(term, operator, term2)

        if operator is None and return_list:
            term = self.cssBuilder.combineTerms(term, None, None)
            return src, term
        return src, term

    def _parseExpressionTerm(self, src):
        """
        term
        : unary_operator?
            [ NUMBER S* | PERCENTAGE S* | LENGTH S* | EMS S* | EXS S* | ANGLE S* |
            TIME S* | FREQ S* | function ]
        | STRING S* | IDENT S* | URI S* | RGB S* | UNICODERANGE S* | hexcolor
        ;
        """
        ctxsrc = src

        result, src = self._getMatchResult(self.re_num, src)
        if result is not None:
            units, src = self._getMatchResult(self.re_unit, src)
            term = self.cssBuilder.termNumber(result, units)
            return src.lstrip(), term

        result, src = self._getString(src, self.re_uri)
        if result is not None:
            # XXX URL!!!!
            term = self.cssBuilder.termURI(result)
            return src.lstrip(), term

        result, src = self._getString(src)
        if result is not None:
            term = self.cssBuilder.termString(result)
            return src.lstrip(), term

        result, src = self._getMatchResult(self.re_functionterm, src)
        if result is not None:
            src, params = self._parseExpression(src, return_list=True)
            if src[0] != ")":
                msg = "Terminal function expression expected closing ')'"
                raise self.ParseError(msg, src, ctxsrc)
            src = src[1:].lstrip()
            term = self.cssBuilder.termFunction(result, params)
            return src, term

        result, src = self._getMatchResult(self.re_rgbcolor, src)
        if result is not None:
            term = self.cssBuilder.termRGB(result)
            return src.lstrip(), term

        result, src = self._getMatchResult(self.re_unicoderange, src)
        if result is not None:
            term = self.cssBuilder.termUnicodeRange(result)
            return src.lstrip(), term

        nsPrefix, src = self._getMatchResult(self.re_namespace_selector, src)
        result, src = self._getIdent(src)
        if result is not None:
            if nsPrefix is not None:
                result = self.cssBuilder.resolveNamespacePrefix(nsPrefix, result)
            term = self.cssBuilder.termIdent(result)
            return src.lstrip(), term

        result, src = self._getMatchResult(self.re_unicodeid, src)
        if result is not None:
            term = self.cssBuilder.termIdent(result)
            return src.lstrip(), term

        result, src = self._getMatchResult(self.re_unicodestr, src)
        if result is not None:
            term = self.cssBuilder.termString(result)
            return src.lstrip(), term

        return self.cssBuilder.termUnknown(src)

    # ~ utility methods ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _getIdent(self, src, default=None):
        return self._getMatchResult(self.re_ident, src, default)

    def _getString(self, src, rexpression=None, default=None):
        if rexpression is None:
            rexpression = self.re_string
        result = rexpression.match(src)
        if result:
            strres = tuple(filter(None, result.groups()))
            if strres:
                try:
                    strres = strres[0]
                except Exception:
                    strres = result.groups()[0]
            else:
                strres = ""
            return strres, src[result.end() :]
        return default, src

    def _getStringOrURI(self, src):
        result, src = self._getString(src, self.re_uri)
        if result is None:
            result, src = self._getString(src)
        return result, src

    @staticmethod
    def _getMatchResult(rexpression, src, default=None, group=1):
        result = rexpression.match(src)
        if result:
            return result.group(group), src[result.end() :]
        return default, src
