# Copyright (C) 2002-2004  TechGame Networks, LLC.
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the BSD style License as found in the
# LICENSE file included with this distribution.
#
#  Modified by Dirk Holtwick <holtwick@web.de>, 2007-2008

"""
CSS-2.1 engine

Primary classes:
    * CSSElementInterfaceAbstract
        Provide a concrete implementation for the XML element model used.

    * CSSCascadeStrategy
        Implements the CSS-2.1 engine's attribute lookup rules.

    * CSSParser
        Parses CSS source forms into usable results using CSSBuilder and
        CSSMutableSelector.  You may want to override parseExternal()

    * CSSBuilder (and CSSMutableSelector)
        A concrete implementation for cssParser.CSSBuilderAbstract (and
        cssParser.CSSSelectorAbstract) to provide usable results to
        CSSParser requests.

Dependencies:
    sets, cssParser, re (via cssParser)
"""
# ruff: noqa: N802, N803, N815
from __future__ import annotations

import copy
from abc import abstractmethod
from pathlib import Path
from typing import ClassVar

from xhtml2pdf.w3c import cssParser, cssSpecial

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ To replace any for with list comprehension
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def stopIter(value):
    raise StopIteration(*value)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ Constants / Variables / Etc.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CSSParseError = cssParser.CSSParseError


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ Definitions
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSElementInterfaceAbstract:
    @abstractmethod
    def getAttr(self, name, default=NotImplemented):
        raise NotImplementedError

    def getIdAttr(self):
        return self.getAttr("id", "")

    def getClassAttr(self):
        return self.getAttr("class", "")

    @abstractmethod
    def getInlineStyle(self):
        raise NotImplementedError

    @abstractmethod
    def matchesNode(self):
        raise NotImplementedError

    @abstractmethod
    def inPseudoState(self, name, params=()):
        raise NotImplementedError

    @abstractmethod
    def iterXMLParents(self):
        """Results must be compatible with CSSElementInterfaceAbstract."""
        raise NotImplementedError

    @abstractmethod
    def getPreviousSibling(self):
        raise NotImplementedError


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSCascadeStrategy:
    author = None
    user = None
    userAgenr = None

    def __init__(self, author=None, user=None, userAgent=None) -> None:
        if author is not None:
            self.author = author
        if user is not None:
            self.user = user
        if userAgent is not None:
            self.userAgenr = userAgent

    def copyWithUpdate(self, author=None, user=None, userAgent=None):
        if author is None:
            author = self.author
        if user is None:
            user = self.user
        if userAgent is None:
            userAgent = self.userAgenr
        return type(self)(author, user, userAgent)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def iterCSSRulesets(self, inline=None):
        if self.userAgenr is not None:
            yield self.userAgenr[0]
            yield self.userAgenr[1]

        if self.user is not None:
            yield self.user[0]

        if self.author is not None:
            yield self.author[0]
            yield self.author[1]

        if inline:
            yield inline[0]
            yield inline[1]

        if self.user is not None:
            yield self.user[1]

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def findStyleFor(self, element, attrName, default=NotImplemented):
        """
        Attempts to find the style setting for attrName in the CSSRulesets.

        Note: This method does not attempt to resolve rules that return
        "inherited", "default", or values that have units (including "%").
        This is left up to the client app to re-query the CSS in order to
        implement these semantics.
        """
        rule = self.findCSSRulesFor(element, attrName)
        return self._extractStyleForRule(rule, attrName, default)

    def findStylesForEach(self, element, attrNames, default=NotImplemented):
        """
        Attempts to find the style setting for attrName in the CSSRulesets.

        Note: This method does not attempt to resolve rules that return
        "inherited", "default", or values that have units (including "%").
        This is left up to the client app to re-query the CSS in order to
        implement these semantics.
        """
        rules = self.findCSSRulesForEach(element, attrNames)
        return [
            (attrName, self._extractStyleForRule(rule, attrName, default))
            for attrName, rule in rules.items()
        ]

    def findCSSRulesFor(self, element, attrName):
        rules = []

        inline = element.getInlineStyle()

        # Generator are wonderful but sometime slow...
        # for ruleset in self.iterCSSRulesets(inline):
        #    rules += ruleset.findCSSRuleFor(element, attrName)

        if self.userAgenr is not None:
            rules += self.userAgenr[0].findCSSRuleFor(element, attrName)
            rules += self.userAgenr[1].findCSSRuleFor(element, attrName)

        if self.user is not None:
            rules += self.user[0].findCSSRuleFor(element, attrName)

        if self.author is not None:
            rules += self.author[0].findCSSRuleFor(element, attrName)
            rules += self.author[1].findCSSRuleFor(element, attrName)

        if inline:
            rules += inline[0].findCSSRuleFor(element, attrName)
            rules += inline[1].findCSSRuleFor(element, attrName)

        if self.user is not None:
            rules += self.user[1].findCSSRuleFor(element, attrName)

        rules.sort()
        return rules

    def findCSSRulesForEach(self, element, attrNames):
        rules = {name: [] for name in attrNames}

        inline = element.getInlineStyle()
        for ruleset in self.iterCSSRulesets(inline):
            for attrName, attrRules in rules.items():
                attrRules += ruleset.findCSSRuleFor(element, attrName)

        for attrRules in rules.items():
            attrRules.sort()
        return rules

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @staticmethod
    def _extractStyleForRule(rule, attrName, default=NotImplemented):
        if rule:
            # rule is packed in a list to differentiate from "no rule" vs "rule
            # whose value evaluates as False"
            style = rule[-1][1]
            return style[attrName]
        if default is not NotImplemented:
            return default
        msg = f"Could not find style for '{attrName}' in {rule!r}"
        raise LookupError(msg)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ CSS Selectors
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSSelectorBase:
    inline = False
    _hash = None
    _specificity = None

    def __init__(self, completeName="*") -> None:
        if not isinstance(completeName, tuple):
            completeName = (None, "*", completeName)
        self.completeName = completeName

    def _updateHash(self):
        self._hash = hash((self.fullName, self.specificity(), self.qualifiers))

    def __hash__(self):
        if self._hash is None:
            return object.__hash__(self)
        return self._hash

    @property
    def nsPrefix(self):
        return self.completeName[0]

    @property
    def name(self):
        return self.completeName[2]

    @property
    def namespace(self):
        return self.completeName[1]

    @property
    def fullName(self):
        return self.completeName[1:3]

    def __repr__(self) -> str:
        strArgs = (type(self).__name__, *self.specificity(), self.asString())
        return "<%s %d:%d:%d:%d %s >" % strArgs

    def __str__(self) -> str:
        return self.asString()

    def _as_comparison_key(self):
        return (self.specificity(), self.fullName, self.qualifiers)

    def __eq__(self, other):
        return self._as_comparison_key() == other._as_comparison_key()

    def __lt__(self, other):
        return self._as_comparison_key() < other._as_comparison_key()

    def specificity(self):
        if self._specificity is None:
            self._specificity = self._calcSpecificity()
        return self._specificity

    def _calcSpecificity(self):
        """From http://www.w3.org/TR/CSS21/cascade.html#specificity."""
        hashCount = 0
        qualifierCount = 0
        elementCount = int(self.name != "*")
        for qualifier in self.qualifiers:
            if qualifier.isHash():
                hashCount += 1
            elif qualifier.isClass() or qualifier.isAttr():
                qualifierCount += 1
            elif qualifier.isPseudo():
                elementCount += 1
            elif qualifier.isCombiner():
                i, h, q, e = qualifier.selector.specificity()
                hashCount += h
                qualifierCount += q
                elementCount += e
        return self.inline, hashCount, qualifierCount, elementCount

    def matches(self, element=None):
        if element is None:
            return False

        # with  CSSDOMElementInterface.matchesNode(self, (namespace, tagName)) replacement:
        if self.fullName[1] not in {"*", element.domElement.tagName}:
            return False
        if (
            self.fullName[0] not in {None, "", "*"}
            and self.fullName[0] != element.domElement.namespaceURI
        ):
            return False

        return all(qualifier.matches(element) for qualifier in self.qualifiers)

    def asString(self):
        result = []
        if self.nsPrefix is not None:
            result.append(f"{self.nsPrefix}|{self.name}")
        else:
            result.append(self.name)

        for q in self.qualifiers:
            if q.isCombiner():
                result.insert(0, q.asString())
            else:
                result.append(q.asString())
        return "".join(result)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSInlineSelector(CSSSelectorBase):
    inline = True


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSMutableSelector(CSSSelectorBase, cssParser.CSSSelectorAbstract):
    qualifiers: ClassVar[list] = []

    def asImmutable(self):
        return CSSImmutableSelector(
            self.completeName, [q.asImmutable() for q in self.qualifiers]
        )

    @staticmethod
    def combineSelectors(selectorA, op, selectorB):
        selectorB.addCombination(op, selectorA)
        return selectorB

    def addCombination(self, op, other):
        self._addQualifier(CSSSelectorCombinationQualifier(op, other))

    def addHashId(self, hashId):
        self._addQualifier(CSSSelectorHashQualifier(hashId))

    def addClass(self, class_):
        self._addQualifier(CSSSelectorClassQualifier(class_))

    def addAttribute(self, attrName):
        self._addQualifier(CSSSelectorAttributeQualifier(attrName))

    def addAttributeOperation(self, attrName, op, attr_value):
        self._addQualifier(CSSSelectorAttributeQualifier(attrName, op, attr_value))

    def addPseudo(self, name):
        self._addQualifier(CSSSelectorPseudoQualifier(name))

    def addPseudoFunction(self, name, params):
        self._addQualifier(CSSSelectorPseudoQualifier(name, params))

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _addQualifier(self, qualifier):
        if self.qualifiers:
            self.qualifiers.append(qualifier)
        else:
            self.qualifiers = [qualifier]


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSImmutableSelector(CSSSelectorBase):
    def __init__(self, completeName="*", qualifiers=()) -> None:
        # print completeName, qualifiers
        self.qualifiers = tuple(qualifiers)
        super().__init__(completeName)
        self._updateHash()

    @classmethod
    def fromSelector(cls, selector):
        return cls(selector.completeName, selector.qualifiers)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ CSS Selector Qualifiers -- see CSSImmutableSelector
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSSelectorQualifierBase:
    @staticmethod
    def isHash():
        return False

    @staticmethod
    def isClass():
        return False

    @staticmethod
    def isAttr():
        return False

    @staticmethod
    def isPseudo():
        return False

    @staticmethod
    def isCombiner():
        return False

    def asImmutable(self):
        return self

    @abstractmethod
    def asString(self):
        raise NotImplementedError

    def __str__(self) -> str:
        return self.asString()


class CSSSelectorHashQualifier(CSSSelectorQualifierBase):
    def __init__(self, hashId) -> None:
        self.hashId = hashId

    @staticmethod
    def isHash():
        return True

    def __hash__(self):
        return hash((self.hashId,))

    def asString(self):
        return f"#{self.hashId}"

    def matches(self, element):
        return element.getIdAttr() == self.hashId

    def __eq__(self, other):
        return self.hashId == other.hashId

    def __lt__(self, other):
        return self.hashId < other.hashId


class CSSSelectorClassQualifier(CSSSelectorQualifierBase):
    def __init__(self, classId) -> None:
        self.classId = classId

    @staticmethod
    def isClass():
        return True

    def __hash__(self):
        return hash((self.classId,))

    def asString(self):
        return f".{self.classId}"

    def matches(self, element):
        # return self.classId in element.getClassAttr().split()
        attr_value = element.domElement.attributes.get("class")
        if attr_value is not None:
            return self.classId in attr_value.value.split()
        return False

    def __eq__(self, other):
        return self.classId == other.classId

    def __lt__(self, other):
        return self.classId < other.classId


class CSSSelectorAttributeQualifier(CSSSelectorQualifierBase):
    name, op, value = None, None, NotImplemented

    def __init__(self, attrName, op=None, attr_value=NotImplemented) -> None:
        self.name = attrName
        if op is not self.op:
            self.op = op
        if attr_value is not self.value:
            self.value = attr_value

    @staticmethod
    def isAttr():
        return True

    def __hash__(self):
        return hash((self.name, self.op, self.value))

    def asString(self):
        if self.value is NotImplemented:
            return f"[{self.name}]"
        return f"[{self.name}{self.op}{self.value}]"

    def matches(self, element):
        if self.op is None:
            return element.getAttr(self.name, NotImplemented) != NotImplemented
        if self.op == "=":
            return self.value == element.getAttr(self.name, NotImplemented)
        if self.op == "~=":
            # return self.value in element.getAttr(self.name, '').split()
            attr_value = element.domElement.attributes.get(self.name)
            if attr_value is not None:
                return self.value in attr_value.value.split()
            return False
        if self.op == "|=":
            # return self.value in element.getAttr(self.name, '').split('-')
            attr_value = element.domElement.attributes.get(self.name)
            if attr_value is not None:
                return self.value in attr_value.value.split("-")
            return False
        msg = f"Unknown operator {self.op!r} for {self!r}"
        raise RuntimeError(msg)


class CSSSelectorPseudoQualifier(CSSSelectorQualifierBase):
    def __init__(self, attrName, params=()) -> None:
        self.name = attrName
        self.params = tuple(params)

    @staticmethod
    def isPseudo():
        return True

    def __hash__(self):
        return hash((self.name, self.params))

    def asString(self):
        if self.params:
            return f":{self.name}"
        return f":{self.name}({self.params})"

    def matches(self, element):
        return element.inPseudoState(self.name, self.params)


class CSSSelectorCombinationQualifier(CSSSelectorQualifierBase):
    def __init__(self, op, selector) -> None:
        self.op = op
        self.selector = selector

    @staticmethod
    def isCombiner():
        return True

    def __hash__(self):
        return hash((self.op, self.selector))

    def asImmutable(self):
        return type(self)(self.op, self.selector.asImmutable())

    def asString(self):
        return f"{self.selector.asString()}{self.op}"

    def matches(self, element):
        op, selector = self.op, self.selector
        if op == " ":
            return any(selector.matches(parent) for parent in element.iterXMLParents())
        if op == ">":
            parent = next(element.iterXMLParents(), None)
            if parent is None:
                return False
            return selector.matches(parent)
        if op == "+":
            return selector.matches(element.getPreviousSibling())
        return None


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ CSS Misc
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSTerminalFunction:
    def __init__(self, name, params) -> None:
        self.name = name
        self.params = [
            param if isinstance(param, str) else str(param) for param in params
        ]

    def __repr__(self) -> str:
        return "<CSS function: {}({})>".format(self.name, ", ".join(self.params))


class CSSTerminalOperator(tuple):
    __slots__ = ()

    def __new__(cls, *args):
        return tuple.__new__(cls, args)

    def __repr__(self) -> str:
        return "op" + tuple.__repr__(self)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ CSS Objects
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSDeclarations(dict):  # noqa: PLW1641
    def __eq__(self, other):
        return False

    def __lt__(self, other):
        return False


class CSSRuleset(dict):
    def findCSSRulesFor(self, element, attrName):
        ruleResults = [
            (nodeFilter, declarations)
            for nodeFilter, declarations in self.items()
            if (attrName in declarations) and (nodeFilter.matches(element))
        ]
        ruleResults.sort()
        return ruleResults

    def findCSSRuleFor(self, element, attrName):
        # rule is packed in a list to differentiate from "no rule" vs "rule
        # whose value evaluates as False"
        return self.findCSSRulesFor(element, attrName)[-1:]

    def mergeStyles(self, styles):
        """XXX Bugfix for use in PISA."""
        for k, v in styles.items():
            if k in self and self[k]:
                self[k] = copy.copy(self[k])
                self[k].update(v)
            else:
                self[k] = v


class CSSInlineRuleset(CSSRuleset, CSSDeclarations):
    def findCSSRulesFor(self, element, attrName):
        if attrName in self:
            return [(CSSInlineSelector(), self)]
        return []

    def findCSSRuleFor(self, *args, **kw):
        # rule is packed in a list to differentiate from "no rule" vs "rule
        # whose value evaluates as False"
        return self.findCSSRulesFor(*args, **kw)[-1:]


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ CSS Builder
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSBuilder(cssParser.CSSBuilderAbstract):
    RulesetFactory = CSSRuleset
    SelectorFactory = CSSMutableSelector
    MediumSetFactory = set
    DeclarationsFactory = CSSDeclarations
    TermFunctionFactory = CSSTerminalFunction
    TermOperatorFactory = CSSTerminalOperator
    xmlnsSynonyms: ClassVar[dict] = {}
    mediumSet = None
    trackImportance = True
    charset = None

    def __init__(self, mediumSet=mediumSet, trackImportance=trackImportance) -> None:
        self.setMediumSet(mediumSet)
        self.setTrackImportance(trackImportance=trackImportance)

    def isValidMedium(self, mediums):
        if not mediums:
            return False
        if "all" in mediums:
            return True

        mediums = self.MediumSetFactory(mediums)
        return bool(self.getMediumSet().intersection(mediums))

    def getMediumSet(self):
        return self.mediumSet

    def setMediumSet(self, mediumSet):
        self.mediumSet = self.MediumSetFactory(mediumSet)

    def updateMediumSet(self, mediumSet):
        self.getMediumSet().update(mediumSet)

    def getTrackImportance(self):
        return self.trackImportance

    def setTrackImportance(self, *, trackImportance=True):
        self.trackImportance = trackImportance

    # ~ helpers ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def _pushState(self):
        _restoreState = self.__dict__
        self.__dict__ = self.__dict__.copy()
        self._restoreState = _restoreState
        self.namespaces = {}

    def _popState(self):
        self.__dict__ = self._restoreState

    def _declarations(self, declarations, DeclarationsFactory=None):
        DeclarationsFactory = DeclarationsFactory or self.DeclarationsFactory
        if self.trackImportance:
            normal, important = [], []
            for d in declarations:
                if d[-1]:
                    important.append(d[:-1])
                else:
                    normal.append(d[:-1])
            return DeclarationsFactory(normal), DeclarationsFactory(important)
        return DeclarationsFactory(declarations)

    def _xmlnsGetSynonym(self, uri):
        # Don't forget to substitute our namespace synonyms!
        return self.xmlnsSynonyms.get(uri or None, uri) or None

    # ~ css results ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def beginStylesheet(self):
        self._pushState()

    def endStylesheet(self):
        self._popState()

    def stylesheet(self, stylesheetElements, stylesheetImports):
        # XXX Updated for PISA
        if self.trackImportance:
            normal, important = self.RulesetFactory(), self.RulesetFactory()
            for normalStylesheet, importantStylesheet in stylesheetImports:
                normal.mergeStyles(normalStylesheet)
                important.mergeStyles(importantStylesheet)
            for normalStyleElement, importantStyleElement in stylesheetElements:
                normal.mergeStyles(normalStyleElement)
                important.mergeStyles(importantStyleElement)
            return normal, important
        result = self.RulesetFactory()
        for stylesheet in stylesheetImports:
            result.mergeStyles(stylesheet)

        for styleElement in stylesheetElements:
            result.mergeStyles(styleElement)
        return result

    def beginInline(self):
        self._pushState()

    def endInline(self):
        self._popState()

    @staticmethod
    def specialRules(declarations):
        return cssSpecial.parseSpecialRules(declarations)

    def inline(self, declarations):
        declarations = self.specialRules(declarations)
        return self._declarations(declarations, CSSInlineRuleset)

    def ruleset(self, selectors, declarations):
        # XXX Modified for pisa!
        declarations = self.specialRules(declarations)
        # XXX Modified for pisa!

        if self.trackImportance:
            normalDecl, importantDecl = self._declarations(declarations)
            normal, important = self.RulesetFactory(), self.RulesetFactory()
            for s in selectors:
                s = s.asImmutable()
                if normalDecl:
                    normal[s] = normalDecl
                if importantDecl:
                    important[s] = importantDecl
            return normal, important
        declarations = self._declarations(declarations)
        result = [(s.asImmutable(), declarations) for s in selectors]
        return self.RulesetFactory(result)

    # ~ css namespaces ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def resolveNamespacePrefix(self, nsPrefix, name):
        if nsPrefix == "*":
            return (nsPrefix, "*", name)
        xmlns = self.namespaces.get(nsPrefix, None)
        xmlns = self._xmlnsGetSynonym(xmlns)
        return (nsPrefix, xmlns, name)

    # ~ css @ directives ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def atCharset(self, charset):
        self.charset = charset

    def atImport(self, import_, mediums, cssParser):
        if self.isValidMedium(mediums):
            return cssParser.parseExternal(import_)
        return None

    def atNamespace(self, nsprefix, uri):
        self.namespaces[nsprefix] = uri

    def atMedia(self, mediums, ruleset):
        if self.isValidMedium(mediums):
            return ruleset
        return None

    def atPage(
        self,
        name: str,
        pseudopage: str | None,
        data: dict,
        *,
        isLandscape: bool,
        pageBorder,
    ):
        """This is overridden by xhtml2pdf.context.pisaCSSBuilder."""
        raise NotImplementedError

    def atFontFace(self, declarations):
        """This is overridden by xhtml2pdf.context.pisaCSSBuilder."""
        return self.ruleset([self.selector("*")], declarations)

    @staticmethod
    def atIdent(_atIdent, _cssParser, src):
        return src, NotImplemented

    # ~ css selectors ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def selector(self, name):
        return self.SelectorFactory(name)

    def combineSelectors(self, selectorA, op, selectorB):
        return self.SelectorFactory.combineSelectors(selectorA, op, selectorB)

    # ~ css declarations ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    def property(self, name, value, *, important=False):  # noqa: A003
        if self.trackImportance:
            return (name, value, important)
        return (name, value)

    def combineTerms(self, termA, op, termB):
        if op in {",", " "}:
            if isinstance(termA, list):
                termA.append(termB)
                return termA
            return [termA, termB]
        if op is None and termB is None:
            return [termA]
        if isinstance(termA, list):
            # Bind these "closer" than the list operators -- i.e. work on
            # the (recursively) last element of the list
            termA[-1] = self.combineTerms(termA[-1], op, termB)
            return termA
        return self.TermOperatorFactory(termA, op, termB)

    @staticmethod
    def termIdent(value):
        return value

    @staticmethod
    def termNumber(value, units=None):
        if units:
            return value, units
        return value

    @staticmethod
    def termRGB(value):
        return value

    @staticmethod
    def termURI(value):
        return value

    @staticmethod
    def termString(value):
        return value

    @staticmethod
    def termUnicodeRange(value):
        return value

    def termFunction(self, name, value):
        return self.TermFunctionFactory(name, value)

    @staticmethod
    def termUnknown(src):
        return src, NotImplemented


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~ CSS Parser -- finally!
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


class CSSParser(cssParser.CSSParser):
    CSSBuilderFactory = CSSBuilder

    def __init__(self, cssBuilder=None, *, create=True, **kw) -> None:
        if not cssBuilder and create:
            assert cssBuilder is None
            cssBuilder = self.createCSSBuilder(**kw)
        super().__init__(cssBuilder)

    def createCSSBuilder(self, **kw):
        return self.CSSBuilderFactory(**kw)

    def parseExternal(self, cssResourceName):
        if Path(cssResourceName).is_file():
            return self.parseFile(cssResourceName)
        raise RuntimeError('Cannot resolve external CSS file: "%s"' % cssResourceName)
