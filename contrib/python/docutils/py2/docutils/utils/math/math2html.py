#! /usr/bin/env python
# -*- coding: utf-8 -*-

#   math2html: convert LaTeX equations to HTML output.
#
#   Copyright (C) 2009-2011 Alex Fernández, 2021 Günter Milde
#
#   Released under the terms of the `2-Clause BSD license'_, in short:
#   Copying and distribution of this file, with or without modification,
#   are permitted in any medium without royalty provided the copyright
#   notice and this notice are preserved.
#   This file is offered as-is, without any warranty.
#
# .. _2-Clause BSD license: https://opensource.org/licenses/BSD-2-Clause

#   Based on eLyXer: convert LyX source files to HTML output.
#   http://alexfernandez.github.io/elyxer/

# Versions:
# 1.2.5  2015-02-26  eLyXer standalone formula conversion to HTML.
# 1.3    2021-06-02  Removed code for conversion of LyX files not
#                    required for LaTeX math.
#                    Support for more math commands from the AMS "math-guide".

import os.path
import sys
import unicodedata

if sys.version_info >= (3, 0):
    from urllib.parse import quote_plus
else:
    from urllib import quote_plus

from docutils.utils.math import tex2unichar

if sys.version_info >= (3, 0):
    unicode = str  #noqa
    basestring = str  # noqa


__version__ = u'1.3 (2021-06-02)'


class Trace(object):
  "A tracing class"

  debugmode = False
  quietmode = False
  showlinesmode = False

  prefix = None

  def debug(cls, message):
    "Show a debug message"
    if not Trace.debugmode or Trace.quietmode:
      return
    Trace.show(message, sys.stdout)

  def message(cls, message):
    "Show a trace message"
    if Trace.quietmode:
      return
    if Trace.prefix and Trace.showlinesmode:
      message = Trace.prefix + message
    Trace.show(message, sys.stdout)

  def error(cls, message):
    "Show an error message"
    message = '* ' + message
    if Trace.prefix and Trace.showlinesmode:
      message = Trace.prefix + message
    Trace.show(message, sys.stderr)

  def fatal(cls, message):
    "Show an error message and terminate"
    Trace.error('FATAL: ' + message)
    exit(-1)

  def show(cls, message, channel):
    "Show a message out of a channel"
    if sys.version_info < (3, 0):
      message = message.encode('utf-8')
    channel.write(message + '\n')

  debug = classmethod(debug)
  message = classmethod(message)
  error = classmethod(error)
  fatal = classmethod(fatal)
  show = classmethod(show)


class ContainerConfig(object):
  "Configuration class from elyxer.config file"

  extracttext = {
      u'allowed': [u'FormulaConstant',],
      u'extracted': [
                     u'AlphaCommand',
                     u'Bracket',
                     u'BracketCommand',
                     u'CombiningFunction',
                     u'EmptyCommand',
                     u'FontFunction',
                     u'Formula',
                     u'FormulaNumber',
                     u'FormulaSymbol',
                     u'OneParamFunction',
                     u'OversetFunction',
                     u'RawText',
                     u'SpacedCommand',
                     u'SymbolFunction',
                     u'TextFunction',
                     u'UndersetFunction',
                    ],
      }


class EscapeConfig(object):
  "Configuration class from elyxer.config file"

  chars = {
      u'\n': u'',
      u"'": u'’',
      u'`': u'‘',
      }

  entities = {
      u'&': u'&amp;',
      u'<': u'&lt;',
      u'>': u'&gt;',
      }


class FormulaConfig(object):
  "Configuration class from elyxer.config file"

  alphacommands = {
      '\\AmS': u'<span class="textsc">AmS</span>',
      '\\AA':        u'Å',
      '\\AE':        u'Æ',
      '\\DH':        u'Ð',
      '\\L':         u'Ł',
      '\\O':         u'Ø',
      '\\OE':        u'Œ',
      '\\TH':        u'Þ',
      '\\aa':        u'å',
      '\\ae':        u'æ',
      '\\dh':        u'ð',
      '\\i':         u'ı',
      '\\j':         u'ȷ',
      '\\l':         u'ł',
      '\\o':         u'ø',
      '\\oe':        u'œ',
      '\\ss':        u'ß',
      '\\th':        u'þ',
      }
  for key, value in tex2unichar.mathalpha.items():
      alphacommands['\\'+key] = value

  array = {
      u'begin': u'\\begin',
      u'cellseparator': u'&',
      u'end': u'\\end',
      u'rowseparator': u'\\\\',
      }

  bigbrackets = {u'(': [u'⎛', u'⎜', u'⎝',],
                 u')': [u'⎞', u'⎟', u'⎠',],
                 u'[': [u'⎡', u'⎢', u'⎣',],
                 u']': [u'⎤', u'⎥', u'⎦',],
                 u'{': [u'⎧', u'⎪', u'⎨', u'⎩',],
                 u'}': [u'⎫', u'⎪', u'⎬', u'⎭',],
                 # TODO: 2-row brackets with ⎰⎱ (\lmoustache \rmoustache)
                 u'|': [u'|',], # 007C VERTICAL LINE
                 # u'|': [u'⎮',], # 23AE INTEGRAL EXTENSION
                 # u'|': [u'⎪',], # 23AA CURLY BRACKET EXTENSION
                 u'‖': [u'‖'], # 2016 DOUBLE VERTICAL LINE
                 # u'∥': [u'∥'], # 2225 PARALLEL TO
                }

  bracketcommands = {
      u'\\left': u'span class="stretchy"',
      u'\\left.': u'<span class="leftdot"></span>',
      u'\\middle': u'span class="stretchy"',
      u'\\right': u'span class="stretchy"',
      u'\\right.': u'<span class="rightdot"></span>',
      }

  combiningfunctions = {
      u"\\'": u'́',
      u'\\"': u'̈',
      u'\\^': u'̂',
      u'\\`': u'̀',
      u'\\~': u'̃',
      u'\\c': u'̧',
      u'\\r': u'̊',
      u'\\s': u'̩',
      u'\\textcircled': u'⃝',
      u'\\textsubring': u'̥',
      u'\\v': u'̌',
      }
  for key, value in tex2unichar.mathaccent.items():
      combiningfunctions['\\'+key] = value

  commands = {
      '\\\\': u'<br/>',
      '\\\n': u' ', # escaped whitespace
      '\\\t': u' ', # escaped whitespace
      '\\centerdot': u'\u2B1D', # BLACK VERY SMALL SQUARE, mathbin
      '\\colon': u': ',
      '\\copyright': u'©',
      '\\dotminus': u'∸',
      '\\dots': u'…',
      '\\dotsb': u'⋯',
      '\\dotsc': u'…',
      '\\dotsi': u'⋯',
      '\\dotsm': u'⋯',
      '\\dotso': u'…',
      '\\euro': u'€',
      '\\guillemotleft': u'«',
      '\\guillemotright': u'»',
      '\\hbar': u'<i>\u0127</i>', # ħ LATIN SMALL LETTER H WITH STROKE
      '\\lVert': u'‖',
      '\\Arrowvert':  u'\u2016', # ‖
      '\\lvert': u'|',
      '\\newline': u'<br/>',
      '\\nobreakspace': u' ',
      '\\nolimits': u'',
      '\\nonumber': u'',
      '\\qquad': u'  ',
      '\\rVert': u'‖',
      '\\rvert': u'|',
      '\\textasciicircum': u'^',
      '\\textasciitilde': u'~',
      '\\textbackslash': u'\\',
      '\\textcopyright': u'©',
      '\\textdegree': u'°',
      '\\textellipsis': u'…',
      '\\textemdash': u'—',
      '\\textendash': u'—',
      '\\texteuro': u'€',
      '\\textgreater': u'>',
      '\\textless': u'<',
      '\\textordfeminine': u'ª',
      '\\textordmasculine': u'º',
      '\\textquotedblleft': u'“',
      '\\textquotedblright': u'”',
      '\\textquoteright': u'’',
      '\\textregistered': u'®',
      '\\textrightarrow': u'→',
      '\\textsection': u'§',
      '\\texttrademark': u'™',
      '\\texttwosuperior': u'²',
      '\\textvisiblespace': u' ',
      '\\thickspace': u'<span class="thickspace"> </span>', # 5/13 em
      '\\;': u'<span class="thickspace"> </span>', # 5/13 em
      '\\triangle': u'\u25B3', # WHITE UP-POINTING TRIANGLE, mathord
      '\\triangledown': u'\u25BD', # WHITE DOWN-POINTING TRIANGLE, mathord
      '\\varnothing': u'\u2300', # ⌀ DIAMETER SIGN
      # functions
      '\\Pr': u'Pr',
      '\\arccos': u'arccos',
      '\\arcsin': u'arcsin',
      '\\arctan': u'arctan',
      '\\arg': u'arg',
      '\\cos': u'cos',
      '\\cosh': u'cosh',
      '\\cot': u'cot',
      '\\coth': u'coth',
      '\\csc': u'csc',
      '\\deg': u'deg',
      '\\det': u'det',
      '\\dim': u'dim',
      '\\exp': u'exp',
      '\\gcd': u'gcd',
      '\\hom': u'hom',
      '\\injlim': u'inj lim',
      '\\ker': u'ker',
      '\\lg': u'lg',
      '\\liminf': u'lim inf',
      '\\limsup': u'lim sup',
      '\\ln': u'ln',
      '\\log': u'log',
      '\\projlim': u'proj lim',
      '\\sec': u'sec',
      '\\sin': u'sin',
      '\\sinh': u'sinh',
      '\\tan': u'tan',
      '\\tanh': u'tanh',
  }
  cmddict = {}
  cmddict.update(tex2unichar.mathbin) # TODO: spacing around binary operators
  cmddict.update(tex2unichar.mathopen)
  cmddict.update(tex2unichar.mathclose)
  cmddict.update(tex2unichar.mathfence)
  cmddict.update(tex2unichar.mathord)
  cmddict.update(tex2unichar.mathpunct)
  cmddict.update(tex2unichar.space)
  commands.update(('\\' + key, value) for key, value in cmddict.items())

  oversetfunctions = {
      # math accents (cf. combiningfunctions)
      # '\\acute':    u'´',
      '\\bar':      u'‒',  # FIGURE DASH
      # '\\breve':    u'˘',
      # '\\check':    u'ˇ',
      '\\dddot':    u'<span class="smallsymbol">⋯</span>',
      # '\\ddot':     u'··', # ¨ too high
      # '\\dot':      u'·',
      # '\\grave':    u'`',
      # '\\hat':      u'^',
      # '\\mathring': u'˚',
      # '\\tilde':    u'~',
      '\\vec':      u'<span class="smallsymbol">→</span>',
      # embellishments
      '\\overleftarrow': u'⟵',
      '\\overleftrightarrow': u'⟷',
      '\\overrightarrow': u'⟶',
      '\\widehat': u'^',
      '\\widetilde': u'～',
  }

  undersetfunctions = {
      '\\underleftarrow': u'⟵',
      '\\underleftrightarrow': u'⟷',
      '\\underrightarrow': u'⟶',
  }

  endings = {
      u'bracket': u'}',
      u'complex': u'\\]',
      u'endafter': u'}',
      u'endbefore': u'\\end{',
      u'squarebracket': u']',
      }

  environments = {
      u'align': [u'r', u'l',],
      u'eqnarray': [u'r', u'c', u'l',],
      u'gathered': [u'l', u'l',],
      u'smallmatrix': [u'c', u'c',],
      }

  fontfunctions = {
      u'\\boldsymbol': u'b', u'\\mathbb': u'span class="blackboard"',
      u'\\mathbb{A}': u'𝔸', u'\\mathbb{B}': u'𝔹', u'\\mathbb{C}': u'ℂ',
      u'\\mathbb{D}': u'𝔻', u'\\mathbb{E}': u'𝔼', u'\\mathbb{F}': u'𝔽',
      u'\\mathbb{G}': u'𝔾', u'\\mathbb{H}': u'ℍ', u'\\mathbb{J}': u'𝕁',
      u'\\mathbb{K}': u'𝕂', u'\\mathbb{L}': u'𝕃', u'\\mathbb{N}': u'ℕ',
      u'\\mathbb{O}': u'𝕆', u'\\mathbb{P}': u'ℙ', u'\\mathbb{Q}': u'ℚ',
      u'\\mathbb{R}': u'ℝ', u'\\mathbb{S}': u'𝕊', u'\\mathbb{T}': u'𝕋',
      u'\\mathbb{W}': u'𝕎', u'\\mathbb{Z}': u'ℤ', u'\\mathbf': u'b',
      u'\\mathcal': u'span class="scriptfont"',
      u'\\mathcal{B}': u'ℬ', u'\\mathcal{E}': u'ℰ', u'\\mathcal{F}':
      u'ℱ', u'\\mathcal{H}': u'ℋ', u'\\mathcal{I}': u'ℐ',
      u'\\mathcal{L}': u'ℒ', u'\\mathcal{M}': u'ℳ', u'\\mathcal{R}': u'ℛ',
      u'\\mathfrak': u'span class="fraktur"',
      u'\\mathfrak{C}': u'ℭ', u'\\mathfrak{F}': u'𝔉', u'\\mathfrak{H}': u'ℌ',
      u'\\mathfrak{I}': u'ℑ', u'\\mathfrak{R}': u'ℜ', u'\\mathfrak{Z}': u'ℨ',
      u'\\mathit': u'i',
      u'\\mathring{A}': u'Å', u'\\mathring{U}': u'Ů',
      u'\\mathring{a}': u'å', u'\\mathring{u}': u'ů', u'\\mathring{w}': u'ẘ',
      u'\\mathring{y}': u'ẙ',
      u'\\mathrm': u'span class="mathrm"',
      u'\\mathscr': u'span class="mathscr"',
      u'\\mathscr{B}': u'ℬ', u'\\mathscr{E}': u'ℰ', u'\\mathscr{F}': u'ℱ',
      u'\\mathscr{H}': u'ℋ', u'\\mathscr{I}': u'ℐ', u'\\mathscr{L}': u'ℒ',
      u'\\mathscr{M}': u'ℳ', u'\\mathscr{R}': u'ℛ',
      u'\\mathsf': u'span class="mathsf"',
      u'\\mathtt': u'span class="mathtt"',
      u'\\operatorname': u'span class="mathrm"',
      }

  hybridfunctions = {
      u'\\addcontentsline': [u'{$p!}{$q!}{$r!}', u'f0{}', u'ignored',],
      u'\\addtocontents': [u'{$p!}{$q!}', u'f0{}', u'ignored',],
      u'\\backmatter': [u'', u'f0{}', u'ignored',],
      u'\\binom': [u'{$1}{$2}', u'f2{(}f0{f1{$1}f1{$2}}f2{)}', u'span class="binom"', u'span class="binomstack"', u'span class="bigdelimiter size2"',],
      u'\\boxed': [u'{$1}', u'f0{$1}', u'span class="boxed"',],
      u'\\cfrac': [u'[$p!]{$1}{$2}', u'f0{f3{(}f1{$1}f3{)/(}f2{$2}f3{)}}', u'span class="fullfraction"', u'span class="numerator align-$p"', u'span class="denominator"', u'span class="ignored"',],
      u'\\color': [u'{$p!}{$1}', u'f0{$1}', u'span style="color: $p;"',],
      u'\\colorbox': [u'{$p!}{$1}', u'f0{$1}', u'span class="colorbox" style="background: $p;"',],
      u'\\dbinom': [u'{$1}{$2}', u'(f0{f1{f2{$1}}f1{f2{ }}f1{f2{$2}}})', u'span class="binomial"', u'span class="binomrow"', u'span class="binomcell"',],
      u'\\dfrac': [u'{$1}{$2}', u'f0{f3{(}f1{$1}f3{)/(}f2{$2}f3{)}}', u'span class="fullfraction"', u'span class="numerator"', u'span class="denominator"', u'span class="ignored"',],
      u'\\displaystyle': [u'{$1}', u'f0{$1}', u'span class="displaystyle"',],
      u'\\fancyfoot': [u'[$p!]{$q!}', u'f0{}', u'ignored',],
      u'\\fancyhead': [u'[$p!]{$q!}', u'f0{}', u'ignored',],
      u'\\fbox': [u'{$1}', u'f0{$1}', u'span class="fbox"',],
      u'\\fboxrule': [u'{$p!}', u'f0{}', u'ignored',],
      u'\\fboxsep': [u'{$p!}', u'f0{}', u'ignored',],
      u'\\fcolorbox': [u'{$p!}{$q!}{$1}', u'f0{$1}', u'span class="boxed" style="border-color: $p; background: $q;"',],
      u'\\frac': [u'{$1}{$2}', u'f0{f3{(}f1{$1}f3{)/(}f2{$2}f3{)}}', u'span class="fraction"', u'span class="numerator"', u'span class="denominator"', u'span class="ignored"',],
      u'\\framebox': [u'[$p!][$q!]{$1}', u'f0{$1}', u'span class="framebox align-$q" style="width: $p;"',],
      u'\\frontmatter': [u'', u'f0{}', u'ignored',],
      u'\\href': [u'[$o]{$u!}{$t!}', u'f0{$t}', u'a href="$u"',],
      u'\\hspace': [u'{$p!}', u'f0{ }', u'span class="hspace" style="width: $p;"',],
      u'\\leftroot': [u'{$p!}', u'f0{ }', u'span class="leftroot" style="width: $p;px"',],
      # TODO: convert 1 mu to 1/18 em
      # u'\\mspace': [u'{$p!}', u'f0{ }', u'span class="hspace" style="width: $p;"',],
      u'\\nicefrac': [u'{$1}{$2}', u'f0{f1{$1}⁄f2{$2}}', u'span class="fraction"', u'sup class="numerator"', u'sub class="denominator"', u'span class="ignored"',],
      u'\\parbox': [u'[$p!]{$w!}{$1}', u'f0{1}', u'div class="Boxed" style="width: $w;"',],
      u'\\raisebox': [u'{$p!}{$1}', u'f0{$1.font}', u'span class="raisebox" style="vertical-align: $p;"',],
      u'\\renewenvironment': [u'{$1!}{$2!}{$3!}', u'',],
      u'\\rule': [u'[$v!]{$w!}{$h!}', u'f0/', u'hr class="line" style="width: $w; height: $h;"',],
      u'\\scriptscriptstyle': [u'{$1}', u'f0{$1}', u'span class="scriptscriptstyle"',],
      u'\\scriptstyle': [u'{$1}', u'f0{$1}', u'span class="scriptstyle"',],
      # TODO: increase √-size with argument (\frac in display mode, ...)
      u'\\sqrt': [u'[$0]{$1}', u'f0{f1{$0}f2{√}f4{(}f3{$1}f4{)}}', u'span class="sqrt"', u'sup class="root"', u'span class="radical"', u'span class="root"', u'span class="ignored"',],
      u'\\stackrel': [u'{$1}{$2}', u'f0{f1{$1}f2{$2}}', u'span class="stackrel"', u'span class="upstackrel"', u'span class="downstackrel"',],
      u'\\tbinom': [u'{$1}{$2}', u'(f0{f1{f2{$1}}f1{f2{ }}f1{f2{$2}}})', u'span class="binomial"', u'span class="binomrow"', u'span class="binomcell"',],
      u'\\tfrac':  [u'{$1}{$2}', u'f0{f3{(}f1{$1}f3{)/(}f2{$2}f3{)}}', u'span class="textfraction"', u'span class="numerator"', u'span class="denominator"', u'span class="ignored"',],
      u'\\textcolor': [u'{$p!}{$1}', u'f0{$1}', u'span style="color: $p;"',],
      u'\\textstyle': [u'{$1}', u'f0{$1}', u'span class="textstyle"',],
      u'\\thispagestyle': [u'{$p!}', u'f0{}', u'ignored',],
      u'\\unit': [u'[$0]{$1}', u'$0f0{$1.font}', u'span class="unit"',],
      u'\\unitfrac': [u'[$0]{$1}{$2}', u'$0f0{f1{$1.font}⁄f2{$2.font}}', u'span class="fraction"', u'sup class="unit"', u'sub class="unit"',],
      u'\\uproot': [u'{$p!}', u'f0{ }', u'span class="uproot" style="width: $p;px"',],
      u'\\url': [u'{$u!}', u'f0{$u}', u'a href="$u"',],
      u'\\vspace': [u'{$p!}', u'f0{ }', u'span class="vspace" style="height: $p;"',],
      }

  hybridsizes = {
      u'\\binom': u'$1+$2', u'\\cfrac': u'$1+$2', u'\\dbinom': u'$1+$2+1',
      u'\\dfrac': u'$1+$2', u'\\frac': u'$1+$2', u'\\tbinom': u'$1+$2+1',
      }

  labelfunctions = {
      '\\label': u'a name="#"',
      }

  limitcommands = {
      '\\biginterleave': u'⫼',
      '\\inf': u'inf',
      '\\lim': u'lim',
      '\\max': u'max',
      '\\min': u'min',
      '\\sup': u'sup',
      '\\ointop':    u'<span class="bigoperator integral">∮</span>',
      '\\bigcap':    u'<span class="bigoperator">⋂</span>',
      '\\bigcup':    u'<span class="bigoperator">⋃</span>',
      '\\bigodot':   u'<span class="bigoperator">⨀</span>',
      '\\bigoplus':  u'<span class="bigoperator">⨁</span>',
      '\\bigotimes': u'<span class="bigoperator">⨂</span>',
      '\\bigsqcap':  u'<span class="bigoperator">⨅</span>',
      '\\bigsqcup':  u'<span class="bigoperator">⨆</span>',
      '\\biguplus':  u'<span class="bigoperator">⨄</span>',
      '\\bigvee':    u'<span class="bigoperator">⋁</span>',
      '\\bigwedge':  u'<span class="bigoperator">⋀</span>',
      '\\coprod':    u'<span class="bigoperator">∐</span>',
      '\\intop':     u'<span class="bigoperator integral">∫</span>',
      '\\prod':      u'<span class="bigoperator">∏</span>',
      '\\sum':       u'<span class="bigoperator">∑</span>',
      '\\varprod':   u'<span class="bigoperator">⨉</span>',
      '\\zcmp': u'⨟', '\\zhide': u'⧹', '\\zpipe': u'⨠', '\\zproject': u'⨡',
      # integrals have limits in index position with LaTeX default settings
      # TODO: move to commands?
      '\\int': u'<span class="bigoperator integral">∫</span>',
      '\\iint': u'<span class="bigoperator integral">∬</span>',
      '\\iiint': u'<span class="bigoperator integral">∭</span>',
      '\\iiiint': u'<span class="bigoperator integral">⨌</span>',
      '\\fint': u'<span class="bigoperator integral">⨏</span>',
      '\\idotsint': u'<span class="bigoperator integral">∫⋯∫</span>',
      '\\oint': u'<span class="bigoperator integral">∮</span>',
      '\\oiint': u'<span class="bigoperator integral">∯</span>',
      '\\oiiint': u'<span class="bigoperator integral">∰</span>',
      '\\ointclockwise': u'<span class="bigoperator integral">∲</span>',
      '\\ointctrclockwise': u'<span class="bigoperator integral">∳</span>',
      '\\smallint': u'<span class="smallsymbol integral">∫</span>',
      '\\sqint': u'<span class="bigoperator integral">⨖</span>',
      '\\varointclockwise': u'<span class="bigoperator integral">∲</span>',
      }

  modified = {
      u'\n': u'', u' ': u'', u'$': u'', u'&': u'	', u'\'': u'’', u'+': u'\u2009+\u2009',
      u',': u',\u2009', u'-': u'\u2009−\u2009', u'/': u'\u2009⁄\u2009', u':': u' : ', u'<': u'\u2009&lt;\u2009',
      u'=': u'\u2009=\u2009', u'>': u'\u2009&gt;\u2009', u'@': u'', u'~': u'\u00a0',
      }

  onefunctions = {
      '\\big': 'span class="bigdelimiter size1"',
      '\\bigl': 'span class="bigdelimiter size1"',
      '\\bigr': 'span class="bigdelimiter size1"',
      '\\Big': 'span class="bigdelimiter size2"',
      '\\Bigl': 'span class="bigdelimiter size2"',
      '\\Bigr': 'span class="bigdelimiter size2"',
      '\\bigg': 'span class="bigdelimiter size3"',
      '\\biggl': 'span class="bigdelimiter size3"',
      '\\biggr': 'span class="bigdelimiter size3"',
      '\\Bigg': 'span class="bigdelimiter size4"',
      '\\Biggl': 'span class="bigdelimiter size4"',
      '\\Biggr': 'span class="bigdelimiter size4"',
      # '\\bar': 'span class="bar"',
      '\\begin{array}': 'span class="arraydef"',
      '\\centering': 'span class="align-center"',
      '\\ensuremath': 'span class="ensuremath"',
      '\\hphantom': 'span class="phantom"',
      '\\noindent': 'span class="noindent"',
      '\\overbrace': 'span class="overbrace"',
      '\\overline': 'span class="overline"',
      '\\phantom': 'span class="phantom"',
      '\\underbrace': 'span class="underbrace"',
      '\\underline': 'u',
      '\\vphantom': 'span class="phantom"',
      }

  # relations (put additional space before and after the symbol)
  spacedcommands = {
      # negated symbols without pre-composed Unicode character
      '\\nleqq':      u'\u2266\u0338', # ≦̸
      '\\ngeqq':      u'\u2267\u0338', # ≧̸
      '\\nleqslant':  u'\u2a7d\u0338', # ⩽̸
      '\\ngeqslant':  u'\u2a7e\u0338', # ⩾̸
      '\\nsubseteqq': u'\u2AC5\u0338', # ⫅̸
      '\\nsupseteqq': u'\u2AC6\u0338', # ⫆̸
      '\\nsqsubset':  u'\u2276\u228F', # ⊏̸
      # modified glyphs
      '\\shortmid': u'<span class="smallsymbol">∣</span>',
      '\\shortparallel': u'<span class="smallsymbol">∥</span>',
      '\\nshortmid': u'<span class="smallsymbol">∤</span>',
      '\\nshortparallel': u'<span class="smallsymbol">∦</span>',
      '\\smallfrown': u'<span class="smallsymbol">⌢</span>',
      '\\smallsmile': u'<span class="smallsymbol">⌣</span>',
      '\\thickapprox': u'<span class="boldsymbol">≈</span>',
      '\\thicksim': u'<span class="boldsymbol">∼</span>',
      '\\varpropto': u'<span class="mathsf">\u221d</span>', # ∝ PROPORTIONAL TO
      }
  for key, value in tex2unichar.mathrel.items():
      spacedcommands['\\'+key] = value
  starts = {
      u'beginafter': u'}', u'beginbefore': u'\\begin{', u'bracket': u'{',
      u'command': u'\\', u'comment': u'%', u'complex': u'\\[', u'simple': u'$',
      u'squarebracket': u'[', u'unnumbered': u'*',
      }

  symbolfunctions = {
      u'^': u'sup', u'_': u'sub',
      }

  textfunctions = {
      u'\\mbox': u'span class="mbox"',
      u'\\text': u'span class="text"',
      u'\\textbf': u'span class="textbf"',
      u'\\textit': u'span class="textit"',
      u'\\textnormal': u'span class="textnormal"',
      u'\\textrm': u'span class="textrm"',
      u'\\textsc': u'span class="textsc"',
      u'\\textsf': u'span class="textsf"',
      u'\\textsl': u'span class="textsl"',
      u'\\texttt': u'span class="texttt"',
      u'\\textup': u'span class="normal"',
      }

  unmodified = {
      u'characters': [u'.', u'*', u'€', u'(', u')', u'[', u']',
                      u'·', u'!', u';', u'|', u'§', u'"', u'?'],
      }


class CommandLineParser(object):
  "A parser for runtime options"

  def __init__(self, options):
    self.options = options

  def parseoptions(self, args):
    "Parse command line options"
    if len(args) == 0:
      return None
    while len(args) > 0 and args[0].startswith('--'):
      key, value = self.readoption(args)
      if not key:
        return 'Option ' + value + ' not recognized'
      if not value:
        return 'Option ' + key + ' needs a value'
      setattr(self.options, key, value)
    return None

  def readoption(self, args):
    "Read the key and value for an option"
    arg = args[0][2:]
    del args[0]
    if '=' in arg:
      key = self.readequalskey(arg, args)
    else:
      key = arg.replace('-', '')
    if not hasattr(self.options, key):
      return None, key
    current = getattr(self.options, key)
    if isinstance(current, bool):
      return key, True
    # read value
    if len(args) == 0:
      return key, None
    if args[0].startswith('"'):
      initial = args[0]
      del args[0]
      return key, self.readquoted(args, initial)
    value = args[0].decode('utf-8')
    del args[0]
    if isinstance(current, list):
      current.append(value)
      return key, current
    return key, value

  def readquoted(self, args, initial):
    "Read a value between quotes"
    Trace.error('Oops')
    value = initial[1:]
    while len(args) > 0 and not args[0].endswith('"') and not args[0].startswith('--'):
      Trace.error('Appending ' + args[0])
      value += ' ' + args[0]
      del args[0]
    if len(args) == 0 or args[0].startswith('--'):
      return None
    value += ' ' + args[0:-1]
    return value

  def readequalskey(self, arg, args):
    "Read a key using equals"
    split = arg.split('=', 1)
    key = split[0]
    value = split[1]
    args.insert(0, value)
    return key


class Options(object):
  "A set of runtime options"

  location = None

  debug = False
  quiet = False
  version = False
  help = False
  simplemath = False
  showlines = True

  branches = dict()

  def parseoptions(self, args):
    "Parse command line options"
    Options.location = args[0]
    del args[0]
    parser = CommandLineParser(Options)
    result = parser.parseoptions(args)
    if result:
      Trace.error(result)
      self.usage()
    self.processoptions()

  def processoptions(self):
    "Process all options parsed."
    if Options.help:
      self.usage()
    if Options.version:
      self.showversion()
    # set in Trace if necessary
    for param in dir(Trace):
      if param.endswith('mode'):
        setattr(Trace, param, getattr(self, param[:-4]))

  def usage(self):
    "Show correct usage"
    Trace.error('Usage: ' + os.path.basename(Options.location)
                + ' [options] "input string"')
    Trace.error('Convert input string with LaTeX math to MathML')
    self.showoptions()

  def showoptions(self):
    "Show all possible options"
    Trace.error('    --help:                 show this online help')
    Trace.error('    --quiet:                disables all runtime messages')
    Trace.error('    --debug:                enable debugging messages (for developers)')
    Trace.error('    --version:              show version number and release date')
    Trace.error('    --simplemath:           do not generate fancy math constructions')
    sys.exit()

  def showversion(self):
    "Return the current eLyXer version string"
    Trace.error('math2html '+__version__)
    sys.exit()


class Cloner(object):
  "An object used to clone other objects."

  def clone(cls, original):
    "Return an exact copy of an object."
    "The original object must have an empty constructor."
    return cls.create(original.__class__)

  def create(cls, type):
    "Create an object of a given class."
    clone = type.__new__(type)
    clone.__init__()
    return clone

  clone = classmethod(clone)
  create = classmethod(create)

class ContainerExtractor(object):
  """A class to extract certain containers.

  The config parameter is a map containing three lists:
  allowed, copied and extracted.
  Each of the three is a list of class names for containers.
  Allowed containers are included as is into the result.
  Cloned containers are cloned and placed into the result.
  Extracted containers are looked into.
  All other containers are silently ignored.
  """

  def __init__(self, config):
    self.allowed = config['allowed']
    self.extracted = config['extracted']

  def extract(self, container):
    "Extract a group of selected containers from a container."
    list = []
    locate = lambda c: c.__class__.__name__ in self.allowed
    recursive = lambda c: c.__class__.__name__ in self.extracted
    process = lambda c: self.process(c, list)
    container.recursivesearch(locate, recursive, process)
    return list

  def process(self, container, list):
    "Add allowed containers."
    name = container.__class__.__name__
    if name in self.allowed:
      list.append(container)
    else:
      Trace.error('Unknown container class ' + name)

  def safeclone(self, container):
    "Return a new container with contents only in a safe list, recursively."
    clone = Cloner.clone(container)
    clone.output = container.output
    clone.contents = self.extract(container)
    return clone


class Parser(object):
  "A generic parser"

  def __init__(self):
    self.begin = 0
    self.parameters = dict()

  def parseheader(self, reader):
    "Parse the header"
    header = reader.currentline().split()
    reader.nextline()
    self.begin = reader.linenumber
    return header

  def parseparameter(self, reader):
    "Parse a parameter"
    split = reader.currentline().strip().split(' ', 1)
    reader.nextline()
    if len(split) == 0:
      return
    key = split[0]
    if len(split) == 1:
      self.parameters[key] = True
      return
    if not '"' in split[1]:
      self.parameters[key] = split[1].strip()
      return
    doublesplit = split[1].split('"')
    self.parameters[key] = doublesplit[1]

  def parseending(self, reader, process):
    "Parse until the current ending is found"
    if not self.ending:
      Trace.error('No ending for ' + unicode(self))
      return
    while not reader.currentline().startswith(self.ending):
      process()

  def parsecontainer(self, reader, contents):
    container = self.factory.createcontainer(reader)
    if container:
      container.parent = self.parent
      contents.append(container)

  def __unicode__(self):
    "Return a description"
    return self.__class__.__name__ + ' (' + unicode(self.begin) + ')'

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class LoneCommand(Parser):
  "A parser for just one command line"

  def parse(self, reader):
    "Read nothing"
    return []

class TextParser(Parser):
  "A parser for a command and a bit of text"

  stack = []

  def __init__(self, container):
    Parser.__init__(self)
    self.ending = None
    if container.__class__.__name__ in ContainerConfig.endings:
      self.ending = ContainerConfig.endings[container.__class__.__name__]
    self.endings = []

  def parse(self, reader):
    "Parse lines as long as they are text"
    TextParser.stack.append(self.ending)
    self.endings = TextParser.stack + [ContainerConfig.endings['Layout'],
        ContainerConfig.endings['Inset'], self.ending]
    contents = []
    while not self.isending(reader):
      self.parsecontainer(reader, contents)
    return contents

  def isending(self, reader):
    "Check if text is ending"
    current = reader.currentline().split()
    if len(current) == 0:
      return False
    if current[0] in self.endings:
      if current[0] in TextParser.stack:
        TextParser.stack.remove(current[0])
      else:
        TextParser.stack = []
      return True
    return False

class ExcludingParser(Parser):
  "A parser that excludes the final line"

  def parse(self, reader):
    "Parse everything up to (and excluding) the final line"
    contents = []
    self.parseending(reader, lambda: self.parsecontainer(reader, contents))
    return contents

class BoundedParser(ExcludingParser):
  "A parser bound by a final line"

  def parse(self, reader):
    "Parse everything, including the final line"
    contents = ExcludingParser.parse(self, reader)
    # skip last line
    reader.nextline()
    return contents

class BoundedDummy(Parser):
  "A bound parser that ignores everything"

  def parse(self, reader):
    "Parse the contents of the container"
    self.parseending(reader, lambda: reader.nextline())
    # skip last line
    reader.nextline()
    return []

class StringParser(Parser):
  "Parses just a string"

  def parseheader(self, reader):
    "Do nothing, just take note"
    self.begin = reader.linenumber + 1
    return []

  def parse(self, reader):
    "Parse a single line"
    contents = reader.currentline()
    reader.nextline()
    return contents



class ContainerOutput(object):
  "The generic HTML output for a container."

  def gethtml(self, container):
    "Show an error."
    Trace.error('gethtml() not implemented for ' + unicode(self))

  def isempty(self):
    "Decide if the output is empty: by default, not empty."
    return False

class EmptyOutput(ContainerOutput):

  def gethtml(self, container):
    "Return empty HTML code."
    return []

  def isempty(self):
    "This output is particularly empty."
    return True

class FixedOutput(ContainerOutput):
  "Fixed output"

  def gethtml(self, container):
    "Return constant HTML code"
    return container.html

class ContentsOutput(ContainerOutput):
  "Outputs the contents converted to HTML"

  def gethtml(self, container):
    "Return the HTML code"
    html = []
    if container.contents == None:
      return html
    for element in container.contents:
      if not hasattr(element, 'gethtml'):
        Trace.error('No html in ' + element.__class__.__name__ + ': ' + unicode(element))
        return html
      html += element.gethtml()
    return html

class TaggedOutput(ContentsOutput):
  "Outputs an HTML tag surrounding the contents."

  tag = None
  breaklines = False
  empty = False

  def settag(self, tag, breaklines=False, empty=False):
    "Set the value for the tag and other attributes."
    self.tag = tag
    if breaklines:
      self.breaklines = breaklines
    if empty:
      self.empty = empty
    return self

  def setbreaklines(self, breaklines):
    "Set the value for breaklines."
    self.breaklines = breaklines
    return self

  def gethtml(self, container):
    "Return the HTML code."
    if self.empty:
      return [self.selfclosing(container)]
    html = [self.open(container)]
    html += ContentsOutput.gethtml(self, container)
    html.append(self.close(container))
    return html

  def open(self, container):
    "Get opening line."
    if not self.checktag(container):
      return ''
    open = '<' + self.tag + '>'
    if self.breaklines:
      return open + '\n'
    return open

  def close(self, container):
    "Get closing line."
    if not self.checktag(container):
      return ''
    close = '</' + self.tag.split()[0] + '>'
    if self.breaklines:
      return '\n' + close + '\n'
    return close

  def selfclosing(self, container):
    "Get self-closing line."
    if not self.checktag(container):
      return ''
    selfclosing = '<' + self.tag + '/>'
    if self.breaklines:
      return selfclosing + '\n'
    return selfclosing

  def checktag(self, container):
    "Check that the tag is valid."
    if not self.tag:
      Trace.error('No tag in ' + unicode(container))
      return False
    if self.tag == '':
      return False
    return True

class FilteredOutput(ContentsOutput):
  "Returns the output in the contents, but filtered:"
  "some strings are replaced by others."

  def __init__(self):
    "Initialize the filters."
    self.filters = []

  def addfilter(self, original, replacement):
    "Add a new filter: replace the original by the replacement."
    self.filters.append((original, replacement))

  def gethtml(self, container):
    "Return the HTML code"
    result = []
    html = ContentsOutput.gethtml(self, container)
    for line in html:
      result.append(self.filter(line))
    return result

  def filter(self, line):
    "Filter a single line with all available filters."
    for original, replacement in self.filters:
      if original in line:
        line = line.replace(original, replacement)
    return line


class StringOutput(ContainerOutput):
  "Returns a bare string as output"

  def gethtml(self, container):
    "Return a bare string"
    return [container.string]


class Globable(object):
  """A bit of text which can be globbed (lumped together in bits).
  Methods current(), skipcurrent(), checkfor() and isout() have to be
  implemented by subclasses."""

  leavepending = False

  def __init__(self):
    self.endinglist = EndingList()

  def checkbytemark(self):
    "Check for a Unicode byte mark and skip it."
    if self.finished():
      return
    if ord(self.current()) == 0xfeff:
      self.skipcurrent()

  def isout(self):
    "Find out if we are out of the position yet."
    Trace.error('Unimplemented isout()')
    return True

  def current(self):
    "Return the current character."
    Trace.error('Unimplemented current()')
    return ''

  def checkfor(self, string):
    "Check for the given string in the current position."
    Trace.error('Unimplemented checkfor()')
    return False

  def finished(self):
    "Find out if the current text has finished."
    if self.isout():
      if not self.leavepending:
        self.endinglist.checkpending()
      return True
    return self.endinglist.checkin(self)

  def skipcurrent(self):
    "Return the current character and skip it."
    Trace.error('Unimplemented skipcurrent()')
    return ''

  def glob(self, currentcheck):
    "Glob a bit of text that satisfies a check on the current char."
    glob = ''
    while not self.finished() and currentcheck():
      glob += self.skipcurrent()
    return glob

  def globalpha(self):
    "Glob a bit of alpha text"
    return self.glob(lambda: self.current().isalpha())

  def globnumber(self):
    "Glob a row of digits."
    return self.glob(lambda: self.current().isdigit())

  def isidentifier(self):
    "Return if the current character is alphanumeric or _."
    if self.current().isalnum() or self.current() == '_':
      return True
    return False

  def globidentifier(self):
    "Glob alphanumeric and _ symbols."
    return self.glob(self.isidentifier)

  def isvalue(self):
    "Return if the current character is a value character:"
    "not a bracket or a space."
    if self.current().isspace():
      return False
    if self.current() in '{}()':
      return False
    return True

  def globvalue(self):
    "Glob a value: any symbols but brackets."
    return self.glob(self.isvalue)

  def skipspace(self):
    "Skip all whitespace at current position."
    return self.glob(lambda: self.current().isspace())

  def globincluding(self, magicchar):
    "Glob a bit of text up to (including) the magic char."
    glob = self.glob(lambda: self.current() != magicchar) + magicchar
    self.skip(magicchar)
    return glob

  def globexcluding(self, excluded):
    "Glob a bit of text up until (excluding) any excluded character."
    return self.glob(lambda: self.current() not in excluded)

  def pushending(self, ending, optional = False):
    "Push a new ending to the bottom"
    self.endinglist.add(ending, optional)

  def popending(self, expected = None):
    "Pop the ending found at the current position"
    if self.isout() and self.leavepending:
      return expected
    ending = self.endinglist.pop(self)
    if expected and expected != ending:
      Trace.error('Expected ending ' + expected + ', got ' + ending)
    self.skip(ending)
    return ending

  def nextending(self):
    "Return the next ending in the queue."
    nextending = self.endinglist.findending(self)
    if not nextending:
      return None
    return nextending.ending

class EndingList(object):
  "A list of position endings"

  def __init__(self):
    self.endings = []

  def add(self, ending, optional = False):
    "Add a new ending to the list"
    self.endings.append(PositionEnding(ending, optional))

  def pickpending(self, pos):
    "Pick any pending endings from a parse position."
    self.endings += pos.endinglist.endings

  def checkin(self, pos):
    "Search for an ending"
    if self.findending(pos):
      return True
    return False

  def pop(self, pos):
    "Remove the ending at the current position"
    if pos.isout():
      Trace.error('No ending out of bounds')
      return ''
    ending = self.findending(pos)
    if not ending:
      Trace.error('No ending at ' + pos.current())
      return ''
    for each in reversed(self.endings):
      self.endings.remove(each)
      if each == ending:
        return each.ending
      elif not each.optional:
        Trace.error('Removed non-optional ending ' + each)
    Trace.error('No endings left')
    return ''

  def findending(self, pos):
    "Find the ending at the current position"
    if len(self.endings) == 0:
      return None
    for index, ending in enumerate(reversed(self.endings)):
      if ending.checkin(pos):
        return ending
      if not ending.optional:
        return None
    return None

  def checkpending(self):
    "Check if there are any pending endings"
    if len(self.endings) != 0:
      Trace.error('Pending ' + unicode(self) + ' left open')

  def __unicode__(self):
    "Printable representation"
    string = 'endings ['
    for ending in self.endings:
      string += unicode(ending) + ','
    if len(self.endings) > 0:
      string = string[:-1]
    return string + ']'

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class PositionEnding(object):
  "An ending for a parsing position"

  def __init__(self, ending, optional):
    self.ending = ending
    self.optional = optional

  def checkin(self, pos):
    "Check for the ending"
    return pos.checkfor(self.ending)

  def __unicode__(self):
    "Printable representation"
    string = 'Ending ' + self.ending
    if self.optional:
      string += ' (optional)'
    return string

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class Position(Globable):
  """A position in a text to parse.
  Including those in Globable, functions to implement by subclasses are:
  skip(), identifier(), extract(), isout() and current()."""

  def __init__(self):
    Globable.__init__(self)

  def skip(self, string):
    "Skip a string"
    Trace.error('Unimplemented skip()')

  def identifier(self):
    "Return an identifier for the current position."
    Trace.error('Unimplemented identifier()')
    return 'Error'

  def extract(self, length):
    "Extract the next string of the given length, or None if not enough text,"
    "without advancing the parse position."
    Trace.error('Unimplemented extract()')
    return None

  def checkfor(self, string):
    "Check for a string at the given position."
    return string == self.extract(len(string))

  def checkforlower(self, string):
    "Check for a string in lower case."
    extracted = self.extract(len(string))
    if not extracted:
      return False
    return string.lower() == self.extract(len(string)).lower()

  def skipcurrent(self):
    "Return the current character and skip it."
    current = self.current()
    self.skip(current)
    return current

  def __next__(self):
    "Advance the position and return the next character."
    self.skipcurrent()
    return self.current()

  if sys.version_info < (3, 0):
      next = __next__

  def checkskip(self, string):
    "Check for a string at the given position; if there, skip it"
    if not self.checkfor(string):
      return False
    self.skip(string)
    return True

  def error(self, message):
    "Show an error message and the position identifier."
    Trace.error(message + ': ' + self.identifier())

class TextPosition(Position):
  "A parse position based on a raw text."

  def __init__(self, text):
    "Create the position from some text."
    Position.__init__(self)
    self.pos = 0
    self.text = text
    self.checkbytemark()

  def skip(self, string):
    "Skip a string of characters."
    self.pos += len(string)

  def identifier(self):
    "Return a sample of the remaining text."
    length = 30
    if self.pos + length > len(self.text):
      length = len(self.text) - self.pos
    return '*' + self.text[self.pos:self.pos + length] + '*'

  def isout(self):
    "Find out if we are out of the text yet."
    return self.pos >= len(self.text)

  def current(self):
    "Return the current character, assuming we are not out."
    return self.text[self.pos]

  def extract(self, length):
    "Extract the next string of the given length, or None if not enough text."
    if self.pos + length > len(self.text):
      return None
    return self.text[self.pos : self.pos + length]


class Container(object):
  "A container for text and objects in a lyx file"

  partkey = None
  parent = None
  begin = None

  def __init__(self):
    self.contents = list()

  def process(self):
    "Process contents"
    pass

  def gethtml(self):
    "Get the resulting HTML"
    html = self.output.gethtml(self)
    if isinstance(html, basestring):
      Trace.error('Raw string ' + html)
      html = [html]
    return html

  def escape(self, line, replacements = EscapeConfig.entities):
    "Escape a line with replacements from a map"
    pieces = sorted(replacements.keys())
    # do them in order
    for piece in pieces:
      if piece in line:
        line = line.replace(piece, replacements[piece])
    return line

  def escapeentities(self, line):
    "Escape all Unicode characters to HTML entities."
    result = ''
    pos = TextPosition(line)
    while not pos.finished():
      if ord(pos.current()) > 128:
        codepoint = hex(ord(pos.current()))
        if codepoint == '0xd835':
          codepoint = hex(ord(next(pos)) + 0xf800)
        result += '&#' + codepoint[1:] + ';'
      else:
        result += pos.current()
      pos.skipcurrent()
    return result

  def searchall(self, type):
    "Search for all embedded containers of a given type"
    list = []
    self.searchprocess(type, lambda container: list.append(container))
    return list

  def searchremove(self, type):
    "Search for all containers of a type and remove them"
    list = self.searchall(type)
    for container in list:
      container.parent.contents.remove(container)
    return list

  def searchprocess(self, type, process):
    "Search for elements of a given type and process them"
    self.locateprocess(lambda container: isinstance(container, type), process)

  def locateprocess(self, locate, process):
    "Search for all embedded containers and process them"
    for container in self.contents:
      container.locateprocess(locate, process)
      if locate(container):
        process(container)

  def recursivesearch(self, locate, recursive, process):
    "Perform a recursive search in the container."
    for container in self.contents:
      if recursive(container):
        container.recursivesearch(locate, recursive, process)
      if locate(container):
        process(container)

  def extracttext(self):
    "Extract all text from allowed containers."
    result = ''
    constants = ContainerExtractor(ContainerConfig.extracttext).extract(self)
    return ''.join(constant.string for constant in constants)

  def group(self, index, group, isingroup):
    "Group some adjoining elements into a group"
    if index >= len(self.contents):
      return
    if hasattr(self.contents[index], 'grouped'):
      return
    while index < len(self.contents) and isingroup(self.contents[index]):
      self.contents[index].grouped = True
      group.contents.append(self.contents[index])
      self.contents.pop(index)
    self.contents.insert(index, group)

  def remove(self, index):
    "Remove a container but leave its contents"
    container = self.contents[index]
    self.contents.pop(index)
    while len(container.contents) > 0:
      self.contents.insert(index, container.contents.pop())

  def tree(self, level = 0):
    "Show in a tree"
    Trace.debug("  " * level + unicode(self))
    for container in self.contents:
      container.tree(level + 1)

  def getparameter(self, name):
    "Get the value of a parameter, if present."
    if not name in self.parameters:
      return None
    return self.parameters[name]

  def getparameterlist(self, name):
    "Get the value of a comma-separated parameter as a list."
    paramtext = self.getparameter(name)
    if not paramtext:
      return []
    return paramtext.split(',')

  def hasemptyoutput(self):
    "Check if the parent's output is empty."
    current = self.parent
    while current:
      if current.output.isempty():
        return True
      current = current.parent
    return False

  def __unicode__(self):
    "Get a description"
    if not self.begin:
      return self.__class__.__name__
    return self.__class__.__name__ + '@' + unicode(self.begin)

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class BlackBox(Container):
  "A container that does not output anything"

  def __init__(self):
    self.parser = LoneCommand()
    self.output = EmptyOutput()
    self.contents = []

class StringContainer(Container):
  "A container for a single string"

  parsed = None

  def __init__(self):
    self.parser = StringParser()
    self.output = StringOutput()
    self.string = ''

  def process(self):
    "Replace special chars from the contents."
    if self.parsed:
      self.string = self.replacespecial(self.parsed)
      self.parsed = None

  def replacespecial(self, line):
    "Replace all special chars from a line"
    replaced = self.escape(line, EscapeConfig.entities)
    replaced = self.changeline(replaced)
    if ContainerConfig.string['startcommand'] in replaced and len(replaced) > 1:
      # unprocessed commands
      if self.begin:
        message = 'Unknown command at ' + unicode(self.begin) + ': '
      else:
        message = 'Unknown command: '
      Trace.error(message + replaced.strip())
    return replaced

  def changeline(self, line):
    line = self.escape(line, EscapeConfig.chars)
    return line

  def extracttext(self):
    "Return all text."
    return self.string

  def __unicode__(self):
    "Return a printable representation."
    result = 'StringContainer'
    if self.begin:
      result += '@' + unicode(self.begin)
    ellipsis = '...'
    if len(self.string.strip()) <= 15:
      ellipsis = ''
    return result + ' (' + self.string.strip()[:15] + ellipsis + ')'

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class Constant(StringContainer):
  "A constant string"

  def __init__(self, text):
    self.contents = []
    self.string = text
    self.output = StringOutput()

  def __unicode__(self):
    return 'Constant: ' + self.string

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class DocumentParameters(object):
  "Global parameters for the document."

  displaymode = False


class FormulaParser(Parser):
  "Parses a formula"

  def parseheader(self, reader):
    "See if the formula is inlined"
    self.begin = reader.linenumber + 1
    type = self.parsetype(reader)
    if not type:
      reader.nextline()
      type = self.parsetype(reader)
      if not type:
        Trace.error('Unknown formula type in ' + reader.currentline().strip())
        return ['unknown']
    return [type]

  def parsetype(self, reader):
    "Get the formula type from the first line."
    if reader.currentline().find(FormulaConfig.starts['simple']) >= 0:
      return 'inline'
    if reader.currentline().find(FormulaConfig.starts['complex']) >= 0:
      return 'block'
    if reader.currentline().find(FormulaConfig.starts['unnumbered']) >= 0:
      return 'block'
    if reader.currentline().find(FormulaConfig.starts['beginbefore']) >= 0:
      return 'numbered'
    return None

  def parse(self, reader):
    "Parse the formula until the end"
    formula = self.parseformula(reader)
    while not reader.currentline().startswith(self.ending):
      stripped = reader.currentline().strip()
      if len(stripped) > 0:
        Trace.error('Unparsed formula line ' + stripped)
      reader.nextline()
    reader.nextline()
    return formula

  def parseformula(self, reader):
    "Parse the formula contents"
    simple = FormulaConfig.starts['simple']
    if simple in reader.currentline():
      rest = reader.currentline().split(simple, 1)[1]
      if simple in rest:
        # formula is $...$
        return self.parsesingleliner(reader, simple, simple)
      # formula is multiline $...$
      return self.parsemultiliner(reader, simple, simple)
    if FormulaConfig.starts['complex'] in reader.currentline():
      # formula of the form \[...\]
      return self.parsemultiliner(reader, FormulaConfig.starts['complex'],
          FormulaConfig.endings['complex'])
    beginbefore = FormulaConfig.starts['beginbefore']
    beginafter = FormulaConfig.starts['beginafter']
    if beginbefore in reader.currentline():
      if reader.currentline().strip().endswith(beginafter):
        current = reader.currentline().strip()
        endsplit = current.split(beginbefore)[1].split(beginafter)
        startpiece = beginbefore + endsplit[0] + beginafter
        endbefore = FormulaConfig.endings['endbefore']
        endafter = FormulaConfig.endings['endafter']
        endpiece = endbefore + endsplit[0] + endafter
        return startpiece + self.parsemultiliner(reader, startpiece, endpiece) + endpiece
      Trace.error('Missing ' + beginafter + ' in ' + reader.currentline())
      return ''
    begincommand = FormulaConfig.starts['command']
    beginbracket = FormulaConfig.starts['bracket']
    if begincommand in reader.currentline() and beginbracket in reader.currentline():
      endbracket = FormulaConfig.endings['bracket']
      return self.parsemultiliner(reader, beginbracket, endbracket)
    Trace.error('Formula beginning ' + reader.currentline() + ' is unknown')
    return ''

  def parsesingleliner(self, reader, start, ending):
    "Parse a formula in one line"
    line = reader.currentline().strip()
    if not start in line:
      Trace.error('Line ' + line + ' does not contain formula start ' + start)
      return ''
    if not line.endswith(ending):
      Trace.error('Formula ' + line + ' does not end with ' + ending)
      return ''
    index = line.index(start)
    rest = line[index + len(start):-len(ending)]
    reader.nextline()
    return rest

  def parsemultiliner(self, reader, start, ending):
    "Parse a formula in multiple lines"
    formula = ''
    line = reader.currentline()
    if not start in line:
      Trace.error('Line ' + line.strip() + ' does not contain formula start ' + start)
      return ''
    index = line.index(start)
    line = line[index + len(start):].strip()
    while not line.endswith(ending):
      formula += line + '\n'
      reader.nextline()
      line = reader.currentline()
    formula += line[:-len(ending)]
    reader.nextline()
    return formula


class FormulaBit(Container):
  "A bit of a formula"

  type = None
  size = 1
  original = ''

  def __init__(self):
    "The formula bit type can be 'alpha', 'number', 'font'."
    self.contents = []
    self.output = ContentsOutput()

  def setfactory(self, factory):
    "Set the internal formula factory."
    self.factory = factory
    return self

  def add(self, bit):
    "Add any kind of formula bit already processed"
    self.contents.append(bit)
    self.original += bit.original
    bit.parent = self

  def skiporiginal(self, string, pos):
    "Skip a string and add it to the original formula"
    self.original += string
    if not pos.checkskip(string):
      Trace.error('String ' + string + ' not at ' + pos.identifier())

  def computesize(self):
    "Compute the size of the bit as the max of the sizes of all contents."
    if len(self.contents) == 0:
      return 1
    self.size = max([element.size for element in self.contents])
    return self.size

  def clone(self):
    "Return a copy of itself."
    return self.factory.parseformula(self.original)

  def __unicode__(self):
    "Get a string representation"
    return self.__class__.__name__ + ' read in ' + self.original

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class TaggedBit(FormulaBit):
  "A tagged string in a formula"

  def constant(self, constant, tag):
    "Set the constant and the tag"
    self.output = TaggedOutput().settag(tag)
    self.add(FormulaConstant(constant))
    return self

  def complete(self, contents, tag, breaklines = False):
    "Set the constant and the tag"
    self.contents = contents
    self.output = TaggedOutput().settag(tag, breaklines)
    return self

  def selfcomplete(self, tag):
    "Set the self-closing tag, no contents (as in <hr/>)."
    self.output = TaggedOutput().settag(tag, empty = True)
    return self

class FormulaConstant(Constant):
  "A constant string in a formula"

  def __init__(self, string):
    "Set the constant string"
    Constant.__init__(self, string)
    self.original = string
    self.size = 1
    self.type = None

  def computesize(self):
    "Compute the size of the constant: always 1."
    return self.size

  def clone(self):
    "Return a copy of itself."
    return FormulaConstant(self.original)

  def __unicode__(self):
    "Return a printable representation."
    return 'Formula constant: ' + self.string

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class RawText(FormulaBit):
  "A bit of text inside a formula"

  def detect(self, pos):
    "Detect a bit of raw text"
    return pos.current().isalpha()

  def parsebit(self, pos):
    "Parse alphabetic text"
    alpha = pos.globalpha()
    self.add(FormulaConstant(alpha))
    self.type = 'alpha'

class FormulaSymbol(FormulaBit):
  "A symbol inside a formula"

  modified = FormulaConfig.modified
  unmodified = FormulaConfig.unmodified['characters']

  def detect(self, pos):
    "Detect a symbol"
    if pos.current() in FormulaSymbol.unmodified:
      return True
    if pos.current() in FormulaSymbol.modified:
      return True
    return False

  def parsebit(self, pos):
    "Parse the symbol"
    if pos.current() in FormulaSymbol.unmodified:
      self.addsymbol(pos.current(), pos)
      return
    if pos.current() in FormulaSymbol.modified:
      self.addsymbol(FormulaSymbol.modified[pos.current()], pos)
      return
    Trace.error('Symbol ' + pos.current() + ' not found')

  def addsymbol(self, symbol, pos):
    "Add a symbol"
    self.skiporiginal(pos.current(), pos)
    self.contents.append(FormulaConstant(symbol))

class FormulaNumber(FormulaBit):
  "A string of digits in a formula"

  def detect(self, pos):
    "Detect a digit"
    return pos.current().isdigit()

  def parsebit(self, pos):
    "Parse a bunch of digits"
    digits = pos.glob(lambda: pos.current().isdigit())
    self.add(FormulaConstant(digits))
    self.type = 'number'

class Comment(FormulaBit):
  "A LaTeX comment: % to the end of the line."

  start = FormulaConfig.starts['comment']

  def detect(self, pos):
    "Detect the %."
    return pos.current() == self.start

  def parsebit(self, pos):
    "Parse to the end of the line."
    self.original += pos.globincluding('\n')

class WhiteSpace(FormulaBit):
  "Some white space inside a formula."

  def detect(self, pos):
    "Detect the white space."
    return pos.current().isspace()

  def parsebit(self, pos):
    "Parse all whitespace."
    self.original += pos.skipspace()

  def __unicode__(self):
    "Return a printable representation."
    return 'Whitespace: *' + self.original + '*'

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class Bracket(FormulaBit):
  "A {} bracket inside a formula"

  start = FormulaConfig.starts['bracket']
  ending = FormulaConfig.endings['bracket']

  def __init__(self):
    "Create a (possibly literal) new bracket"
    FormulaBit.__init__(self)
    self.inner = None

  def detect(self, pos):
    "Detect the start of a bracket"
    return pos.checkfor(self.start)

  def parsebit(self, pos):
    "Parse the bracket"
    self.parsecomplete(pos, self.innerformula)
    return self

  def parsetext(self, pos):
    "Parse a text bracket"
    self.parsecomplete(pos, self.innertext)
    return self

  def parseliteral(self, pos):
    "Parse a literal bracket"
    self.parsecomplete(pos, self.innerliteral)
    return self

  def parsecomplete(self, pos, innerparser):
    "Parse the start and end marks"
    if not pos.checkfor(self.start):
      Trace.error('Bracket should start with ' + self.start + ' at ' + pos.identifier())
      return None
    self.skiporiginal(self.start, pos)
    pos.pushending(self.ending)
    innerparser(pos)
    self.original += pos.popending(self.ending)
    self.computesize()

  def innerformula(self, pos):
    "Parse a whole formula inside the bracket"
    while not pos.finished():
      self.add(self.factory.parseany(pos))

  def innertext(self, pos):
    "Parse some text inside the bracket, following textual rules."
    specialchars = list(FormulaConfig.symbolfunctions.keys())
    specialchars.append(FormulaConfig.starts['command'])
    specialchars.append(FormulaConfig.starts['bracket'])
    specialchars.append(Comment.start)
    while not pos.finished():
      if pos.current() in specialchars:
        self.add(self.factory.parseany(pos))
        if pos.checkskip(' '):
          self.original += ' '
      else:
        self.add(FormulaConstant(pos.skipcurrent()))

  def innerliteral(self, pos):
    "Parse a literal inside the bracket, which does not generate HTML."
    self.literal = ''
    while not pos.finished() and not pos.current() == self.ending:
      if pos.current() == self.start:
        self.parseliteral(pos)
      else:
        self.literal += pos.skipcurrent()
    self.original += self.literal

class SquareBracket(Bracket):
  "A [] bracket inside a formula"

  start = FormulaConfig.starts['squarebracket']
  ending = FormulaConfig.endings['squarebracket']

  def clone(self):
    "Return a new square bracket with the same contents."
    bracket = SquareBracket()
    bracket.contents = self.contents
    return bracket


class MathsProcessor(object):
  "A processor for a maths construction inside the FormulaProcessor."

  def process(self, contents, index):
    "Process an element inside a formula."
    Trace.error('Unimplemented process() in ' + unicode(self))

  def __unicode__(self):
    "Return a printable description."
    return 'Maths processor ' + self.__class__.__name__

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class FormulaProcessor(object):
  "A processor specifically for formulas."

  processors = []

  def process(self, bit):
    "Process the contents of every formula bit, recursively."
    self.processcontents(bit)
    self.processinsides(bit)
    self.traversewhole(bit)

  def processcontents(self, bit):
    "Process the contents of a formula bit."
    if not isinstance(bit, FormulaBit):
      return
    bit.process()
    for element in bit.contents:
      self.processcontents(element)

  def processinsides(self, bit):
    "Process the insides (limits, brackets) in a formula bit."
    if not isinstance(bit, FormulaBit):
      return
    for index, element in enumerate(bit.contents):
      for processor in self.processors:
        processor.process(bit.contents, index)
      # continue with recursive processing
      self.processinsides(element)

  def traversewhole(self, formula):
    "Traverse over the contents to alter variables and space units."
    last = None
    for bit, contents in self.traverse(formula):
      if bit.type == 'alpha':
        self.italicize(bit, contents)
      elif bit.type == 'font' and last and last.type == 'number':
        bit.contents.insert(0, FormulaConstant(u'\u2009'))
      last = bit

  def traverse(self, bit):
    "Traverse a formula and yield a flattened structure of (bit, list) pairs."
    for element in bit.contents:
      if hasattr(element, 'type') and element.type:
        yield (element, bit.contents)
      elif isinstance(element, FormulaBit):
        for pair in self.traverse(element):
          yield pair

  def italicize(self, bit, contents):
    "Italicize the given bit of text."
    index = contents.index(bit)
    contents[index] = TaggedBit().complete([bit], 'i')


class Formula(Container):
  "A LaTeX formula"

  def __init__(self):
    self.parser = FormulaParser()
    self.output = TaggedOutput().settag('span class="formula"')

  def process(self):
    "Convert the formula to tags"
    if self.header[0] == 'inline':
      DocumentParameters.displaymode = False
    else:
      DocumentParameters.displaymode = True
      self.output.settag('div class="formula"', True)
    self.classic()

  def classic(self):
    "Make the contents using classic output generation with XHTML and CSS."
    whole = FormulaFactory().parseformula(self.parsed)
    FormulaProcessor().process(whole)
    whole.parent = self
    self.contents = [whole]

  def parse(self, pos):
    "Parse using a parse position instead of self.parser."
    if pos.checkskip('$$'):
      self.parsedollarblock(pos)
    elif pos.checkskip('$'):
      self.parsedollarinline(pos)
    elif pos.checkskip('\\('):
      self.parseinlineto(pos, '\\)')
    elif pos.checkskip('\\['):
      self.parseblockto(pos, '\\]')
    else:
      pos.error('Unparseable formula')
    self.process()
    return self

  def parsedollarinline(self, pos):
    "Parse a $...$ formula."
    self.header = ['inline']
    self.parsedollar(pos)

  def parsedollarblock(self, pos):
    "Parse a $$...$$ formula."
    self.header = ['block']
    self.parsedollar(pos)
    if not pos.checkskip('$'):
      pos.error('Formula should be $$...$$, but last $ is missing.')

  def parsedollar(self, pos):
    "Parse to the next $."
    pos.pushending('$')
    self.parsed = pos.globexcluding('$')
    pos.popending('$')

  def parseinlineto(self, pos, limit):
    "Parse a \\(...\\) formula."
    self.header = ['inline']
    self.parseupto(pos, limit)

  def parseblockto(self, pos, limit):
    "Parse a \\[...\\] formula."
    self.header = ['block']
    self.parseupto(pos, limit)

  def parseupto(self, pos, limit):
    "Parse a formula that ends with the given command."
    pos.pushending(limit)
    self.parsed = pos.glob(lambda: True)
    pos.popending(limit)

  def __unicode__(self):
    "Return a printable representation."
    if self.partkey and self.partkey.number:
      return 'Formula (' + self.partkey.number + ')'
    return 'Unnumbered formula'

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class WholeFormula(FormulaBit):
  "Parse a whole formula"

  def detect(self, pos):
    "Not outside the formula is enough."
    return not pos.finished()

  def parsebit(self, pos):
    "Parse with any formula bit"
    while not pos.finished():
      self.add(self.factory.parseany(pos))

class FormulaFactory(object):
  "Construct bits of formula"

  # bit types will be appended later
  types = [FormulaSymbol, RawText, FormulaNumber, Bracket, Comment, WhiteSpace]
  skippedtypes = [Comment, WhiteSpace]
  defining = False

  def __init__(self):
    "Initialize the map of instances."
    self.instances = dict()

  def detecttype(self, type, pos):
    "Detect a bit of a given type."
    if pos.finished():
      return False
    return self.instance(type).detect(pos)

  def instance(self, type):
    "Get an instance of the given type."
    if not type in self.instances or not self.instances[type]:
      self.instances[type] = self.create(type)
    return self.instances[type]

  def create(self, type):
    "Create a new formula bit of the given type."
    return Cloner.create(type).setfactory(self)

  def clearskipped(self, pos):
    "Clear any skipped types."
    while not pos.finished():
      if not self.skipany(pos):
        return
    return

  def skipany(self, pos):
    "Skip any skipped types."
    for type in self.skippedtypes:
      if self.instance(type).detect(pos):
        return self.parsetype(type, pos)
    return None

  def parseany(self, pos):
    "Parse any formula bit at the current location."
    for type in self.types + self.skippedtypes:
      if self.detecttype(type, pos):
        return self.parsetype(type, pos)
    Trace.error('Unrecognized formula at ' + pos.identifier())
    return FormulaConstant(pos.skipcurrent())

  def parsetype(self, type, pos):
    "Parse the given type and return it."
    bit = self.instance(type)
    self.instances[type] = None
    returnedbit = bit.parsebit(pos)
    if returnedbit:
      return returnedbit.setfactory(self)
    return bit

  def parseformula(self, formula):
    "Parse a string of text that contains a whole formula."
    pos = TextPosition(formula)
    whole = self.create(WholeFormula)
    if whole.detect(pos):
      whole.parsebit(pos)
      return whole
    # no formula found
    if not pos.finished():
      Trace.error('Unknown formula at: ' + pos.identifier())
      whole.add(TaggedBit().constant(formula, 'span class="unknown"'))
    return whole


class FormulaCommand(FormulaBit):
  "A LaTeX command inside a formula"

  types = []
  start = FormulaConfig.starts['command']
  commandmap = None

  def detect(self, pos):
    "Find the current command."
    return pos.checkfor(FormulaCommand.start)

  def parsebit(self, pos):
    "Parse the command."
    command = self.extractcommand(pos)
    bit = self.parsewithcommand(command, pos)
    if bit:
      return bit
    if command.startswith('\\up') or command.startswith('\\Up'):
      upgreek = self.parseupgreek(command, pos)
      if upgreek:
        return upgreek
    if not self.factory.defining:
      Trace.error('Unknown command ' + command)
    self.output = TaggedOutput().settag('span class="unknown"')
    self.add(FormulaConstant(command))
    return None

  def parsewithcommand(self, command, pos):
    "Parse the command type once we have the command."
    for type in FormulaCommand.types:
      if command in type.commandmap:
        return self.parsecommandtype(command, type, pos)
    return None

  def parsecommandtype(self, command, type, pos):
    "Parse a given command type."
    bit = self.factory.create(type)
    bit.setcommand(command)
    returned = bit.parsebit(pos)
    if returned:
      return returned
    return bit

  def extractcommand(self, pos):
    "Extract the command from the current position."
    if not pos.checkskip(FormulaCommand.start):
      pos.error('Missing command start ' + FormulaCommand.start)
      return
    if pos.finished():
      return self.emptycommand(pos)
    if pos.current().isalpha():
      # alpha command
      command = FormulaCommand.start + pos.globalpha()
      # skip mark of short command
      pos.checkskip('*')
      return command
    # symbol command
    return FormulaCommand.start + pos.skipcurrent()

  def emptycommand(self, pos):
    """Check for an empty command: look for command disguised as ending.
    Special case against '{ \\{ \\} }' situation."""
    command = ''
    if not pos.isout():
      ending = pos.nextending()
      if ending and pos.checkskip(ending):
        command = ending
    return FormulaCommand.start + command

  def parseupgreek(self, command, pos):
    "Parse the Greek \\up command.."
    if len(command) < 4:
      return None
    if command.startswith('\\up'):
      upcommand = '\\' + command[3:]
    elif pos.checkskip('\\Up'):
      upcommand = '\\' + command[3:4].upper() + command[4:]
    else:
      Trace.error('Impossible upgreek command: ' + command)
      return
    upgreek = self.parsewithcommand(upcommand, pos)
    if upgreek:
      upgreek.type = 'font'
    return upgreek

class CommandBit(FormulaCommand):
  "A formula bit that includes a command"

  def setcommand(self, command):
    "Set the command in the bit"
    self.command = command
    if self.commandmap:
      self.original += command
      self.translated = self.commandmap[self.command]

  def parseparameter(self, pos):
    "Parse a parameter at the current position"
    self.factory.clearskipped(pos)
    if pos.finished():
      return None
    parameter = self.factory.parseany(pos)
    self.add(parameter)
    return parameter

  def parsesquare(self, pos):
    "Parse a square bracket"
    self.factory.clearskipped(pos)
    if not self.factory.detecttype(SquareBracket, pos):
      return None
    bracket = self.factory.parsetype(SquareBracket, pos)
    self.add(bracket)
    return bracket

  def parseliteral(self, pos):
    "Parse a literal bracket."
    self.factory.clearskipped(pos)
    if not self.factory.detecttype(Bracket, pos):
      if not pos.isvalue():
        Trace.error('No literal parameter found at: ' + pos.identifier())
        return None
      return pos.globvalue()
    bracket = Bracket().setfactory(self.factory)
    self.add(bracket.parseliteral(pos))
    return bracket.literal

  def parsesquareliteral(self, pos):
    "Parse a square bracket literally."
    self.factory.clearskipped(pos)
    if not self.factory.detecttype(SquareBracket, pos):
      return None
    bracket = SquareBracket().setfactory(self.factory)
    self.add(bracket.parseliteral(pos))
    return bracket.literal

  def parsetext(self, pos):
    "Parse a text parameter."
    self.factory.clearskipped(pos)
    if not self.factory.detecttype(Bracket, pos):
      Trace.error('No text parameter for ' + self.command)
      return None
    bracket = Bracket().setfactory(self.factory).parsetext(pos)
    self.add(bracket)
    return bracket

class EmptyCommand(CommandBit):
  "An empty command (without parameters)"

  commandmap = FormulaConfig.commands

  def parsebit(self, pos):
    "Parse a command without parameters"
    self.contents = [FormulaConstant(self.translated)]

class SpacedCommand(CommandBit):
  """An empty command which should have math spacing in formulas."""

  commandmap = FormulaConfig.spacedcommands

  def parsebit(self, pos):
    "Place as contents the command translated and spaced."
    # pad with MEDIUM MATHEMATICAL SPACE (4/18 em): too wide in STIX fonts :(
    # self.contents = [FormulaConstant(u'\u205f' + self.translated + u'\u205f')]
    # pad with THIN SPACE (1/5 em)
    self.contents = [FormulaConstant(u'\u2009' + self.translated + u'\u2009')]

class AlphaCommand(EmptyCommand):
  """A command without parameters whose result is alphabetical."""

  commandmap = FormulaConfig.alphacommands
  greek_capitals = ('\\Xi', '\\Theta', '\\Pi', '\\Sigma', '\\Gamma',
                    '\\Lambda', '\\Phi', '\\Psi', '\\Delta',
                    '\\Upsilon', '\\Omega')

  def parsebit(self, pos):
    "Parse the command and set type to alpha"
    EmptyCommand.parsebit(self, pos)
    if self.command not in self.greek_capitals:
        # Greek Capital letters are upright in LaTeX default math-style.
        # TODO: use italic, like in MathML and "iso" math-style?
        self.type = 'alpha'

class OneParamFunction(CommandBit):
  "A function of one parameter"

  commandmap = FormulaConfig.onefunctions
  simplified = False

  def parsebit(self, pos):
    "Parse a function with one parameter"
    self.output = TaggedOutput().settag(self.translated)
    self.parseparameter(pos)
    self.simplifyifpossible()

  def simplifyifpossible(self):
    "Try to simplify to a single character."
    if self.original in self.commandmap:
      self.output = FixedOutput()
      self.html = [self.commandmap[self.original]]
      self.simplified = True

class SymbolFunction(CommandBit):
  "Find a function which is represented by a symbol (like _ or ^)"

  commandmap = FormulaConfig.symbolfunctions

  def detect(self, pos):
    "Find the symbol"
    return pos.current() in SymbolFunction.commandmap

  def parsebit(self, pos):
    "Parse the symbol"
    self.setcommand(pos.current())
    pos.skip(self.command)
    self.output = TaggedOutput().settag(self.translated)
    self.parseparameter(pos)

class TextFunction(CommandBit):
  "A function where parameters are read as text."

  commandmap = FormulaConfig.textfunctions

  def parsebit(self, pos):
    "Parse a text parameter"
    self.output = TaggedOutput().settag(self.translated)
    self.parsetext(pos)

  def process(self):
    "Set the type to font"
    self.type = 'font'

class LabelFunction(CommandBit):
  "A function that acts as a label"

  commandmap = FormulaConfig.labelfunctions

  def parsebit(self, pos):
    "Parse a literal parameter"
    self.key = self.parseliteral(pos)

  def process(self):
    "Add an anchor with the label contents."
    self.type = 'font'
    self.label = Label().create(' ', self.key, type = 'eqnumber')
    self.contents = [self.label]
    # store as a Label so we know it's been seen
    Label.names[self.key] = self.label

class FontFunction(OneParamFunction):
  """A function of one parameter that changes the font."""
  # TODO: keep letters italic with \boldsymbol.

  commandmap = FormulaConfig.fontfunctions

  def process(self):
    "Simplify if possible using a single character."
    self.type = 'font'
    self.simplifyifpossible()

FormulaFactory.types += [FormulaCommand, SymbolFunction]
FormulaCommand.types = [
    AlphaCommand, EmptyCommand, OneParamFunction, FontFunction,
    LabelFunction, TextFunction, SpacedCommand]


class BigBracket(object):
  "A big bracket generator."

  def __init__(self, size, bracket, alignment='l'):
    "Set the size and symbol for the bracket."
    self.size = size
    self.original = bracket
    self.alignment = alignment
    self.pieces = None
    if bracket in FormulaConfig.bigbrackets:
      self.pieces = FormulaConfig.bigbrackets[bracket]

  def getpiece(self, index):
    "Return the nth piece for the bracket."
    function = getattr(self, 'getpiece' + unicode(len(self.pieces)))
    return function(index)

  def getpiece1(self, index):
    "Return the only piece for a single-piece bracket."
    return self.pieces[0]

  def getpiece3(self, index):
    "Get the nth piece for a 3-piece bracket: parenthesis or square bracket."
    if index == 0:
      return self.pieces[0]
    if index == self.size - 1:
      return self.pieces[-1]
    return self.pieces[1]

  def getpiece4(self, index):
    "Get the nth piece for a 4-piece bracket: curly bracket."
    if index == 0:
      return self.pieces[0]
    if index == self.size - 1:
      return self.pieces[3]
    if index == (self.size - 1)/2:
      return self.pieces[2]
    return self.pieces[1]

  def getcell(self, index):
    "Get the bracket piece as an array cell."
    piece = self.getpiece(index)
    span = 'span class="bracket align-' + self.alignment + '"'
    return TaggedBit().constant(piece, span)

  def getcontents(self):
    "Get the bracket as an array or as a single bracket."
    if self.size == 1 or not self.pieces:
      return self.getsinglebracket()
    rows = []
    for index in range(self.size):
      cell = self.getcell(index)
      rows.append(TaggedBit().complete([cell], 'span class="arrayrow"'))
    return [TaggedBit().complete(rows, 'span class="array"')]

  def getsinglebracket(self):
    "Return the bracket as a single sign."
    if self.original == '.':
      return [TaggedBit().constant('', 'span class="emptydot"')]
    return [TaggedBit().constant(self.original, 'span class="stretchy"')]


class FormulaEquation(CommandBit):
  "A simple numbered equation."

  piece = 'equation'

  def parsebit(self, pos):
    "Parse the array"
    self.output = ContentsOutput()
    self.add(self.factory.parsetype(WholeFormula, pos))

class FormulaCell(FormulaCommand):
  "An array cell inside a row"

  def setalignment(self, alignment):
    self.alignment = alignment
    self.output = TaggedOutput().settag('span class="arraycell align-' + alignment +'"', True)
    return self

  def parsebit(self, pos):
    self.factory.clearskipped(pos)
    if pos.finished():
      return
    self.add(self.factory.parsetype(WholeFormula, pos))

class FormulaRow(FormulaCommand):
  "An array row inside an array"

  cellseparator = FormulaConfig.array['cellseparator']

  def setalignments(self, alignments):
    self.alignments = alignments
    self.output = TaggedOutput().settag('span class="arrayrow"', True)
    return self

  def parsebit(self, pos):
    "Parse a whole row"
    index = 0
    pos.pushending(self.cellseparator, optional=True)
    while not pos.finished():
      cell = self.createcell(index)
      cell.parsebit(pos)
      self.add(cell)
      index += 1
      pos.checkskip(self.cellseparator)
    if len(self.contents) == 0:
      self.output = EmptyOutput()

  def createcell(self, index):
    "Create the cell that corresponds to the given index."
    alignment = self.alignments[index % len(self.alignments)]
    return self.factory.create(FormulaCell).setalignment(alignment)

class MultiRowFormula(CommandBit):
  "A formula with multiple rows."

  def parserows(self, pos):
    "Parse all rows, finish when no more row ends"
    self.rows = []
    first = True
    for row in self.iteraterows(pos):
      if first:
        first = False
      else:
        # intersparse empty rows
        self.addempty()
      row.parsebit(pos)
      self.addrow(row)
    self.size = len(self.rows)

  def iteraterows(self, pos):
    "Iterate over all rows, end when no more row ends"
    rowseparator = FormulaConfig.array['rowseparator']
    while True:
      pos.pushending(rowseparator, True)
      row = self.factory.create(FormulaRow)
      yield row.setalignments(self.alignments)
      if pos.checkfor(rowseparator):
        self.original += pos.popending(rowseparator)
      else:
        return

  def addempty(self):
    "Add an empty row."
    row = self.factory.create(FormulaRow).setalignments(self.alignments)
    for index, originalcell in enumerate(self.rows[-1].contents):
      cell = row.createcell(index)
      cell.add(FormulaConstant(u' '))
      row.add(cell)
    self.addrow(row)

  def addrow(self, row):
    "Add a row to the contents and to the list of rows."
    self.rows.append(row)
    self.add(row)

class FormulaArray(MultiRowFormula):
  "An array within a formula"

  piece = 'array'

  def parsebit(self, pos):
    "Parse the array"
    self.output = TaggedOutput().settag('span class="array"', False)
    self.parsealignments(pos)
    self.parserows(pos)

  def parsealignments(self, pos):
    "Parse the different alignments"
    # vertical
    self.valign = 'c'
    literal = self.parsesquareliteral(pos)
    if literal:
      self.valign = literal
    # horizontal
    literal = self.parseliteral(pos)
    self.alignments = []
    for l in literal:
      self.alignments.append(l)

class FormulaMatrix(MultiRowFormula):
  "A matrix (array with center alignment)."

  piece = 'matrix'

  def parsebit(self, pos):
    "Parse the matrix, set alignments to 'c'."
    self.output = TaggedOutput().settag('span class="array"', False)
    self.valign = 'c'
    self.alignments = ['c']
    self.parserows(pos)

class FormulaCases(MultiRowFormula):
  "A cases statement"

  piece = 'cases'

  def parsebit(self, pos):
    "Parse the cases"
    self.output = ContentsOutput()
    self.alignments = ['l', 'l']
    self.parserows(pos)
    for row in self.contents:
      for cell in row.contents:
        cell.output.settag('span class="case align-l"', True)
        cell.contents.append(FormulaConstant(u' '))
    array = TaggedBit().complete(self.contents, 'span class="bracketcases"', True)
    brace = BigBracket(len(self.contents), '{', 'l')
    self.contents = brace.getcontents() + [array]

class EquationEnvironment(MultiRowFormula):
  "A \\begin{}...\\end equation environment with rows and cells."

  def parsebit(self, pos):
    "Parse the whole environment."
    environment = self.piece.replace('*', '')
    self.output = TaggedOutput().settag(
                    'span class="environment %s"'%environment, False)
    if environment in FormulaConfig.environments:
      self.alignments = FormulaConfig.environments[environment]
    else:
      Trace.error('Unknown equation environment ' + self.piece)
      # print in red
      self.output = TaggedOutput().settag('span class="unknown"')
      self.add(FormulaConstant('\\begin{%s} '%environment))

      self.alignments = ['l']
    self.parserows(pos)

class BeginCommand(CommandBit):
  "A \\begin{}...\\end command and what it entails (array, cases, aligned)"

  commandmap = {FormulaConfig.array['begin']:''}

  types = [FormulaEquation, FormulaArray, FormulaCases, FormulaMatrix]

  def parsebit(self, pos):
    "Parse the begin command"
    command = self.parseliteral(pos)
    bit = self.findbit(command)
    ending = FormulaConfig.array['end'] + '{' + command + '}'
    pos.pushending(ending)
    bit.parsebit(pos)
    self.add(bit)
    self.original += pos.popending(ending)
    self.size = bit.size

  def findbit(self, piece):
    "Find the command bit corresponding to the \\begin{piece}"
    for type in BeginCommand.types:
      if piece.replace('*', '') == type.piece:
        return self.factory.create(type)
    bit = self.factory.create(EquationEnvironment)
    bit.piece = piece
    return bit

FormulaCommand.types += [BeginCommand]


class CombiningFunction(OneParamFunction):

  commandmap = FormulaConfig.combiningfunctions

  def parsebit(self, pos):
    "Parse a combining function."
    combining = self.translated
    parameter = self.parsesingleparameter(pos)
    if not parameter:
      Trace.error('Missing parameter for combining function ' + self.command)
      return
    # Trace.message('apply %s to %r'%(self.command, parameter.extracttext()))
    # parameter.tree()
    if not isinstance(parameter, FormulaConstant):
      try:
        extractor = ContainerExtractor(ContainerConfig.extracttext)
        parameter = extractor.extract(parameter)[0]
      except IndexError:
        Trace.error('No base character found for "%s".' % self.command)
        return
    # Trace.message('  basechar: %r' % parameter.string)
    # Insert combining character after the first character:
    if parameter.string.startswith(u'\u2009'):
        i = 2 # skip padding by SpacedCommand and FormulaConfig.modified
    else:
        i = 1
    parameter.string = parameter.string[:i] + combining + parameter.string[i:]
    # Use pre-composed characters if possible: \not{=} -> ≠, say.
    parameter.string = unicodedata.normalize('NFC', parameter.string)

  def parsesingleparameter(self, pos):
    "Parse a parameter, or a single letter."
    self.factory.clearskipped(pos)
    if pos.finished():
      return None
    return self.parseparameter(pos)

class OversetFunction(OneParamFunction):
  "A function that decorates some bit of text with an overset."

  commandmap = FormulaConfig.oversetfunctions

  def parsebit(self, pos):
    "Parse an overset-function"
    symbol = self.translated
    self.symbol = TaggedBit().constant(symbol, 'sup')
    self.parameter = self.parseparameter(pos)
    self.output = TaggedOutput().settag('span class="embellished"')
    self.contents.insert(0, self.symbol)
    self.parameter.output = TaggedOutput().settag('span class="base"')
    self.simplifyifpossible()

class UndersetFunction(OneParamFunction):
  "A function that decorates some bit of text with an underset."

  commandmap = FormulaConfig.undersetfunctions

  def parsebit(self, pos):
    "Parse an underset-function"
    symbol = self.translated
    self.symbol = TaggedBit().constant(symbol, 'sub')
    self.parameter = self.parseparameter(pos)
    self.output = TaggedOutput().settag('span class="embellished"')
    self.contents.insert(0, self.symbol)
    self.parameter.output = TaggedOutput().settag('span class="base"')
    self.simplifyifpossible()

class LimitCommand(EmptyCommand):
  "A command which accepts limits above and below, in display mode."

  commandmap = FormulaConfig.limitcommands

  def parsebit(self, pos):
    "Parse a limit command."
    self.output = TaggedOutput().settag('span class="limits"')
    symbol = self.translated
    self.contents.append(TaggedBit().constant(symbol, 'span class="limit"'))

class LimitPreviousCommand(LimitCommand):
  "A command to limit the previous command."

  commandmap = None

  def parsebit(self, pos):
    "Do nothing."
    self.output = TaggedOutput().settag('span class="limits"')
    self.factory.clearskipped(pos)

  def __unicode__(self):
    "Return a printable representation."
    return 'Limit previous command'

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class LimitsProcessor(MathsProcessor):
  "A processor for limits inside an element."

  def process(self, contents, index):
    "Process the limits for an element."
    if Options.simplemath:
      return
    if self.checklimits(contents, index):
      self.modifylimits(contents, index)
    if self.checkscript(contents, index) and self.checkscript(contents, index + 1):
      self.modifyscripts(contents, index)

  def checklimits(self, contents, index):
    "Check if the current position has a limits command."
    # TODO: check for \limits macro
    if not DocumentParameters.displaymode:
      return False
    if self.checkcommand(contents, index + 1, LimitPreviousCommand):
      self.limitsahead(contents, index)
      return False
    if not isinstance(contents[index], LimitCommand):
      return False
    return self.checkscript(contents, index + 1)

  def limitsahead(self, contents, index):
    "Limit the current element based on the next."
    contents[index + 1].add(contents[index].clone())
    contents[index].output = EmptyOutput()

  def modifylimits(self, contents, index):
    "Modify a limits commands so that the limits appear above and below."
    limited = contents[index]
    subscript = self.getlimit(contents, index + 1)
    if self.checkscript(contents, index + 1):
      superscript = self.getlimit(contents, index  + 1)
    else:
      superscript = TaggedBit().constant(u'\u2009', 'sup class="limit"')
    # fix order if source is x^i
    if subscript.command == '^':
        superscript, subscript = subscript, superscript
    limited.contents.append(subscript)
    limited.contents.insert(0, superscript)

  def getlimit(self, contents, index):
    "Get the limit for a limits command."
    limit = self.getscript(contents, index)
    limit.output.tag = limit.output.tag.replace('script', 'limit')
    return limit

  def modifyscripts(self, contents, index):
    "Modify the super- and subscript to appear vertically aligned."
    subscript = self.getscript(contents, index)
    # subscript removed so instead of index + 1 we get index again
    superscript = self.getscript(contents, index)
    # super-/subscript are reversed if source is x^i_j
    if subscript.command == '^':
        superscript, subscript = subscript, superscript
    scripts = TaggedBit().complete([superscript, subscript], 'span class="scripts"')
    contents.insert(index, scripts)

  def checkscript(self, contents, index):
    "Check if the current element is a sub- or superscript."
    return self.checkcommand(contents, index, SymbolFunction)

  def checkcommand(self, contents, index, type):
    "Check for the given type as the current element."
    if len(contents) <= index:
      return False
    return isinstance(contents[index], type)

  def getscript(self, contents, index):
    "Get the sub- or superscript."
    bit = contents[index]
    bit.output.tag += ' class="script"'
    del contents[index]
    return bit

class BracketCommand(OneParamFunction):
  "A command which defines a bracket."

  commandmap = FormulaConfig.bracketcommands

  def parsebit(self, pos):
    "Parse the bracket."
    OneParamFunction.parsebit(self, pos)

  def create(self, direction, character):
    "Create the bracket for the given character."
    self.original = character
    self.command = '\\' + direction
    self.contents = [FormulaConstant(character)]
    return self

class BracketProcessor(MathsProcessor):
  "A processor for bracket commands."

  def process(self, contents, index):
    "Convert the bracket using Unicode pieces, if possible."
    if Options.simplemath:
      return
    if self.checkleft(contents, index):
      return self.processleft(contents, index)

  def processleft(self, contents, index):
    "Process a left bracket."
    rightindex = self.findright(contents, index + 1)
    if not rightindex:
      return
    size = self.findmax(contents, index, rightindex)
    self.resize(contents[index], size)
    self.resize(contents[rightindex], size)

  def checkleft(self, contents, index):
    "Check if the command at the given index is left."
    return self.checkdirection(contents[index], '\\left')

  def checkright(self, contents, index):
    "Check if the command at the given index is right."
    return self.checkdirection(contents[index], '\\right')

  def checkdirection(self, bit, command):
    "Check if the given bit is the desired bracket command."
    if not isinstance(bit, BracketCommand):
      return False
    return bit.command == command

  def findright(self, contents, index):
    "Find the right bracket starting at the given index, or 0."
    depth = 1
    while index < len(contents):
      if self.checkleft(contents, index):
        depth += 1
      if self.checkright(contents, index):
        depth -= 1
      if depth == 0:
        return index
      index += 1
    return None

  def findmax(self, contents, leftindex, rightindex):
    "Find the max size of the contents between the two given indices."
    sliced = contents[leftindex:rightindex]
    return max([element.size for element in sliced])

  def resize(self, command, size):
    "Resize a bracket command to the given size."
    character = command.extracttext()
    alignment = command.command.replace('\\', '')
    bracket = BigBracket(size, character, alignment)
    command.output = ContentsOutput()
    command.contents = bracket.getcontents()


FormulaCommand.types += [OversetFunction, UndersetFunction,
                         CombiningFunction, LimitCommand, BracketCommand]

FormulaProcessor.processors += [
    LimitsProcessor(), BracketProcessor(),
    ]



class ParameterDefinition(object):
  "The definition of a parameter in a hybrid function."
  "[] parameters are optional, {} parameters are mandatory."
  "Each parameter has a one-character name, like {$1} or {$p}."
  "A parameter that ends in ! like {$p!} is a literal."
  "Example: [$1]{$p!} reads an optional parameter $1 and a literal mandatory parameter p."

  parambrackets = [('[', ']'), ('{', '}')]

  def __init__(self):
    self.name = None
    self.literal = False
    self.optional = False
    self.value = None
    self.literalvalue = None

  def parse(self, pos):
    "Parse a parameter definition: [$0], {$x}, {$1!}..."
    for (opening, closing) in ParameterDefinition.parambrackets:
      if pos.checkskip(opening):
        if opening == '[':
          self.optional = True
        if not pos.checkskip('$'):
          Trace.error('Wrong parameter name, did you mean $' + pos.current() + '?')
          return None
        self.name = pos.skipcurrent()
        if pos.checkskip('!'):
          self.literal = True
        if not pos.checkskip(closing):
          Trace.error('Wrong parameter closing ' + pos.skipcurrent())
          return None
        return self
    Trace.error('Wrong character in parameter template: ' + pos.skipcurrent())
    return None

  def read(self, pos, function):
    "Read the parameter itself using the definition."
    if self.literal:
      if self.optional:
        self.literalvalue = function.parsesquareliteral(pos)
      else:
        self.literalvalue = function.parseliteral(pos)
      if self.literalvalue:
        self.value = FormulaConstant(self.literalvalue)
    elif self.optional:
      self.value = function.parsesquare(pos)
    else:
      self.value = function.parseparameter(pos)

  def __unicode__(self):
    "Return a printable representation."
    result = 'param ' + self.name
    if self.value:
      result += ': ' + unicode(self.value)
    else:
      result += ' (empty)'
    return result

  if sys.version_info >= (3, 0):
    __str__ = __unicode__


class ParameterFunction(CommandBit):
  "A function with a variable number of parameters defined in a template."
  "The parameters are defined as a parameter definition."

  def readparams(self, readtemplate, pos):
    "Read the params according to the template."
    self.params = dict()
    for paramdef in self.paramdefs(readtemplate):
      paramdef.read(pos, self)
      self.params['$' + paramdef.name] = paramdef

  def paramdefs(self, readtemplate):
    "Read each param definition in the template"
    pos = TextPosition(readtemplate)
    while not pos.finished():
      paramdef = ParameterDefinition().parse(pos)
      if paramdef:
        yield paramdef

  def getparam(self, name):
    "Get a parameter as parsed."
    if not name in self.params:
      return None
    return self.params[name]

  def getvalue(self, name):
    "Get the value of a parameter."
    return self.getparam(name).value

  def getliteralvalue(self, name):
    "Get the literal value of a parameter."
    param = self.getparam(name)
    if not param or not param.literalvalue:
      return None
    return param.literalvalue

class HybridFunction(ParameterFunction):
  """
  A parameter function where the output is also defined using a template.
  The template can use a number of functions; each function has an associated
  tag.
  Example: [f0{$1},span class="fbox"] defines a function f0 which corresponds
  to a span of class fbox, yielding <span class="fbox">$1</span>.
  Literal parameters can be used in tags definitions:
    [f0{$1},span style="color: $p;"]
  yields <span style="color: $p;">$1</span>, where $p is a literal parameter.
  Sizes can be specified in hybridsizes, e.g. adding parameter sizes. By
  default the resulting size is the max of all arguments. Sizes are used
  to generate the right parameters.
  A function followed by a single / is output as a self-closing XHTML tag:
    [f0/,hr]
  will generate <hr/>.
  """

  commandmap = FormulaConfig.hybridfunctions

  def parsebit(self, pos):
    "Parse a function with [] and {} parameters"
    readtemplate = self.translated[0]
    writetemplate = self.translated[1]
    self.readparams(readtemplate, pos)
    self.contents = self.writeparams(writetemplate)
    self.computehybridsize()

  def writeparams(self, writetemplate):
    "Write all params according to the template"
    return self.writepos(TextPosition(writetemplate))

  def writepos(self, pos):
    "Write all params as read in the parse position."
    result = []
    while not pos.finished():
      if pos.checkskip('$'):
        param = self.writeparam(pos)
        if param:
          result.append(param)
      elif pos.checkskip('f'):
        function = self.writefunction(pos)
        if function:
          function.type = None
          result.append(function)
      elif pos.checkskip('('):
        result.append(self.writebracket('left', '('))
      elif pos.checkskip(')'):
        result.append(self.writebracket('right', ')'))
      else:
        result.append(FormulaConstant(pos.skipcurrent()))
    return result

  def writeparam(self, pos):
    "Write a single param of the form $0, $x..."
    name = '$' + pos.skipcurrent()
    if not name in self.params:
      Trace.error('Unknown parameter ' + name)
      return None
    if not self.params[name]:
      return None
    if pos.checkskip('.'):
      self.params[name].value.type = pos.globalpha()
    return self.params[name].value

  def writefunction(self, pos):
    "Write a single function f0,...,fn."
    tag = self.readtag(pos)
    if not tag:
      return None
    if pos.checkskip('/'):
      # self-closing XHTML tag, such as <hr/>
      return TaggedBit().selfcomplete(tag)
    if not pos.checkskip('{'):
      Trace.error('Function should be defined in {}')
      return None
    pos.pushending('}')
    contents = self.writepos(pos)
    pos.popending()
    if len(contents) == 0:
      return None
    return TaggedBit().complete(contents, tag)

  def readtag(self, pos):
    "Get the tag corresponding to the given index. Does parameter substitution."
    if not pos.current().isdigit():
      Trace.error('Function should be f0,...,f9: f' + pos.current())
      return None
    index = int(pos.skipcurrent())
    if 2 + index > len(self.translated):
      Trace.error('Function f' + unicode(index) + ' is not defined')
      return None
    tag = self.translated[2 + index]
    if not '$' in tag:
      return tag
    for variable in self.params:
      if variable in tag:
        param = self.params[variable]
        if not param.literal:
          Trace.error('Parameters in tag ' + tag + ' should be literal: {' + variable + '!}')
          continue
        if param.literalvalue:
          value = param.literalvalue
        else:
          value = ''
        tag = tag.replace(variable, value)
    return tag

  def writebracket(self, direction, character):
    "Return a new bracket looking at the given direction."
    return self.factory.create(BracketCommand).create(direction, character)

  def computehybridsize(self):
    "Compute the size of the hybrid function."
    if not self.command in HybridSize.configsizes:
      self.computesize()
      return
    self.size = HybridSize().getsize(self)
    # set the size in all elements at first level
    for element in self.contents:
      element.size = self.size


class HybridSize(object):
  "The size associated with a hybrid function."

  configsizes = FormulaConfig.hybridsizes

  def getsize(self, function):
    "Read the size for a function and parse it."
    sizestring = self.configsizes[function.command]
    for name in function.params:
      if name in sizestring:
        size = function.params[name].value.computesize()
        sizestring = sizestring.replace(name, unicode(size))
    if '$' in sizestring:
      Trace.error('Unconverted variable in hybrid size: ' + sizestring)
      return 1
    return eval(sizestring)


FormulaCommand.types += [HybridFunction]



def math2html(formula):
  "Convert some TeX math to HTML."
  factory = FormulaFactory()
  whole = factory.parseformula(formula)
  FormulaProcessor().process(whole)
  whole.process()
  return ''.join(whole.gethtml())

def main():
  "Main function, called if invoked from the command line"
  args = sys.argv
  Options().parseoptions(args)
  if len(args) != 1:
    Trace.error('Usage: math2html.py escaped_string')
    exit()
  result = math2html(args[0])
  Trace.message(result)

if __name__ == '__main__':
  main()
