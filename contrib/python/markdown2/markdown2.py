#!/usr/bin/env python
# Copyright (c) 2012 Trent Mick.
# Copyright (c) 2007-2008 ActiveState Corp.
# License: MIT (http://www.opensource.org/licenses/mit-license.php)

r"""A fast and complete Python implementation of Markdown.

[from http://daringfireball.net/projects/markdown/]
> Markdown is a text-to-HTML filter; it translates an easy-to-read /
> easy-to-write structured text format into HTML.  Markdown's text
> format is most similar to that of plain text email, and supports
> features such as headers, *emphasis*, code blocks, blockquotes, and
> links.
>
> Markdown's syntax is designed not as a generic markup language, but
> specifically to serve as a front-end to (X)HTML. You can use span-level
> HTML tags anywhere in a Markdown document, and you can use block level
> HTML tags (like <div> and <table> as well).

Module usage:

    >>> import markdown2
    >>> markdown2.markdown("*boo!*")  # or use `html = markdown_path(PATH)`
    u'<p><em>boo!</em></p>\n'

    >>> markdowner = Markdown()
    >>> markdowner.convert("*boo!*")
    u'<p><em>boo!</em></p>\n'
    >>> markdowner.convert("**boom!**")
    u'<p><strong>boom!</strong></p>\n'

This implementation of Markdown implements the full "core" syntax plus a
number of extras (e.g., code syntax coloring, footnotes) as described on
<https://github.com/trentm/python-markdown2/wiki/Extras>.
"""

cmdln_desc = """A fast and complete Python implementation of Markdown, a
text-to-HTML conversion tool for web writers.

Supported extra syntax options (see -x|--extras option below and
see <https://github.com/trentm/python-markdown2/wiki/Extras> for details):

* admonitions: Enable parsing of RST admonitions.
* breaks: Control where hard breaks are inserted in the markdown.
  Options include:
  - on_newline: Replace single new line characters with <br> when True
  - on_backslash: Replace backslashes at the end of a line with <br>
* break-on-newline: Alias for the on_newline option in the breaks extra.
* code-friendly: Disable _ and __ for em and strong.
* cuddled-lists: Allow lists to be cuddled to the preceding paragraph.
* fenced-code-blocks: Allows a code block to not have to be indented
  by fencing it with '```' on a line before and after. Based on
  <http://github.github.com/github-flavored-markdown/> with support for
  syntax highlighting.
* footnotes: Support footnotes as in use on daringfireball.net and
  implemented in other Markdown processors (tho not in Markdown.pl v1.0.1).
* header-ids: Adds "id" attributes to headers. The id value is a slug of
  the header text.
* highlightjs-lang: Allows specifying the language which used for syntax
  highlighting when using fenced-code-blocks and highlightjs.
* html-classes: Takes a dict mapping html tag names (lowercase) to a
  string to use for a "class" tag attribute. Currently only supports "img",
  "table", "thead", "pre", "code", "ul" and "ol" tags. Add an issue if you require
  this for other tags.
* link-patterns: Auto-link given regex patterns in text (e.g. bug number
  references, revision number references).
* link-shortrefs: allow shortcut reference links, not followed by `[]` or
  a link label.
* markdown-file-links: Replace links to `.md` files with `.html` links
* markdown-in-html: Allow the use of `markdown="1"` in a block HTML tag to
  have markdown processing be done on its contents. Similar to
  <http://michelf.com/projects/php-markdown/extra/#markdown-attr> but with
  some limitations.
* metadata: Extract metadata from a leading '---'-fenced block.
  See <https://github.com/trentm/python-markdown2/issues/77> for details.
* middle-word-em: Allows or disallows emphasis syntax in the middle of words,
  defaulting to allow. Disabling this means that `this_text_here` will not be
  converted to `this<em>text</em>here`.
* nofollow: Add `rel="nofollow"` to add `<a>` tags with an href. See
  <http://en.wikipedia.org/wiki/Nofollow>.
* numbering: Support of generic counters.  Non standard extension to
  allow sequential numbering of figures, tables, equations, exhibits etc.
* pyshell: Treats unindented Python interactive shell sessions as <code>
  blocks.
* smarty-pants: Replaces ' and " with curly quotation marks or curly
  apostrophes.  Replaces --, ---, ..., and . . . with en dashes, em dashes,
  and ellipses.
* spoiler: A special kind of blockquote commonly hidden behind a
  click on SO. Syntax per <http://meta.stackexchange.com/a/72878>.
* strike: text inside of double tilde is ~~strikethrough~~
* tag-friendly: Requires atx style headers to have a space between the # and
  the header text. Useful for applications that require twitter style tags to
  pass through the parser.
* tables: Tables using the same format as GFM
  <https://help.github.com/articles/github-flavored-markdown#tables> and
  PHP-Markdown Extra <https://michelf.ca/projects/php-markdown/extra/#table>.
* toc: The returned HTML string gets a new "toc_html" attribute which is
  a Table of Contents for the document. (experimental)
* use-file-vars: Look for an Emacs-style markdown-extras file variable to turn
  on Extras.
* wiki-tables: Google Code Wiki-style tables. See
  <http://code.google.com/p/support/wiki/WikiSyntax#Tables>.
* wavedrom: Support for generating Wavedrom digital timing diagrams
* xml: Passes one-liner processing instructions and namespaced XML tags.
"""

# Dev Notes:
# - Python's regex syntax doesn't have '\z', so I'm using '\Z'. I'm
#   not yet sure if there implications with this. Compare 'pydoc sre'
#   and 'perldoc perlre'.

__version_info__ = (2, 5, 4)
__version__ = '.'.join(map(str, __version_info__))
__author__ = "Trent Mick"

import argparse
import codecs
import logging
import re
import sys
from collections import defaultdict, OrderedDict
from abc import ABC, abstractmethod
import functools
from collections.abc import Iterable
from hashlib import sha256
from random import random
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Type, TypedDict, Union
from collections.abc import Collection
from enum import IntEnum, auto
from os import urandom

# ---- type defs
_safe_mode = Literal['replace', 'escape']
_extras_dict = dict[str, Any]
_extras_param = Union[list[str], _extras_dict]
_link_patterns = Iterable[tuple[re.Pattern, Union[str, Callable[[re.Match], str]]]]

# ---- globals

DEBUG = False
log = logging.getLogger("markdown")

DEFAULT_TAB_WIDTH = 4


SECRET_SALT = urandom(16)
# MD5 function was previously used for this; the "md5" prefix was kept for
# backwards compatibility.
def _hash_text(s: str) -> str:
    return 'md5-' + sha256(SECRET_SALT + s.encode("utf-8")).hexdigest()[32:]

# Table of hash values for escaped characters:
g_escape_table = {ch: _hash_text(ch)
    for ch in '\\`*_{}[]()>#+-.!'}

# Ampersand-encoding based entirely on Nat Irons's Amputator MT plugin:
#   http://bumppo.net/projects/amputator/
_AMPERSAND_RE = re.compile(r'&(?!#?[xX]?(?:[0-9a-fA-F]+|\w+);)')


# ---- exceptions
class MarkdownError(Exception):
    pass


# ---- public api

def markdown_path(
    path: str,
    encoding: str = "utf-8",
    html4tags: bool = False,
    tab_width: int = DEFAULT_TAB_WIDTH,
    safe_mode: Optional[_safe_mode] = None,
    extras: Optional[_extras_param] = None,
    link_patterns: Optional[_link_patterns] = None,
    footnote_title: Optional[str] = None,
    footnote_return_symbol: Optional[str] = None,
    use_file_vars: bool = False
) -> 'UnicodeWithAttrs':
    fp = codecs.open(path, 'r', encoding)
    text = fp.read()
    fp.close()
    return Markdown(html4tags=html4tags, tab_width=tab_width,
                    safe_mode=safe_mode, extras=extras,
                    link_patterns=link_patterns,
                    footnote_title=footnote_title,
                    footnote_return_symbol=footnote_return_symbol,
                    use_file_vars=use_file_vars).convert(text)


def markdown(
    text: str,
    html4tags: bool = False,
    tab_width: int = DEFAULT_TAB_WIDTH,
    safe_mode: Optional[_safe_mode] = None,
    extras: Optional[_extras_param] = None,
    link_patterns: Optional[_link_patterns] = None,
    footnote_title: Optional[str] = None,
    footnote_return_symbol: Optional[str] =None,
    use_file_vars: bool = False,
    cli: bool = False
) -> 'UnicodeWithAttrs':
    return Markdown(html4tags=html4tags, tab_width=tab_width,
                    safe_mode=safe_mode, extras=extras,
                    link_patterns=link_patterns,
                    footnote_title=footnote_title,
                    footnote_return_symbol=footnote_return_symbol,
                    use_file_vars=use_file_vars, cli=cli).convert(text)


class Stage(IntEnum):
    PREPROCESS = auto()
    HASH_HTML = auto()
    LINK_DEFS = auto()

    BLOCK_GAMUT = auto()
    HEADERS = auto()
    LISTS = auto()
    CODE_BLOCKS = auto()
    BLOCK_QUOTES = auto()
    PARAGRAPHS = auto()

    SPAN_GAMUT = auto()
    CODE_SPANS = auto()
    ESCAPE_SPECIAL = auto()
    LINKS = auto()  # and auto links
    ITALIC_AND_BOLD = auto()

    POSTPROCESS = auto()
    UNHASH_HTML = auto()


def mark_stage(stage: Stage):
    '''
    Decorator that handles executing relevant `Extra`s before and after this `Stage` executes.
    '''
    def wrapper(func):
        @functools.wraps(func)
        def inner(md: 'Markdown', text, *args, **kwargs):
            md.stage = stage
            # set "order" prop so extras can tell if they're being invoked before/after the stage
            md.order = stage - 0.5

            if stage in Extra._exec_order:
                for klass in Extra._exec_order[stage][0]:
                    if klass.name not in md.extra_classes:
                        continue
                    extra = md.extra_classes[klass.name]
                    if extra.test(text):
                        text = extra.run(text)

            md.order = stage
            text = func(md, text, *args, **kwargs)
            md.order = stage + 0.5

            if stage in Extra._exec_order:
                for klass in Extra._exec_order[stage][1]:
                    if klass.name not in md.extra_classes:
                        continue
                    extra = md.extra_classes[klass.name]
                    if extra.test(text):
                        text = extra.run(text)

            return text

        return inner

    return wrapper


class Markdown:
    # The dict of "extras" to enable in processing -- a mapping of
    # extra name to argument for the extra. Most extras do not have an
    # argument, in which case the value is None.
    #
    # This can be set via (a) subclassing and (b) the constructor
    # "extras" argument.
    extras: _extras_dict
    # dict of `Extra` names and associated class instances, populated during _setup_extras
    extra_classes: dict[str, 'Extra']

    urls: dict[str, str]
    titles: dict[str, str]
    html_blocks: dict[str, str]
    html_spans: dict[str, str]
    html_removed_text: str = "{(#HTML#)}"  # placeholder removed text that does not trigger bold
    html_removed_text_compat: str = "[HTML_REMOVED]"  # for compat with markdown.py
    safe_mode: Optional[_safe_mode]

    _toc: list[tuple[int, str, str]]

    # Used to track when we're inside an ordered or unordered list
    # (see _ProcessListItems() for details):
    list_level = 0

    stage: Stage
    '''Current "stage" of markdown conversion taking place'''
    order: float
    '''
    Same as `Stage` but will be +/- 0.5 of the value of `Stage`.
    This allows extras to check if they are running before or after a particular stage
    with `if md.order < md.stage`.
    '''

    _ws_only_line_re = re.compile(r"^[ \t]+$", re.M)

    def __init__(
        self,
        html4tags: bool = False,
        tab_width: int = DEFAULT_TAB_WIDTH,
        safe_mode: Optional[_safe_mode] = None,
        extras: Optional[_extras_param] = None,
        link_patterns: Optional[_link_patterns] = None,
        footnote_title: Optional[str] = None,
        footnote_return_symbol: Optional[str] = None,
        use_file_vars: bool = False,
        cli: bool = False
    ):
        if html4tags:
            self.empty_element_suffix = ">"
        else:
            self.empty_element_suffix = " />"
        self.tab_width = tab_width
        self.tab = tab_width * " "

        # For compatibility with earlier markdown2.py and with
        # markdown.py's safe_mode being a boolean,
        #   safe_mode == True -> "replace"
        if safe_mode is True:
            self.safe_mode = "replace"
        else:
            self.safe_mode = safe_mode

        # Massaging and building the "extras" info.
        if getattr(self, 'extras', None) is None:
            self.extras = {}
        elif not isinstance(self.extras, dict):
            # inheriting classes may set `self.extras` as List[str].
            # we can't allow it through type hints but we can convert it
            self.extras = {e: None for e in self.extras}  # type:ignore

        if extras:
            if not isinstance(extras, dict):
                extras = {e: None for e in extras}
            self.extras.update(extras)
        assert isinstance(self.extras, dict)

        if "toc" in self.extras:
            if "header-ids" not in self.extras:
                self.extras["header-ids"] = None   # "toc" implies "header-ids"

            if self.extras["toc"] is None:
                self._toc_depth = 6
            else:
                self._toc_depth = self.extras["toc"].get("depth", 6)

        if 'header-ids' in self.extras:
            if not isinstance(self.extras['header-ids'], dict):
                self.extras['header-ids'] = {
                    'mixed': False,
                    'prefix': self.extras['header-ids'],
                    'reset-count': True
                }

        if 'break-on-newline' in self.extras:
            self.extras.setdefault('breaks', {})
            self.extras['breaks']['on_newline'] = True

        if 'link-patterns' in self.extras:
            # allow link patterns via extras dict without kwarg explicitly set
            link_patterns = link_patterns or self.extras['link-patterns']
            if link_patterns is None:
                # if you have specified that the link-patterns extra SHOULD
                # be used (via self.extras) but you haven't provided anything
                # via the link_patterns argument then an error is raised
                raise MarkdownError("If the 'link-patterns' extra is used, an argument for 'link_patterns' is required")
            self.extras['link-patterns'] = link_patterns

        self._instance_extras = self.extras.copy()
        self.link_patterns = link_patterns
        self.footnote_title = footnote_title
        self.footnote_return_symbol = footnote_return_symbol
        self.use_file_vars = use_file_vars
        self._outdent_re = re.compile(r'^(\t|[ ]{1,%d})' % tab_width, re.M)
        self.cli = cli

        self._escape_table = g_escape_table.copy()
        self._code_table = {}
        if "smarty-pants" in self.extras:
            self._escape_table['"'] = _hash_text('"')
            self._escape_table["'"] = _hash_text("'")

    def reset(self):
        self.urls = {}
        self.titles = {}
        self.html_blocks = {}
        self.html_spans = {}
        self.list_level = 0
        self.extras = self._instance_extras.copy()
        self._setup_extras()
        self._toc = []

    def _setup_extras(self):
        if "footnotes" in self.extras:
            # order of insertion matters for footnotes. Use ordered dict for Python < 3.7
            # https://docs.python.org/3/whatsnew/3.7.html#summary-release-highlights
            self.footnotes = OrderedDict()
            self.footnote_ids = []
            self._footnote_marker = _hash_text('<<footnote>>')
        if "header-ids" in self.extras:
            if not hasattr(self, '_count_from_header_id') or self.extras['header-ids'].get('reset-count', False):
                self._count_from_header_id = defaultdict(int)
        if "metadata" in self.extras:
            self.metadata: dict[str, Any] = {}

        self.extra_classes = {}
        for name, klass in Extra._registry.items():
            if name not in self.extras:
                continue
            self.extra_classes[name] = klass(self, (self.extras.get(name, {})))

    # Per <https://developer.mozilla.org/en-US/docs/HTML/Element/a> "rel"
    # should only be used in <a> tags with an "href" attribute.

    # Opens the linked document in a new window or tab
    # should only used in <a> tags with an "href" attribute.
    # same with _a_nofollow
    _a_nofollow_or_blank_links = re.compile(r"""
        <(a)
        (
            [^>]*
            href=   # href is required
            ['"]?   # HTML5 attribute values do not have to be quoted
            [^#'"]  # We don't want to match href values that start with # (like footnotes)
        )
        """,
        re.IGNORECASE | re.VERBOSE
    )

    def convert(self, text: str) -> 'UnicodeWithAttrs':
        """Convert the given text."""
        # Main function. The order in which other subs are called here is
        # essential. Link and image substitutions need to happen before
        # _EscapeSpecialChars(), so that any *'s or _'s in the <a>
        # and <img> tags get encoded.

        # Clear the global hashes. If we don't clear these, you get conflicts
        # from other articles when generating a page which contains more than
        # one article (e.g. an index page that shows the N most recent
        # articles):
        self.reset()

        if not isinstance(text, str):
            # TODO: perhaps shouldn't presume UTF-8 for string input?
            text = str(text, 'utf-8')

        if self.use_file_vars:
            # Look for emacs-style file variable hints.
            text = self._emacs_oneliner_vars_pat.sub(self._emacs_vars_oneliner_sub, text)
            emacs_vars = self._get_emacs_vars(text)
            if "markdown-extras" in emacs_vars:
                splitter = re.compile("[ ,]+")
                for e in splitter.split(emacs_vars["markdown-extras"]):
                    if '=' in e:
                        ename, earg = e.split('=', 1)
                        try:
                            earg = int(earg)
                        except ValueError:
                            pass
                    else:
                        ename, earg = e, None
                    self.extras[ename] = earg

            self._setup_extras()

        # Standardize line endings:
        text = text.replace("\r\n", "\n")
        text = text.replace("\r", "\n")

        # Make sure $text ends with a couple of newlines:
        text += "\n\n"

        # Convert all tabs to spaces.
        text = self._detab(text)

        # Strip any lines consisting only of spaces and tabs.
        # This makes subsequent regexen easier to write, because we can
        # match consecutive blank lines with /\n+/ instead of something
        # contorted like /[ \t]*\n+/ .
        text = self._ws_only_line_re.sub("", text)

        # strip metadata from head and extract
        if "metadata" in self.extras:
            text = self._extract_metadata(text)

        text = self.preprocess(text)

        if self.safe_mode:
            text = self._hash_html_spans(text)

        # Turn block-level HTML blocks into hash entries
        text = self._hash_html_blocks(text, raw=True)

        # Strip link definitions, store in hashes.
        if "footnotes" in self.extras:
            # Must do footnotes first because an unlucky footnote defn
            # looks like a link defn:
            #   [^4]: this "looks like a link defn"
            text = self._strip_footnote_definitions(text)
        text = self._strip_link_definitions(text)

        text = self._run_block_gamut(text)

        if "footnotes" in self.extras:
            def footnote_sub(match):
                normed_id = match.group(1)
                self.footnote_ids.append(normed_id)
                return str(len(self.footnote_ids))

            text = re.sub(r'%s-(.*?)(?=</a></sup>)' % self._footnote_marker, footnote_sub, text)
            text = self._add_footnotes(text)

        text = self.postprocess(text)

        text = self._unescape_special_chars(text)

        text = self._unhash_html_spans(text)
        if self.safe_mode:
            # return the removed text warning to its markdown.py compatible form
            text = text.replace(self.html_removed_text, self.html_removed_text_compat)

        do_target_blank_links = "target-blank-links" in self.extras
        do_nofollow_links = "nofollow" in self.extras

        if do_target_blank_links and do_nofollow_links:
            text = self._a_nofollow_or_blank_links.sub(r'<\1 rel="nofollow noopener" target="_blank"\2', text)
        elif do_target_blank_links:
            text = self._a_nofollow_or_blank_links.sub(r'<\1 rel="noopener" target="_blank"\2', text)
        elif do_nofollow_links:
            text = self._a_nofollow_or_blank_links.sub(r'<\1 rel="nofollow"\2', text)

        if "toc" in self.extras and self._toc:
            if self.extras['header-ids'].get('mixed'):
                # TOC will only be out of order if mixed headers is enabled
                def toc_sort(entry):
                    '''Sort the TOC by order of appearance in text'''
                    match = re.search(
                        # header tag, any attrs, the ID, any attrs, the text, close tag
                        r'^<(h%d).*?id=(["\'])%s\2.*>%s</\1>$' % (entry[0], entry[1], re.escape(entry[2])),
                        text, re.M
                    )
                    return match.start() if match else 0

                self._toc.sort(key=toc_sort)
            self._toc_html = calculate_toc_html(self._toc)

            # Prepend toc html to output
            if self.cli or (self.extras['toc'] is not None and self.extras['toc'].get('prepend', False)):
                text = f'{self._toc_html}\n{text}'

        text += "\n"

        # Attach attrs to output
        rv = UnicodeWithAttrs(text)

        if "toc" in self.extras and self._toc:
            rv.toc_html = self._toc_html

        if "metadata" in self.extras:
            rv.metadata = self.metadata
        return rv

    @mark_stage(Stage.POSTPROCESS)
    def postprocess(self, text: str) -> str:
        """A hook for subclasses to do some postprocessing of the html, if
        desired. This is called before unescaping of special chars and
        unhashing of raw HTML spans.
        """
        return text

    @mark_stage(Stage.PREPROCESS)
    def preprocess(self, text: str) -> str:
        """A hook for subclasses to do some preprocessing of the Markdown, if
        desired. This is called after basic formatting of the text, but prior
        to any extras, safe mode, etc. processing.
        """
        return text

    # Is metadata if the content starts with optional '---'-fenced `key: value`
    # pairs. E.g. (indented for presentation):
    #   ---
    #   foo: bar
    #   another-var: blah blah
    #   ---
    #   # header
    # or:
    #   foo: bar
    #   another-var: blah blah
    #
    #   # header
    _meta_data_pattern = re.compile(r'''
        ^{0}(  # optional opening fence
            (?:
                {1}:(?:\n+[ \t]+.*)+  # indented lists
            )|(?:
                (?:{1}:\s+>(?:\n\s+.*)+?)  # multiline long descriptions
                (?=\n{1}:\s*.*\n|\s*\Z)  # match up until the start of the next key:value definition or the end of the input text
            )|(?:
                {1}:(?! >).*\n?  # simple key:value pair, leading spaces allowed
            )
        ){0}  # optional closing fence
        '''.format(r'(?:---[\ \t]*\n)?', r'[\S \t]*\w[\S \t]*\s*'), re.MULTILINE | re.VERBOSE
    )

    _key_val_list_pat = re.compile(
        r"^-(?:[ \t]*([^\n]*)(?:[ \t]*[:-][ \t]*(\S+))?)(?:\n((?:[ \t]+[^\n]+\n?)+))?",
        re.MULTILINE,
    )
    _key_val_dict_pat = re.compile(
        r"^([^:\n]+)[ \t]*:[ \t]*([^\n]*)(?:((?:\n[ \t]+[^\n]+)+))?", re.MULTILINE
    )  # grp0: key, grp1: value, grp2: multiline value
    _meta_data_fence_pattern = re.compile(r'^---[\ \t]*\n', re.MULTILINE)
    _meta_data_newline = re.compile("^\n", re.MULTILINE)

    def _extract_metadata(self, text: str) -> str:
        if text.startswith("---"):
            fence_splits = re.split(self._meta_data_fence_pattern, text, maxsplit=2)
            metadata_content = fence_splits[1]
            tail = fence_splits[2]
        else:
            metadata_split = re.split(self._meta_data_newline, text, maxsplit=1)
            metadata_content = metadata_split[0]
            tail = metadata_split[1]

        # _meta_data_pattern only has one capturing group, so we can assume
        # the returned type to be list[str]
        match: list[str] = re.findall(self._meta_data_pattern, metadata_content)
        if not match:
            return text

        def parse_structured_value(value: str) -> Union[list[Any], dict[str, Any]]:
            vs = value.lstrip()
            vs = value.replace(v[: len(value) - len(vs)], "\n")[1:]

            # List
            if vs.startswith("-"):
                r: list[Any] = []
                # the regex used has multiple capturing groups, so
                # returned type from findall will be List[List[str]]
                match: list[str]
                for match in re.findall(self._key_val_list_pat, vs):
                    if match[0] and not match[1] and not match[2]:
                        r.append(match[0].strip())
                    elif match[0] == ">" and not match[1] and match[2]:
                        r.append(match[2].strip())
                    elif match[0] and match[1]:
                        r.append({match[0].strip(): match[1].strip()})
                    elif not match[0] and not match[1] and match[2]:
                        r.append(parse_structured_value(match[2]))
                    else:
                        # Broken case
                        pass

                return r

            # Dict
            else:
                return {
                    match[0].strip(): (
                        match[1].strip()
                        if match[1]
                        else parse_structured_value(match[2])
                    )
                    for match in re.findall(self._key_val_dict_pat, vs)
                }

        for item in match:

            k, v = item.split(":", 1)

            # Multiline value
            if v[:3] == " >\n":
                self.metadata[k.strip()] = _dedent(v[3:]).strip()

            # Empty value
            elif v == "\n":
                self.metadata[k.strip()] = ""

            # Structured value
            elif v[0] == "\n":
                self.metadata[k.strip()] = parse_structured_value(v)

            # Simple value
            else:
                self.metadata[k.strip()] = v.strip()

        return tail

    _emacs_oneliner_vars_pat = re.compile(r"((?:<!--)?\s*-\*-)\s*(?:(\S[^\r\n]*?)([\r\n]\s*)?)?(-\*-\s*(?:-->)?)", re.UNICODE)
    # This regular expression is intended to match blocks like this:
    #    PREFIX Local Variables: SUFFIX
    #    PREFIX mode: Tcl SUFFIX
    #    PREFIX End: SUFFIX
    # Some notes:
    # - "[ \t]" is used instead of "\s" to specifically exclude newlines
    # - "(\r\n|\n|\r)" is used instead of "$" because the sre engine does
    #   not like anything other than Unix-style line terminators.
    _emacs_local_vars_pat = re.compile(r"""^
        (?P<prefix>(?:[^\r\n|\n|\r])*?)
        [\ \t]*Local\ Variables:[\ \t]*
        (?P<suffix>.*?)(?:\r\n|\n|\r)
        (?P<content>.*?\1End:)
        """, re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE)

    def _emacs_vars_oneliner_sub(self, match: re.Match) -> str:
        if match.group(1).strip() == '-*-' and match.group(4).strip() == '-*-':
            lead_ws = re.findall(r'^\s*', match.group(1))[0]
            tail_ws = re.findall(r'\s*$', match.group(4))[0]
            return '{}<!-- {} {} {} -->{}'.format(lead_ws, '-*-', match.group(2).strip(), '-*-', tail_ws)

        start, end = match.span()
        return match.string[start: end]

    def _get_emacs_vars(self, text: str) -> dict[str, str]:
        """Return a dictionary of emacs-style local variables.

        Parsing is done loosely according to this spec (and according to
        some in-practice deviations from this):
        http://www.gnu.org/software/emacs/manual/html_node/emacs/Specifying-File-Variables.html#Specifying-File-Variables
        """
        emacs_vars = {}
        SIZE = pow(2, 13)  # 8kB

        # Search near the start for a '-*-'-style one-liner of variables.
        head = text[:SIZE]
        if "-*-" in head:
            match = self._emacs_oneliner_vars_pat.search(head)
            if match:
                emacs_vars_str = match.group(2)
                assert '\n' not in emacs_vars_str
                emacs_var_strs = [s.strip() for s in emacs_vars_str.split(';')
                                  if s.strip()]
                if len(emacs_var_strs) == 1 and ':' not in emacs_var_strs[0]:
                    # While not in the spec, this form is allowed by emacs:
                    #   -*- Tcl -*-
                    # where the implied "variable" is "mode". This form
                    # is only allowed if there are no other variables.
                    emacs_vars["mode"] = emacs_var_strs[0].strip()
                else:
                    for emacs_var_str in emacs_var_strs:
                        try:
                            variable, value = emacs_var_str.strip().split(':', 1)
                        except ValueError:
                            log.debug("emacs variables error: malformed -*- "
                                      "line: %r", emacs_var_str)
                            continue
                        # Lowercase the variable name because Emacs allows "Mode"
                        # or "mode" or "MoDe", etc.
                        emacs_vars[variable.lower()] = value.strip()

        tail = text[-SIZE:]
        if "Local Variables" in tail:
            match = self._emacs_local_vars_pat.search(tail)
            if match:
                prefix = match.group("prefix")
                suffix = match.group("suffix")
                lines = match.group("content").splitlines(False)
                # print "prefix=%r, suffix=%r, content=%r, lines: %s"\
                #      % (prefix, suffix, match.group("content"), lines)

                # Validate the Local Variables block: proper prefix and suffix
                # usage.
                for i, line in enumerate(lines):
                    if not line.startswith(prefix):
                        log.debug("emacs variables error: line '%s' "
                                  "does not use proper prefix '%s'"
                                  % (line, prefix))
                        return {}
                    # Don't validate suffix on last line. Emacs doesn't care,
                    # neither should we.
                    if i != len(lines)-1 and not line.endswith(suffix):
                        log.debug("emacs variables error: line '%s' "
                                  "does not use proper suffix '%s'"
                                  % (line, suffix))
                        return {}

                # Parse out one emacs var per line.
                continued_for = None
                for line in lines[:-1]:  # no var on the last line ("PREFIX End:")
                    if prefix:
                        line = line[len(prefix):]  # strip prefix
                    if suffix:
                        line = line[:-len(suffix)]  # strip suffix
                    line = line.strip()
                    if continued_for:
                        variable = continued_for
                        if line.endswith('\\'):
                            line = line[:-1].rstrip()
                        else:
                            continued_for = None
                        emacs_vars[variable] += ' ' + line
                    else:
                        try:
                            variable, value = line.split(':', 1)
                        except ValueError:
                            log.debug("local variables error: missing colon "
                                      "in local variables entry: '%s'" % line)
                            continue
                        # Do NOT lowercase the variable name, because Emacs only
                        # allows "mode" (and not "Mode", "MoDe", etc.) in this block.
                        value = value.strip()
                        if value.endswith('\\'):
                            value = value[:-1].rstrip()
                            continued_for = variable
                        else:
                            continued_for = None
                        emacs_vars[variable] = value

        # Unquote values.
        for var, val in list(emacs_vars.items()):
            if len(val) > 1 and (val.startswith('"') and val.endswith('"')
               or val.startswith('"') and val.endswith('"')):
                emacs_vars[var] = val[1:-1]

        return emacs_vars

    def _detab_line(self, line: str) -> str:
        r"""Recusively convert tabs to spaces in a single line.

        Called from _detab()."""
        if '\t' not in line:
            return line
        chunk1, chunk2 = line.split('\t', 1)
        chunk1 += (' ' * (self.tab_width - len(chunk1) % self.tab_width))
        output = chunk1 + chunk2
        return self._detab_line(output)

    def _detab(self, text: str) -> str:
        r"""Iterate text line by line and convert tabs to spaces.

            >>> m = Markdown()
            >>> m._detab("\tfoo")
            '    foo'
            >>> m._detab("  \tfoo")
            '    foo'
            >>> m._detab("\t  foo")
            '      foo'
            >>> m._detab("  foo")
            '  foo'
            >>> m._detab("  foo\n\tbar\tblam")
            '  foo\n    bar blam'
        """
        if '\t' not in text:
            return text
        output = []
        for line in text.splitlines():
            output.append(self._detab_line(line))
        return '\n'.join(output)

    # I broke out the html5 tags here and add them to _block_tags_a and
    # _block_tags_b.  This way html5 tags are easy to keep track of.
    _html5tags = '|address|article|aside|canvas|figcaption|figure|footer|header|main|nav|section|video'

    _block_tags_a = 'blockquote|body|dd|del|div|dl|dt|fieldset|form|h[1-6]|head|hr|html|iframe|ins|li|math|noscript|ol|p|pre|script|style|table|tfoot|ul'
    _block_tags_a += _html5tags

    _strict_tag_block_re = re.compile(r"""
        (                       # save in \1
            ^                   # start of line  (with re.M)
            <(%s)               # start tag = \2
            \b                  # word break
            (.*\n)*?            # any number of lines, minimally matching
            </\2>               # the matching end tag
            [ \t]*              # trailing spaces/tabs
            (?=\n+|\Z)          # followed by a newline or end of document
        )
        """ % _block_tags_a,
        re.X | re.M)

    _block_tags_b = 'blockquote|div|dl|fieldset|form|h[1-6]|iframe|math|noscript|ol|p|pre|script|table|ul'
    _block_tags_b += _html5tags

    _span_tags = (
        'a|abbr|acronym|b|bdo|big|br|button|cite|code|dfn|em|i|img|input|kbd|label|map|object|output|q'
        '|samp|script|select|small|span|strong|sub|sup|textarea|time|tt|var'
    )

    _liberal_tag_block_re = re.compile(r"""
        (                       # save in \1
            ^                   # start of line  (with re.M)
            <(%s)               # start tag = \2
            \b                  # word break
            (.*\n)*?            # any number of lines, minimally matching
            .*</\2>             # the matching end tag
            [ \t]*              # trailing spaces/tabs
            (?=\n+|\Z)          # followed by a newline or end of document
        )
        """ % _block_tags_b,
        re.X | re.M)

    _html_markdown_attr_re = re.compile(
        r'''\s+markdown=("1"|'1')''')
    def _hash_html_block_sub(
        self,
        match: Union[re.Match, str],
        raw: bool = False
    ) -> str:
        if isinstance(match, str):
            html = match
            tag = None
        else:
            html = match.group(1)
            try:
                tag = match.group(2)
            except IndexError:
                tag = None

        if not tag:
            m = re.match(r'.*?<(\S).*?\s*>', html)
            # tag shouldn't be none but make the assertion for type checker
            assert m is not None
            tag = m.group(1)

        if raw and self.safe_mode:
            html = self._sanitize_html(html)
        elif 'markdown-in-html' in self.extras and 'markdown=' in html:
            first_line = html.split('\n', 1)[0]
            m = self._html_markdown_attr_re.search(first_line)
            if m:
                lines = html.split('\n')
                # if MD is on same line as opening tag then split across two lines
                lines = list(filter(None, (re.split(r'(.*?<%s.*markdown=.*?>)' % tag, lines[0])))) + lines[1:]
                # if MD on same line as closing tag, split across two lines
                lines = lines[:-1] + list(filter(None, re.split(r'(\s*?</%s>.*?$)' % tag, lines[-1])))
                # extract key sections of the match
                first_line = lines[0]
                middle = '\n'.join(lines[1:-1])
                last_line = lines[-1]
                # remove `markdown="1"` attr from tag
                first_line = first_line[:m.start()] + first_line[m.end():]
                # hash the HTML segments to protect them
                f_key = _hash_text(first_line)
                self.html_blocks[f_key] = first_line
                l_key = _hash_text(last_line)
                self.html_blocks[l_key] = last_line
                return ''.join(["\n\n", f_key,
                    "\n\n", middle, "\n\n",
                    l_key, "\n\n"])
        elif self.extras.get('header-ids', {}).get('mixed') and self._h_tag_re.match(html):
            html = self._h_tag_re.sub(self._h_tag_sub, html)
        key = _hash_text(html)
        self.html_blocks[key] = html
        return "\n\n" + key + "\n\n"

    @mark_stage(Stage.HASH_HTML)
    def _hash_html_blocks(self, text: str, raw: bool = False) -> str:
        """Hashify HTML blocks

        We only want to do this for block-level HTML tags, such as headers,
        lists, and tables. That's because we still want to wrap <p>s around
        "paragraphs" that are wrapped in non-block-level tags, such as anchors,
        phrase emphasis, and spans. The list of tags we're looking for is
        hard-coded.

        @param raw {boolean} indicates if these are raw HTML blocks in
            the original source. It makes a difference in "safe" mode.
        """
        if '<' not in text:
            return text

        # Pass `raw` value into our calls to self._hash_html_block_sub.
        hash_html_block_sub = _curry(self._hash_html_block_sub, raw=raw)

        # First, look for nested blocks, e.g.:
        #   <div>
        #       <div>
        #       tags for inner block must be indented.
        #       </div>
        #   </div>
        #
        # The outermost tags must start at the left margin for this to match, and
        # the inner nested divs must be indented.
        # We need to do this before the next, more liberal match, because the next
        # match will start at the first `<div>` and stop at the first `</div>`.
        text = self._strict_tag_block_sub(text, self._block_tags_a, hash_html_block_sub)

        # Now match more liberally, simply from `\n<tag>` to `</tag>\n`
        text = self._liberal_tag_block_re.sub(hash_html_block_sub, text)

        # now do the same for spans that are acting like blocks
        # eg: an anchor split over multiple lines for readability
        text = self._strict_tag_block_sub(
            text, self._span_tags,
            # inline elements can't contain block level elements, so only span gamut is required
            lambda t: hash_html_block_sub(self._run_span_gamut(t))
        )

        # Special case just for <hr />. It was easier to make a special
        # case than to make the other regex more complicated.
        if "<hr" in text:
            _hr_tag_re = _hr_tag_re_from_tab_width(self.tab_width)
            text = _hr_tag_re.sub(hash_html_block_sub, text)

        # Special case for standalone HTML comments:
        if "<!--" in text:
            start = 0
            while True:
                # Delimiters for next comment block.
                try:
                    start_idx = text.index("<!--", start)
                except ValueError:
                    break
                try:
                    end_idx = text.index("-->", start_idx) + 3
                except ValueError:
                    break

                # Start position for next comment block search.
                start = end_idx

                # Validate whitespace before comment.
                if start_idx:
                    # - Up to `tab_width - 1` spaces before start_idx.
                    for i in range(self.tab_width - 1):
                        if text[start_idx - 1] != ' ':
                            break
                        start_idx -= 1
                        if start_idx == 0:
                            break
                    # - Must be preceded by 2 newlines or hit the start of
                    #   the document.
                    if start_idx == 0:
                        pass
                    elif start_idx == 1 and text[0] == '\n':
                        start_idx = 0  # to match minute detail of Markdown.pl regex
                    elif text[start_idx-2:start_idx] == '\n\n':
                        pass
                    else:
                        break

                # Validate whitespace after comment.
                # - Any number of spaces and tabs.
                while end_idx < len(text):
                    if text[end_idx] not in ' \t':
                        break
                    end_idx += 1
                # - Must be following by 2 newlines or hit end of text.
                if text[end_idx:end_idx+2] not in ('', '\n', '\n\n'):
                    continue

                # Escape and hash (must match `_hash_html_block_sub`).
                html = text[start_idx:end_idx]
                if raw and self.safe_mode:
                    html = self._sanitize_html(html)
                key = _hash_text(html)
                self.html_blocks[key] = html
                text = text[:start_idx] + "\n\n" + key + "\n\n" + text[end_idx:]

        if "xml" in self.extras:
            # Treat XML processing instructions and namespaced one-liner
            # tags as if they were block HTML tags. E.g., if standalone
            # (i.e. are their own paragraph), the following do not get
            # wrapped in a <p> tag:
            #    <?foo bar?>
            #
            #    <xi:include xmlns:xi="http://www.w3.org/2001/XInclude" href="chapter_1.md"/>
            _xml_oneliner_re = _xml_oneliner_re_from_tab_width(self.tab_width)
            text = _xml_oneliner_re.sub(hash_html_block_sub, text)

        return text

    def _strict_tag_block_sub(
        self,
        text: str,
        html_tags_re: str,
        callback: Callable[[str], str],
        allow_indent: bool = False
    ) -> str:
        '''
        Finds and substitutes HTML blocks within blocks of text

        Args:
            text: the text to search
            html_tags_re: a regex pattern of HTML block tags to match against.
                For example, `Markdown._block_tags_a`
            callback: callback function that receives the found HTML text block and returns a new str
            allow_indent: allow matching HTML blocks that are not completely outdented
        '''
        tag_count = 0
        current_tag = html_tags_re
        block = ''
        result = ''

        for chunk in text.splitlines(True):
            is_markup = re.match(
                r'^(\s{{0,{}}})(?:</code>(?=</pre>))?(</?({})\b>?)'.format('' if allow_indent else '0', current_tag), chunk
            )
            block += chunk

            if is_markup:
                if chunk.startswith('%s</' % is_markup.group(1)):
                    tag_count -= 1
                else:
                    # if close tag is in same line
                    if self._tag_is_closed(is_markup.group(3), chunk):
                        # we must ignore these
                        is_markup = None
                    else:
                        tag_count += 1
                        current_tag = is_markup.group(3)

            if tag_count == 0:
                if is_markup:
                    block = callback(block.rstrip('\n'))  # remove trailing newline
                current_tag = html_tags_re
                result += block
                block = ''

        result += block

        return result

    def _tag_is_closed(self, tag_name: str, text: str) -> bool:
        # super basic check if number of open tags == number of closing tags
        return len(re.findall('<%s(?:.*?)>' % tag_name, text)) == len(re.findall('</%s>' % tag_name, text))

    @mark_stage(Stage.LINK_DEFS)
    def _strip_link_definitions(self, text: str) -> str:
        # Strips link definitions from text, stores the URLs and titles in
        # hash references.
        less_than_tab = self.tab_width - 1

        # Link defs are in the form:
        #   [id]: url "optional title"
        _link_def_re = re.compile(r"""
            ^[ ]{0,%d}\[(.+)\]: # id = \1
              [ \t]*
              \n?               # maybe *one* newline
              [ \t]*
            <?(.+?)>?           # url = \2
              [ \t]*
            (?:
                \n?             # maybe one newline
                [ \t]*
                (?<=\s)         # lookbehind for whitespace
                ['"(]
                ([^\n]*)        # title = \3
                ['")]
                [ \t]*
            )?  # title is optional
            (?:\n+|\Z)
            """ % less_than_tab, re.X | re.M | re.U)
        return _link_def_re.sub(self._extract_link_def_sub, text)

    def _extract_link_def_sub(self, match: re.Match) -> str:
        id, url, title = match.groups()
        key = id.lower()    # Link IDs are case-insensitive
        self.urls[key] = self._encode_amps_and_angles(url)
        if title:
            self.titles[key] = title
        return ""

    def _extract_footnote_def_sub(self, match: re.Match) -> str:
        id, text = match.groups()
        text = _dedent(text, skip_first_line=not text.startswith('\n')).strip()
        normed_id = re.sub(r'\W', '-', id)
        # Ensure footnote text ends with a couple newlines (for some
        # block gamut matches).
        self.footnotes[normed_id] = text + "\n\n"
        return ""

    def _strip_footnote_definitions(self, text: str) -> str:
        """A footnote definition looks like this:

            [^note-id]: Text of the note.

                May include one or more indented paragraphs.

        Where,
        - The 'note-id' can be pretty much anything, though typically it
          is the number of the footnote.
        - The first paragraph may start on the next line, like so:

            [^note-id]:
                Text of the note.
        """
        less_than_tab = self.tab_width - 1
        footnote_def_re = re.compile(r'''
            ^[ ]{0,%d}\[\^(.+)\]:   # id = \1
            [ \t]*
            (                       # footnote text = \2
              # First line need not start with the spaces.
              (?:\s*.*\n+)
              (?:
                (?:[ ]{%d} | \t)  # Subsequent lines must be indented.
                .*\n+
              )*
            )
            # Lookahead for non-space at line-start, or end of doc.
            (?:(?=^[ ]{0,%d}\S)|\Z)
            ''' % (less_than_tab, self.tab_width, self.tab_width),
            re.X | re.M)
        return footnote_def_re.sub(self._extract_footnote_def_sub, text)

    _hr_re = re.compile(r'^[ ]{0,3}([-_*])[ ]{0,2}(\1[ ]{0,2}){2,}$', re.M)

    @mark_stage(Stage.BLOCK_GAMUT)
    def _run_block_gamut(self, text: str) -> str:
        # These are all the transformations that form block-level
        # tags like paragraphs, headers, and list items.

        text = self._do_headers(text)

        # Do Horizontal Rules:
        # On the number of spaces in horizontal rules: The spec is fuzzy: "If
        # you wish, you may use spaces between the hyphens or asterisks."
        # Markdown.pl 1.0.1's hr regexes limit the number of spaces between the
        # hr chars to one or two. We'll reproduce that limit here.
        hr = "\n<hr"+self.empty_element_suffix+"\n"
        text = re.sub(self._hr_re, hr, text)

        text = self._do_lists(text)

        text = self._do_code_blocks(text)

        text = self._do_block_quotes(text)

        # We already ran _HashHTMLBlocks() before, in Markdown(), but that
        # was to escape raw HTML in the original Markdown source. This time,
        # we're escaping the markup we've just created, so that we don't wrap
        # <p> tags around block-level tags.
        text = self._hash_html_blocks(text)

        text = self._form_paragraphs(text)

        return text

    @mark_stage(Stage.SPAN_GAMUT)
    def _run_span_gamut(self, text: str) -> str:
        # These are all the transformations that occur *within* block-level
        # tags like paragraphs, headers, and list items.

        text = self._do_code_spans(text)

        text = self._escape_special_chars(text)

        # Process anchor and image tags.
        text = self._do_links(text)

        # Make links out of things like `<http://example.com/>`
        # Must come after _do_links(), because you can use < and >
        # delimiters in inline links like [this](<url>).
        text = self._do_auto_links(text)

        text = self._encode_amps_and_angles(text)

        text = self._do_italics_and_bold(text)

        # Do hard breaks
        text = re.sub(r" {2,}\n(?!\<(?:\/?(ul|ol|li))\>)", "<br%s\n" % self.empty_element_suffix, text)

        return text

    # "Sorta" because auto-links are identified as "tag" tokens.
    _sorta_html_tokenize_re = re.compile(r"""
        (
            \\*  # escapes
            (?:
                # tag
                </?
                (?:\w+)         # tag name
                (?:             # attributes
                    \s+                           # whitespace after tag
                    (?:[^\t<>"'=/]+:)?
                    [^<>"'=/]+=                   # attr name
                    (?:"[^"]*?"|'[^']*?'|[^<>"'=/\s]+)  # value, quoted or unquoted. If unquoted, no spaces allowed
                )*
                \s*/?>
                |
                # auto-link (e.g., <http://www.activestate.com/>)
                <[\w~:/?#\[\]@!$&'\(\)*+,;%=\.\\-]+>
                |
                <!--.*?-->      # comment
                |
                <\?.*?\?>       # processing instruction
            )
        )
        """, re.X)

    @mark_stage(Stage.ESCAPE_SPECIAL)
    def _escape_special_chars(self, text: str) -> str:
        # Python markdown note: the HTML tokenization here differs from
        # that in Markdown.pl, hence the behaviour for subtle cases can
        # differ (I believe the tokenizer here does a better job because
        # it isn't susceptible to unmatched '<' and '>' in HTML tags).
        # Note, however, that '>' is not allowed in an auto-link URL
        # here.
        lead_escape_re = re.compile(r'^((?:\\\\)*(?!\\))')
        escaped = []
        is_html_markup = False
        for token in self._sorta_html_tokenize_re.split(text):
            # check token is preceded by 0 or more PAIRS of escapes, because escape pairs
            # escape themselves and don't affect the token
            if is_html_markup and lead_escape_re.match(token):
                # Within tags/HTML-comments/auto-links, encode * and _
                # so they don't conflict with their use in Markdown for
                # italics and strong.  We're replacing each such
                # character with its corresponding MD5 checksum value;
                # this is likely overkill, but it should prevent us from
                # colliding with the escape values by accident.
                escape_seq, token = lead_escape_re.split(token)[1:] or ('', token)
                escaped.append(
                    escape_seq.replace('\\\\', self._escape_table['\\'])
                    + token.replace('*', self._escape_table['*'])
                           .replace('_', self._escape_table['_'])
                )
            else:
                escaped.append(self._encode_backslash_escapes(token.replace('\\<', '&lt;')))
            is_html_markup = not is_html_markup
        return ''.join(escaped)

    def _is_auto_link(self, text):
        if ':' in text and self._auto_link_re.match(text):
            return True
        elif '@' in text and self._auto_email_link_re.match(text):
            return True
        return False

    @mark_stage(Stage.HASH_HTML)
    def _hash_html_spans(self, text: str) -> str:
        # Used for safe_mode.

        def _is_code_span(index, token):
            try:
                if token == '<code>':
                    peek_tokens = split_tokens[index: index + 3]
                elif token == '</code>':
                    peek_tokens = split_tokens[index - 2: index + 1]
                else:
                    return False
            except IndexError:
                return False

            return re.match(r'<code>md5-[A-Fa-f0-9]{32}</code>', ''.join(peek_tokens))

        def _is_comment(token):
            if self.safe_mode == 'replace':
                # don't bother processing each section of comment in replace mode. Just do the whole thing
                return
            return re.match(r'(<!--)(.*)(-->)', token)

        tokens = []
        split_tokens = self._sorta_html_tokenize_re.split(text)
        is_html_markup = False
        for index, token in enumerate(split_tokens):
            if is_html_markup and not self._is_auto_link(token) and not _is_code_span(index, token):
                is_comment = _is_comment(token)
                if is_comment:
                    tokens.append(self._hash_span(self._sanitize_html(is_comment.group(1))))
                    # sanitise but leave comment body intact for further markdown processing
                    tokens.append(self._sanitize_html(is_comment.group(2)))
                    tokens.append(self._hash_span(self._sanitize_html(is_comment.group(3))))
                else:
                    tokens.append(self._hash_span(self._sanitize_html(token)))
            else:
                tokens.append(self._encode_incomplete_tags(token))
            is_html_markup = not is_html_markup
        return ''.join(tokens)

    def _unhash_html_spans(self, text: str, spans=True, code=False) -> str:
        '''
        Recursively unhash a block of text

        Args:
            spans: unhash anything from `self.html_spans`
            code: unhash code blocks
        '''
        orig = ''
        while text != orig:
            if spans:
                for key, sanitized in list(self.html_spans.items()):
                    text = text.replace(key, sanitized)
            if code:
                for code, key in list(self._code_table.items()):
                    text = text.replace(key, code)
            orig = text
        return text

    def _sanitize_html(self, s: str) -> str:
        if self.safe_mode == "replace":
            return self.html_removed_text
        elif self.safe_mode == "escape":
            replacements = [
                ('&', '&amp;'),
                ('<', '&lt;'),
                ('>', '&gt;'),
            ]
            for before, after in replacements:
                s = s.replace(before, after)
            return s
        else:
            raise MarkdownError("invalid value for 'safe_mode': %r (must be "
                                "'escape' or 'replace')" % self.safe_mode)

    _inline_link_title = re.compile(r'''
            (                   # \1
              [ \t]+
              (['"])            # quote char = \2
              (?P<title>.*?)
              \2
            )?                  # title is optional
          \)$
        ''', re.X | re.S)
    _tail_of_reference_link_re = re.compile(r'''
          # Match tail of: [text][id]
          [ ]?          # one optional space
          (?:\n[ ]*)?   # one optional newline followed by spaces
          \[
            (?P<id>.*?)
          \]
        ''', re.X | re.S)

    _whitespace = re.compile(r'\s*')

    _strip_anglebrackets = re.compile(r'<(.*)>.*')

    def _find_non_whitespace(self, text: str, start: int) -> int:
        """Returns the index of the first non-whitespace character in text
        after (and including) start
        """
        match = self._whitespace.match(text, start)
        return match.end() if match else len(text)

    def _find_balanced(self, text: str, start: int, open_c: str, close_c: str) -> int:
        """Returns the index where the open_c and close_c characters balance
        out - the same number of open_c and close_c are encountered - or the
        end of string if it's reached before the balance point is found.
        """
        i = start
        l = len(text)
        count = 1
        while count > 0 and i < l:
            if text[i] == open_c:
                count += 1
            elif text[i] == close_c:
                count -= 1
            i += 1
        return i

    # https://developer.mozilla.org/en-US/docs/web/http/basics_of_http/data_urls
    # https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types
    _data_url_re = re.compile(r'''
        data:
        # in format type/subtype;parameter=optional
        (?P<mime>\w+/[\w+\.-]+(?:;\w+=[\w+\.-]+)?)?
        # optional base64 token
        (?P<token>;base64)?
        ,(?P<data>.*)
    ''', re.X)

    def _protect_url(self, url: str) -> str:
        '''
        Function that passes a URL through `_html_escape_url` to remove any nasty characters,
        and then hashes the now "safe" URL to prevent other safety mechanisms from tampering
        with it (eg: escaping "&" in URL parameters)
        '''
        data_url = self._data_url_re.match(url)
        charset = None
        if data_url is not None:
            mime = data_url.group('mime') or ''
            if mime.startswith('image/') and data_url.group('token') == ';base64':
                charset='base64'
        url = _html_escape_url(url, safe_mode=self.safe_mode, charset=charset)
        key = _hash_text(url)
        self._escape_table[url] = key
        return key

    _safe_protocols = r'(?:https?|ftp):\/\/|(?:mailto|tel):'

    @property
    def _safe_href(self):
        '''
        _safe_href is adapted from pagedown's Markdown.Sanitizer.js
        From: https://github.com/StackExchange/pagedown/blob/master/LICENSE.txt
        Original Showdown code copyright (c) 2007 John Fraser
        Modifications and bugfixes (c) 2009 Dana Robinson
        Modifications and bugfixes (c) 2009-2014 Stack Exchange Inc.
        '''
        safe = r'-\w'
        # omitted ['"<>] for XSS reasons
        less_safe = r'#/\.!#$%&\(\)\+,/:;=\?@\[\]^`\{\}\|~'
        # dot seperated hostname, optional port number, not followed by protocol seperator
        domain = r'(?:[{}]+(?:\.[{}]+)*)(?:(?<!tel):\d+/?)?(?![^:/]*:/*)'.format(safe, safe)
        fragment = r'[%s]*' % (safe + less_safe)

        return re.compile(r'^(?:({})?({})({})|(#|\.{{,2}}/)({}))$'.format(self._safe_protocols, domain, fragment, fragment), re.I)

    @mark_stage(Stage.LINKS)
    def _do_links(self, text: str) -> str:
        """Turn Markdown link shortcuts into XHTML <a> and <img> tags.

        This is a combination of Markdown.pl's _DoAnchors() and
        _DoImages(). They are done together because that simplified the
        approach. It was necessary to use a different approach than
        Markdown.pl because of the lack of atomic matching support in
        Python's regex engine used in $g_nested_brackets.
        """
        link_processor = LinkProcessor(self, None)
        if link_processor.test(text):
            text = link_processor.run(text)
        return text

    def header_id_from_text(self,
        text: str,
        prefix: str,
        n: Optional[int] = None
    ) -> str:
        """Generate a header id attribute value from the given header
        HTML content.

        This is only called if the "header-ids" extra is enabled.
        Subclasses may override this for different header ids.

        @param text {str} The text of the header tag
        @param prefix {str} The requested prefix for header ids. This is the
            value of the "header-ids" extra key, if any. Otherwise, None.
        @param n {int} (unused) The <hN> tag number, i.e. `1` for an <h1> tag.
        @returns {str} The value for the header tag's "id" attribute. Return
            None to not have an id attribute and to exclude this header from
            the TOC (if the "toc" extra is specified).
        """
        header_id = _slugify(text)
        if prefix and isinstance(prefix, str):
            header_id = prefix + '-' + header_id

        self._count_from_header_id[header_id] += 1
        if 0 == len(header_id) or self._count_from_header_id[header_id] > 1:
            header_id += '-%s' % self._count_from_header_id[header_id]

        return header_id

    def _header_id_exists(self, text: str) -> bool:
        header_id = _slugify(text)
        prefix = self.extras['header-ids'].get('prefix')
        if prefix and isinstance(prefix, str):
            header_id = prefix + '-' + header_id
        return header_id in self._count_from_header_id or header_id in map(lambda x: x[1], self._toc)

    def _toc_add_entry(self, level: int, id: str, name: str) -> None:
        if level > self._toc_depth:
            return
        if self._toc is None:
            self._toc = []
        self._toc.append((level, id, self._unescape_special_chars(name)))

    _h_re_base = r'''
        (^(.+)[ \t]{0,99}\n(=+|-+)[ \t]*\n+)
        |
        (^(\#{1,6})  # \1 = string of #'s
        [ \t]%s
        (.+?)       # \2 = Header text
        [ \t]{0,99}
        (?<!\\)     # ensure not an escaped trailing '#'
        \#*         # optional closing #'s (not counted)
        \n+
        )
        '''

    _h_re = re.compile(_h_re_base % '*', re.X | re.M)
    _h_re_tag_friendly = re.compile(_h_re_base % '+', re.X | re.M)

    def _h_sub(self, match: re.Match) -> str:
        '''Handles processing markdown headers'''
        if match.group(1) is not None and match.group(3) == "-":
            return match.group(1)
        elif match.group(1) is not None:
            # Setext header
            n = {"=": 1, "-": 2}[match.group(3)[0]]
            header_group = match.group(2)
        else:
            # atx header
            n = len(match.group(5))
            header_group = match.group(6)

        demote_headers = self.extras.get("demote-headers")
        if demote_headers:
            n = min(n + demote_headers, 6)
        header_id_attr = ""
        if "header-ids" in self.extras:
            header_id = self.header_id_from_text(header_group,
                self.extras["header-ids"].get('prefix'), n)
            if header_id:
                header_id_attr = ' id="%s"' % header_id
        html = self._run_span_gamut(header_group)
        if "toc" in self.extras and header_id:
            self._toc_add_entry(n, header_id, html)
        return "<h%d%s>%s</h%d>\n\n" % (n, header_id_attr, html, n)

    _h_tag_re = re.compile(r'''
        ^<h([1-6])(.*)>  # \1 tag num, \2 attrs
        (.*)  # \3 text
        </h\1>
    ''', re.X | re.M)

    def _h_tag_sub(self, match: re.Match) -> str:
        '''Different to `_h_sub` in that this function handles existing HTML headers'''
        text = match.string[match.start(): match.end()]
        h_level = int(match.group(1))
        # extract id= attr from tag, trying to account for regex "misses"
        id_attr = (re.match(r'.*?id=(\S+)?.*', match.group(2) or '') or '')
        if id_attr:
            # if id attr exists, extract that
            id_attr = id_attr.group(1) or ''
        id_attr = id_attr.strip('\'" ')
        h_text = match.group(3)

        # check if header was already processed (ie: was a markdown header rather than HTML)
        if id_attr and self._header_id_exists(id_attr):
            return text

        # generate new header id if none existed
        header_id = id_attr or self.header_id_from_text(h_text, self.extras['header-ids'].get('prefix'), h_level)
        if "toc" in self.extras:
            self._toc_add_entry(h_level, header_id, h_text)
        if header_id and not id_attr:
            # '<h[digit]' + new ID + '...'
            return text[:3] + ' id="%s"' % header_id + text[3:]
        return text

    @mark_stage(Stage.HEADERS)
    def _do_headers(self, text: str) -> str:
        # Setext-style headers:
        #     Header 1
        #     ========
        #
        #     Header 2
        #     --------

        # atx-style headers:
        #   # Header 1
        #   ## Header 2
        #   ## Header 2 with closing hashes ##
        #   ...
        #   ###### Header 6

        if 'tag-friendly' in self.extras:
            return self._h_re_tag_friendly.sub(self._h_sub, text)
        return self._h_re.sub(self._h_sub, text)

    _marker_ul_chars = '*+-'
    _marker_any = r'(?:[%s]|\d+\.)' % _marker_ul_chars
    _marker_ul = '(?:[%s])' % _marker_ul_chars
    _marker_ol = r'(?:\d+\.)'

    def _list_sub(self, match: re.Match) -> str:
        lst = match.group(1)
        lst_type = match.group(4) in self._marker_ul_chars and "ul" or "ol"

        if lst_type == 'ol' and match.group(4) != '1.':
            # if list doesn't start at 1 then set the ol start attribute
            lst_opts = ' start="%s"' % match.group(4)[:-1]
        else:
            lst_opts = ''

        lst_opts = lst_opts + self._html_class_str_from_tag(lst_type)

        result = self._process_list_items(lst)
        if self.list_level:
            return "<{}{}>\n{}</{}>\n".format(lst_type, lst_opts, result, lst_type)
        else:
            return "<{}{}>\n{}</{}>\n\n".format(lst_type, lst_opts, result, lst_type)

    @mark_stage(Stage.LISTS)
    def _do_lists(self, text: str) -> str:
        # Form HTML ordered (numbered) and unordered (bulleted) lists.

        # Iterate over each *non-overlapping* list match.
        pos = 0
        while True:
            # Find the *first* hit for either list style (ul or ol). We
            # match ul and ol separately to avoid adjacent lists of different
            # types running into each other (see issue #16).
            hits = []
            for marker_pat in (self._marker_ul, self._marker_ol):
                less_than_tab = self.tab_width - 1
                other_marker_pat = self._marker_ul if marker_pat == self._marker_ol else self._marker_ol
                whole_list = r'''
                    (                   # \1 = whole list
                      (                 # \2
                        ([ ]{0,%d})     # \3 = the indentation level of the list item marker
                        (%s)            # \4 = first list item marker
                        [ \t]+
                        (?!\ *\4\ )     # '- - - ...' isn't a list. See 'not_quite_a_list' test case.
                      )
                      (?:.+?)
                      (                 # \5
                          \Z
                        |
                          \n{2,}
                          (?=\S)
                          (?!           # Negative lookahead for another list item marker
                            [ \t]*
                            %s[ \t]+
                          )
                        |
                          \n+
                          (?=
                            \3          # lookahead for a different style of list item marker
                            %s[ \t]+
                          )
                      )
                    )
                ''' % (less_than_tab, marker_pat, marker_pat, other_marker_pat)
                if self.list_level:  # sub-list
                    list_re = re.compile("^"+whole_list, re.X | re.M | re.S)
                else:
                    list_re = re.compile(r"(?:(?<=\n\n)|\A\n?)"+whole_list,
                                         re.X | re.M | re.S)
                match = list_re.search(text, pos)
                if match:
                    hits.append((match.start(), match))
            if not hits:
                break
            hits.sort()
            match = hits[0][1]
            start, end = match.span()
            middle = self._list_sub(match)
            text = text[:start] + middle + text[end:]
            pos = start + len(middle)  # start pos for next attempted match

        return text

    _list_item_re = re.compile(r'''
        (\n)?                   # leading line = \1
        (^[ \t]*)               # leading whitespace = \2
        (?P<marker>{}) [ \t]+   # list marker = \3
        ((?:.+?)                # list item text = \4
        (\n{{1,2}}))              # eols = \5
        (?= \n* (\Z | \2 (?P<next_marker>{}) [ \t]+))
        '''.format(_marker_any, _marker_any),
        re.M | re.X | re.S)

    _task_list_item_re = re.compile(r'''
        (\[[\ xX]\])[ \t]+       # tasklist marker = \1
        (.*)                   # list item text = \2
    ''', re.M | re.X | re.S)

    _task_list_warpper_str = r'<input type="checkbox" class="task-list-item-checkbox" %sdisabled> %s'

    def _task_list_item_sub(self, match: re.Match) -> str:
        marker = match.group(1)
        item_text = match.group(2)
        if marker in ['[x]','[X]']:
            return self._task_list_warpper_str % ('checked ', item_text)
        elif marker == '[ ]':
            return self._task_list_warpper_str % ('', item_text)
        # returning None has same effect as returning empty str, but only
        # one makes the type checker happy
        return ''

    _last_li_endswith_two_eols = False
    def _list_item_sub(self, match: re.Match) -> str:
        item = match.group(4)
        leading_line = match.group(1)
        if leading_line or "\n\n" in item or self._last_li_endswith_two_eols:
            item = self._uniform_outdent(item, min_outdent=' ', max_outdent=self.tab)[1]
            item = self._run_block_gamut(item)
        else:
            # Recursion for sub-lists:
            item = self._do_lists(self._uniform_outdent(item, min_outdent=' ')[1])
            if item.endswith('\n'):
                item = item[:-1]
            item = self._run_span_gamut(item)
        self._last_li_endswith_two_eols = (len(match.group(5)) == 2)

        if "task_list" in self.extras:
            item = self._task_list_item_re.sub(self._task_list_item_sub, item)

        return "<li>%s</li>\n" % item

    def _process_list_items(self, list_str: str) -> str:
        # Process the contents of a single ordered or unordered list,
        # splitting it into individual list items.

        # The $g_list_level global keeps track of when we're inside a list.
        # Each time we enter a list, we increment it; when we leave a list,
        # we decrement. If it's zero, we're not in a list anymore.
        #
        # We do this because when we're not inside a list, we want to treat
        # something like this:
        #
        #       I recommend upgrading to version
        #       8. Oops, now this line is treated
        #       as a sub-list.
        #
        # As a single paragraph, despite the fact that the second line starts
        # with a digit-period-space sequence.
        #
        # Whereas when we're inside a list (or sub-list), that line will be
        # treated as the start of a sub-list. What a kludge, huh? This is
        # an aspect of Markdown's syntax that's hard to parse perfectly
        # without resorting to mind-reading. Perhaps the solution is to
        # change the syntax rules such that sub-lists must start with a
        # starting cardinal number; e.g. "1." or "a.".
        self.list_level += 1
        self._last_li_endswith_two_eols = False
        list_str = list_str.rstrip('\n') + '\n'
        list_str = self._list_item_re.sub(self._list_item_sub, list_str)
        self.list_level -= 1
        return list_str

    def _get_pygments_lexer(self, lexer_name: str):
        '''
        Returns:
            `pygments.Lexer` or None if a lexer matching `lexer_name` is
            not found
        '''
        try:
            from pygments import lexers, util
        except ImportError:
            return None
        try:
            return lexers.get_lexer_by_name(lexer_name)
        except util.ClassNotFound:
            return None

    def _color_with_pygments(
        self,
        codeblock: str,
        lexer,
        **formatter_opts
    ) -> str:
        '''
        TODO: this function is only referenced by the `FencedCodeBlocks`
        extra. May be worth moving over there

        Args:
            codeblock: the codeblock to highlight
            lexer (pygments.Lexer): lexer to use
            formatter_opts: pygments HtmlFormatter options
        '''
        import pygments
        import pygments.formatters

        class HtmlCodeFormatter(pygments.formatters.HtmlFormatter):
            def _wrap_code(self, inner):
                """A function for use in a Pygments Formatter which
                wraps in <code> tags.
                """
                yield 0, "<code>"
                yield from inner
                yield 0, "</code>"

            def _add_newline(self, inner):
                # Add newlines around the inner contents so that _strict_tag_block_re matches the outer div.
                yield 0, "\n"
                yield from inner
                yield 0, "\n"

            def wrap(self, source, outfile=None):
                """Return the source with a code, pre, and div."""
                if outfile is None:
                    # pygments >= 2.12
                    return self._add_newline(self._wrap_pre(self._wrap_code(source)))
                else:
                    # pygments < 2.12
                    return self._wrap_div(self._add_newline(self._wrap_pre(self._wrap_code(source))))

        formatter_opts.setdefault("cssclass", "codehilite")
        formatter = HtmlCodeFormatter(**formatter_opts)
        return pygments.highlight(codeblock, lexer, formatter)

    def _code_block_sub(self, match: re.Match) -> str:
        codeblock = match.group(1)
        codeblock = self._outdent(codeblock)
        codeblock = self._detab(codeblock)
        codeblock = codeblock.lstrip('\n')  # trim leading newlines
        codeblock = codeblock.rstrip()      # trim trailing whitespace

        pre_class_str = self._html_class_str_from_tag("pre")
        code_class_str = self._html_class_str_from_tag("code")

        codeblock = self._encode_code(codeblock)

        return "\n<pre{}><code{}>{}\n</code></pre>\n".format(
            pre_class_str, code_class_str, codeblock)

    def _html_class_str_from_tag(self, tag: str) -> str:
        """Get the appropriate ' class="..."' string (note the leading
        space), if any, for the given tag.
        """
        if "html-classes" not in self.extras:
            return ""
        try:
            html_classes_from_tag = self.extras["html-classes"]
        except TypeError:
            return ""
        else:
            if isinstance(html_classes_from_tag, dict):
                if tag in html_classes_from_tag:
                    return ' class="%s"' % html_classes_from_tag[tag]
        return ""

    @mark_stage(Stage.CODE_BLOCKS)
    def _do_code_blocks(self, text: str) -> str:
        """Process Markdown `<pre><code>` blocks."""
        code_block_re = re.compile(r'''
            (?:\n\n|\A\n?)
            (               # $1 = the code block -- one or more lines, starting with a space/tab
              (?:
                (?:[ ]{%d} | \t)  # Lines must start with a tab or a tab-width of spaces
                .*\n+
              )+
            )
            ((?=^[ ]{0,%d}\S)|\Z)   # Lookahead for non-space at line-start, or end of doc
            # Lookahead to make sure this block isn't already in a code block.
            # Needed when syntax highlighting is being used.
            (?!([^<]|<(/?)span)*\</code\>)
            ''' % (self.tab_width, self.tab_width),
            re.M | re.X)
        return code_block_re.sub(self._code_block_sub, text)

    # Rules for a code span:
    # - backslash escapes are not interpreted in a code span
    # - to include one or or a run of more backticks the delimiters must
    #   be a longer run of backticks
    # - cannot start or end a code span with a backtick; pad with a
    #   space and that space will be removed in the emitted HTML
    # See `test/tm-cases/escapes.text` for a number of edge-case
    # examples.
    _code_span_re = re.compile(r'''
            (?<!\\)
            (`+)        # \1 = Opening run of `
            (?!`)       # See Note A test/tm-cases/escapes.text
            (.+?)       # \2 = The code block
            (?<!`)
            \1          # Matching closer
            (?!`)
        ''', re.X | re.S)

    def _code_span_sub(self, match: re.Match) -> str:
        c = match.group(2).strip(" \t")
        c = self._encode_code(c)
        return "<code{}>{}</code>".format(self._html_class_str_from_tag("code"), c)

    @mark_stage(Stage.CODE_SPANS)
    def _do_code_spans(self, text: str) -> str:
        #   *   Backtick quotes are used for <code></code> spans.
        #
        #   *   You can use multiple backticks as the delimiters if you want to
        #       include literal backticks in the code span. So, this input:
        #
        #         Just type ``foo `bar` baz`` at the prompt.
        #
        #       Will translate to:
        #
        #         <p>Just type <code>foo `bar` baz</code> at the prompt.</p>
        #
        #       There's no arbitrary limit to the number of backticks you
        #       can use as delimters. If you need three consecutive backticks
        #       in your code, use four for delimiters, etc.
        #
        #   *   You can use spaces to get literal backticks at the edges:
        #
        #         ... type `` `bar` `` ...
        #
        #       Turns to:
        #
        #         ... type <code>`bar`</code> ...
        return self._code_span_re.sub(self._code_span_sub, text)

    def _encode_code(self, text: str) -> str:
        """Encode/escape certain characters inside Markdown code runs.
        The point is that in code, these characters are literals,
        and lose their special Markdown meanings.
        """
        replacements = [
            # Encode all ampersands; HTML entities are not
            # entities within a Markdown code span.
            ('&', '&amp;'),
            # Do the angle bracket song and dance:
            ('<', '&lt;'),
            ('>', '&gt;'),
        ]
        for before, after in replacements:
            text = text.replace(before, after)
        hashed = _hash_text(text)
        self._code_table[text] = hashed
        return hashed

    _strong_re = re.compile(r"(\*\*|__)(?=\S)(.+?[*_]?)(?<=\S)\1", re.S)
    _em_re = re.compile(r"(\*|_)(?=\S)(.*?\S)\1", re.S)

    @mark_stage(Stage.ITALIC_AND_BOLD)
    def _do_italics_and_bold(self, text: str) -> str:
        # <strong> must go first:
        text = self._strong_re.sub(r"<strong>\2</strong>", text)
        text = self._em_re.sub(r"<em>\2</em>", text)
        return text

    _block_quote_base = r'''
        (                           # Wrap whole match in \1
          (
            ^[ \t]*>%s[ \t]?        # '>' at the start of a line
              .+\n                  # rest of the first line
            (.+\n)*                 # subsequent consecutive lines
          )+
        )
    '''
    _block_quote_re = re.compile(_block_quote_base % '', re.M | re.X)
    _block_quote_re_spoiler = re.compile(_block_quote_base % '[ \t]*?!?', re.M | re.X)
    _bq_one_level_re = re.compile('^[ \t]*>[ \t]?', re.M)
    _bq_one_level_re_spoiler = re.compile('^[ \t]*>[ \t]*?![ \t]?', re.M)
    _bq_all_lines_spoilers = re.compile(r'\A(?:^[ \t]*>[ \t]*?!.*[\n\r]*)+\Z', re.M)
    _html_pre_block_re = re.compile(r'(\s*<pre>.+?</pre>)', re.S)
    def _dedent_two_spaces_sub(self, match: re.Match) -> str:
        return re.sub(r'(?m)^  ', '', match.group(1))

    def _block_quote_sub(self, match: re.Match) -> str:
        bq = match.group(1)
        is_spoiler = 'spoiler' in self.extras and self._bq_all_lines_spoilers.match(bq)
        # trim one level of quoting
        if is_spoiler:
            bq = self._bq_one_level_re_spoiler.sub('', bq)
        else:
            bq = self._bq_one_level_re.sub('', bq)
        # trim whitespace-only lines
        bq = self._ws_only_line_re.sub('', bq)
        bq = self._run_block_gamut(bq)          # recurse

        bq = re.sub('(?m)^', '  ', bq)
        # These leading spaces screw with <pre> content, so we need to fix that:
        bq = self._html_pre_block_re.sub(self._dedent_two_spaces_sub, bq)

        if is_spoiler:
            return '<blockquote class="spoiler">\n%s\n</blockquote>\n\n' % bq
        else:
            return '<blockquote>\n%s\n</blockquote>\n\n' % bq

    @mark_stage(Stage.BLOCK_QUOTES)
    def _do_block_quotes(self, text: str) -> str:
        if '>' not in text:
            return text
        if 'spoiler' in self.extras:
            return self._block_quote_re_spoiler.sub(self._block_quote_sub, text)
        else:
            return self._block_quote_re.sub(self._block_quote_sub, text)

    @mark_stage(Stage.PARAGRAPHS)
    def _form_paragraphs(self, text: str) -> str:
        # Strip leading and trailing lines:
        text = text.strip('\n')

        # Wrap <p> tags.
        grafs = []
        for i, graf in enumerate(re.split(r"\n{2,}", text)):
            if graf in self.html_blocks:
                # Unhashify HTML blocks
                grafs.append(self.html_blocks[graf])
            else:
                cuddled_list = None
                if "cuddled-lists" in self.extras:
                    # Need to put back trailing '\n' for `_list_item_re`
                    # match at the end of the paragraph.
                    li = self._list_item_re.search(graf + '\n')
                    # Two of the same list marker in this paragraph: a likely
                    # candidate for a list cuddled to preceding paragraph
                    # text (issue 33). Note the `[-1]` is a quick way to
                    # consider numeric bullets (e.g. "1." and "2.") to be
                    # equal.
                    if (li and len(li.group(2)) <= 3
                            and (
                                    (li.group("next_marker") and li.group("marker")[-1] == li.group("next_marker")[-1])
                                    or
                                    li.group("next_marker") is None
                            )
                    ):
                        start = li.start()
                        cuddled_list = self._do_lists(graf[start:]).rstrip("\n")
                        if re.match(r'^<(?:ul|ol).*?>', cuddled_list):
                            graf = graf[:start]
                        else:
                            # Not quite a cuddled list. (See not_quite_a_list_cuddled_lists test case)
                            # Store as a simple paragraph.
                            graf = cuddled_list
                            cuddled_list = None

                # Wrap <p> tags.
                graf = self._run_span_gamut(graf)
                grafs.append("<p%s>" % self._html_class_str_from_tag('p') + graf.lstrip(" \t") + "</p>")

                if cuddled_list:
                    grafs.append(cuddled_list)

        return "\n\n".join(grafs)

    def _add_footnotes(self, text: str) -> str:
        if self.footnotes:
            footer = [
                '<div class="footnotes">',
                '<hr' + self.empty_element_suffix,
                '<ol>',
            ]

            if not self.footnote_title:
                self.footnote_title = "Jump back to footnote %d in the text."
            if not self.footnote_return_symbol:
                self.footnote_return_symbol = "&#8617;"

            # self.footnotes is generated in _strip_footnote_definitions, which runs re.sub on the whole
            # text. This means that the dict keys are inserted in order of appearance. Use the dict to
            # sort footnote ids by that same order
            self.footnote_ids.sort(key=lambda a: list(self.footnotes.keys()).index(a))
            for i, id in enumerate(self.footnote_ids):
                if i != 0:
                    footer.append('')
                footer.append('<li id="fn-%s">' % id)
                footer.append(self._run_block_gamut(self.footnotes[id]))
                try:
                    backlink = ('<a href="#fnref-%s" ' +
                            'class="footnoteBackLink" ' +
                            'title="' + self.footnote_title + '">' +
                            self.footnote_return_symbol +
                            '</a>') % (id, i+1)
                except TypeError:
                    log.debug("Footnote error. `footnote_title` "
                              "must include parameter. Using defaults.")
                    backlink = ('<a href="#fnref-%s" '
                        'class="footnoteBackLink" '
                        'title="Jump back to footnote %d in the text.">'
                        '&#8617;</a>' % (id, i+1))

                if footer[-1].endswith("</p>"):
                    footer[-1] = footer[-1][:-len("</p>")] \
                        + '&#160;' + backlink + "</p>"
                else:
                    footer.append("\n<p>%s</p>" % backlink)
                footer.append('</li>')
            footer.append('</ol>')
            footer.append('</div>')
            return text + '\n\n' + '\n'.join(footer)
        else:
            return text

    _naked_lt_re = re.compile(r'<(?![a-z/?\$!])', re.I)
    _naked_gt_re = re.compile(r'''(?<![a-z0-9?!/'"-])>''', re.I)

    def _encode_amps_and_angles(self, text: str) -> str:
        # Smart processing for ampersands and angle brackets that need
        # to be encoded.
        text = _AMPERSAND_RE.sub('&amp;', text)

        # Encode naked <'s
        text = self._naked_lt_re.sub('&lt;', text)

        # Encode naked >'s
        # Note: Other markdown implementations (e.g. Markdown.pl, PHP
        # Markdown) don't do this.
        text = self._naked_gt_re.sub('&gt;', text)
        return text

    _incomplete_tags_re = re.compile(r"<(!--|/?\w+?(?!\w)\s*?.+?(?:[\s/]+?|$))")

    def _encode_incomplete_tags(self, text: str) -> str:
        if self.safe_mode not in ("replace", "escape"):
            return text

        if self._is_auto_link(text):
            return text  # this is not an incomplete tag, this is a link in the form <http://x.y.z>

        def incomplete_tags_sub(match):
            return match.group().replace('<', '&lt;')

        return self._incomplete_tags_re.sub(incomplete_tags_sub, text)

    def _encode_backslash_escapes(self, text: str) -> str:
        for ch, escape in list(self._escape_table.items()):
            text = text.replace("\\"+ch, escape)
        return text

    _auto_link_re = re.compile(r'<((https?|ftp):[^\'">\s]+)>', re.I)
    def _auto_link_sub(self, match: re.Match) -> str:
        g1 = match.group(1)
        return '<a href="{}">{}</a>'.format(self._protect_url(g1), g1)

    _auto_email_link_re = re.compile(r"""
          <
           (?:mailto:)?
          (
              [-.\w]+
              \@
              [-\w]+(\.[-\w]+)*\.[a-z]+
          )
          >
        """, re.I | re.X | re.U)
    def _auto_email_link_sub(self, match: re.Match) -> str:
        return self._encode_email_address(
            self._unescape_special_chars(match.group(1)))

    def _do_auto_links(self, text: str) -> str:
        text = self._auto_link_re.sub(self._auto_link_sub, text)
        text = self._auto_email_link_re.sub(self._auto_email_link_sub, text)
        return text

    def _encode_email_address(self, addr: str) -> str:
        #  Input: an email address, e.g. "foo@example.com"
        #
        #  Output: the email address as a mailto link, with each character
        #      of the address encoded as either a decimal or hex entity, in
        #      the hopes of foiling most address harvesting spam bots. E.g.:
        #
        #    <a href="&#x6D;&#97;&#105;&#108;&#x74;&#111;:&#102;&#111;&#111;&#64;&#101;
        #       x&#x61;&#109;&#x70;&#108;&#x65;&#x2E;&#99;&#111;&#109;">&#102;&#111;&#111;
        #       &#64;&#101;x&#x61;&#109;&#x70;&#108;&#x65;&#x2E;&#99;&#111;&#109;</a>
        #
        #  Based on a filter by Matthew Wickline, posted to the BBEdit-Talk
        #  mailing list: <http://tinyurl.com/yu7ue>
        chars = [_xml_encode_email_char_at_random(ch)
                 for ch in "mailto:" + addr]
        # Strip the mailto: from the visible part.
        addr = '<a href="%s">%s</a>' \
               % (''.join(chars), ''.join(chars[7:]))
        return addr

    def _unescape_special_chars(self, text: str) -> str:
        # Swap back in all the special characters we've hidden.
        hashmap = tuple(self._escape_table.items()) + tuple(self._code_table.items())
        # html_blocks table is in format {hash: item} compared to usual {item: hash}
        hashmap += tuple(tuple(reversed(i)) for i in self.html_blocks.items())
        while True:
            orig_text = text
            for ch, hash in hashmap:
                text = text.replace(hash, ch)
            if text == orig_text:
                break
        return text

    def _outdent(self, text: str) -> str:
        # Remove one level of line-leading tabs or spaces
        return self._outdent_re.sub('', text)

    def _hash_span(self, text: str) -> str:
        '''
        Wrapper around `_hash_text` that also adds the hash to `self.hash_spans`,
        meaning it will be automatically unhashed during conversion.
        '''
        key = _hash_text(text)
        self.html_spans[key] = text
        return key

    @staticmethod
    def _uniform_outdent(
        text: str,
        min_outdent: Optional[str] = None,
        max_outdent: Optional[str] = None
    ) -> tuple[str, str]:
        '''
        Removes the smallest common leading indentation from each (non empty)
        line of `text` and returns said indent along with the outdented text.

        Args:
            min_outdent: make sure the smallest common whitespace is at least this size
            max_outdent: the maximum amount a line can be outdented by
        '''

        # find the leading whitespace for every line
        whitespace: list[Union[str, None]] = [
            re.findall(r'^[ \t]*', line)[0] if line else None
            for line in text.splitlines()
        ]
        whitespace_not_empty = [i for i in whitespace if i is not None]

        # if no whitespace detected (ie: no lines in code block, issue #505)
        if not whitespace_not_empty:
            return '', text

        # get minimum common whitespace
        outdent = min(whitespace_not_empty)
        # adjust min common ws to be within bounds
        if min_outdent is not None:
            outdent = min([i for i in whitespace_not_empty if i >= min_outdent] or [min_outdent])
        if max_outdent is not None:
            outdent = min(outdent, max_outdent)

        outdented = []
        for line_ws, line in zip(whitespace, text.splitlines(True)):
            if line.startswith(outdent):
                # if line starts with smallest common ws, dedent it
                outdented.append(line.replace(outdent, '', 1))
            elif line_ws is not None and line_ws < outdent:
                # if less indented than min common whitespace then outdent as much as possible
                outdented.append(line.replace(line_ws, '', 1))
            else:
                outdented.append(line)

        return outdent, ''.join(outdented)

    @staticmethod
    def _uniform_indent(
        text: str,
        indent: str,
        include_empty_lines: bool = False,
        indent_empty_lines: bool = False
    ) -> str:
        '''
        Uniformly indent a block of text by a fixed amount

        Args:
            text: the text to indent
            indent: a string containing the indent to apply
            include_empty_lines: don't remove whitespace only lines
            indent_empty_lines: indent whitespace only lines with the rest of the text
        '''
        blocks = []
        for line in text.splitlines(True):
            if line.strip() or indent_empty_lines:
                blocks.append(indent + line)
            elif include_empty_lines:
                blocks.append(line)
            else:
                blocks.append('')
        return ''.join(blocks)

    @staticmethod
    def _match_overlaps_substr(text, match: re.Match, substr: str) -> bool:
        '''
        Checks if a regex match overlaps with a substring in the given text.
        '''
        for instance in re.finditer(re.escape(substr), text):
            start, end = instance.span()
            if start <= match.start() <= end:
                return True
            if start <= match.end() <= end:
                return True
        return False


class MarkdownWithExtras(Markdown):
    """A markdowner class that enables most extras:

    - footnotes
    - fenced-code-blocks (only highlights code if 'pygments' Python module on path)

    These are not included:
    - pyshell (specific to Python-related documenting)
    - code-friendly (because it *disables* part of the syntax)
    - link-patterns (because you need to specify some actual
      link-patterns anyway)
    """
    extras = ["footnotes", "fenced-code-blocks"]  # type: ignore


# ----------------------------------------------------------
# Extras
# ----------------------------------------------------------

# Base classes
# ----------------------------------------------------------

class Extra(ABC):
    _registry: dict[str, type['Extra']] = {}
    _exec_order: dict[Stage, tuple[list[type['Extra']], list[type['Extra']]]] = {}

    name: str
    '''
    An identifiable name that users can use to invoke the extra
    in the Markdown class
    '''
    order: tuple[Collection[Union[Stage, type['Extra']]], Collection[Union[Stage, type['Extra']]]]
    '''
    Tuple of two iterables containing the stages/extras this extra will run before and
    after, respectively
    '''

    def __init__(self, md: Markdown, options: Optional[dict]):
        '''
        Args:
            md: An instance of `Markdown`
            options: a dict of settings to alter the extra's behaviour
        '''
        self.md = md
        self.options = options if options is not None else {}

    @classmethod
    def deregister(cls):
        '''
        Removes the class from the extras registry and unsets its execution order.
        '''
        if cls.name in cls._registry:
            del cls._registry[cls.name]

        for exec_order in Extra._exec_order.values():
            # find everywhere this extra is mentioned and remove it
            for section in exec_order:
                while cls in section:
                    section.remove(cls)

    @classmethod
    def register(cls):
        '''
        Registers the class for use with `Markdown` and calculates its execution order based on
        the `order` class attribute.
        '''
        cls._registry[cls.name] = cls

        for index, item in enumerate((*cls.order[0], *cls.order[1])):
            before = index < len(cls.order[0])
            if not isinstance(item, Stage) and issubclass(item, Extra):
                # eg: FencedCodeBlocks
                for exec_orders in Extra._exec_order.values():
                    # insert this extra everywhere the other one is mentioned
                    for section in exec_orders:
                        if item in section:
                            to_index = section.index(item)
                            if not before:
                                to_index += 1
                            section.insert(to_index, cls)
            else:
                # eg: Stage.PREPROCESS
                Extra._exec_order.setdefault(item, ([], []))
                if cls in Extra._exec_order[item][0 if before else 1]:
                    # extra is already runnig after this stage. Don't duplicate that effort
                    continue
                if before:
                    Extra._exec_order[item][0].insert(0, cls)
                else:
                    Extra._exec_order[item][1].append(cls)

    @abstractmethod
    def run(self, text: str) -> str:
        '''
        Run the extra against the given text.

        Returns:
            The new text after being modified by the extra
        '''
        ...

    def test(self, text: str) -> bool:
        '''
        Check a section of markdown to see if this extra should be run upon it.
        The default implementation will always return True but it's recommended to override
        this behaviour to improve performance.
        '''
        return True


class ItalicAndBoldProcessor(Extra):
    '''
    An ABC that provides hooks for dealing with italics and bold syntax.
    This class is set to trigger both before AND after the italics and bold stage.
    This allows any child classes to intercept instances of bold or italic syntax and
    change the output or hash it to prevent it from being processed.

    After the I&B stage any hashes in the `hash_tables` instance variable are replaced.
    '''
    name = 'italic-and-bold-processor'
    order = (Stage.ITALIC_AND_BOLD,), (Stage.ITALIC_AND_BOLD,)

    strong_re = Markdown._strong_re
    em_re = Markdown._em_re

    def __init__(self, md: Markdown, options: Optional[dict]):
        super().__init__(md, options)
        self.hash_table = {}

    def run(self, text):
        if self.md.order < Stage.ITALIC_AND_BOLD:
            text = self.strong_re.sub(self.sub, text)
            text = self.em_re.sub(self.sub, text)
        else:
            # push any hashed values back, using a while loop to deal with recursive hashes
            orig_text = ''
            while orig_text != text:
                orig_text = text
                for key, substr in self.hash_table.items():
                    text = text.replace(key, substr)
        return text

    @abstractmethod
    def sub(self, match: re.Match) -> str:
        # do nothing. Let `Markdown._do_italics_and_bold` do its thing later
        return match.string[match.start(): match.end()]

    def sub_hash(self, match: re.Match) -> str:
        substr = match.string[match.start(): match.end()]
        key = _hash_text(substr)
        self.hash_table[key] = substr
        return key

    def test(self, text):
        if self.md.order < Stage.ITALIC_AND_BOLD:
            return '*' in text or '_' in text
        return self.hash_table and re.search(r'md5-[0-9a-z]{32}', text)


class _LinkProcessorExtraOpts(TypedDict, total=False):
    '''Options for the `LinkProcessor` extra'''
    tags: List[str]
    '''List of tags to be processed by the extra. Default is `['a', 'img']`'''
    inline: bool
    '''Whether to process inline links. Default: True'''
    ref: bool
    '''Whether to process reference links. Default: True'''


class LinkProcessor(Extra):
    name = 'link-processor'
    order = (Stage.ITALIC_AND_BOLD,), (Stage.ESCAPE_SPECIAL,)
    options: _LinkProcessorExtraOpts

    def __init__(self, md: Markdown, options: Optional[dict]):
        options = options or {}
        super().__init__(md, options)

    def parse_inline_anchor_or_image(self, text: str, _link_text: str, start_idx: int) -> Optional[Tuple[str, str, Optional[str], int]]:
        '''
        Parse a string and extract a link from it. This can be an inline anchor or an image.

        Args:
            text: the whole text containing the link
            link_text: the human readable text inside the link
            start_idx: the index of the link within `text`

        Returns:
            None if a link was not able to be parsed from `text`.
            If successful, a tuple is returned containing:

            1. potentially modified version of the `text` param
            2. the URL
            3. the title (can be None if not present)
            4. the index where the link ends within text
        '''
        idx = self.md._find_non_whitespace(text, start_idx + 1)
        if idx == len(text):
            return
        end_idx = idx
        has_anglebrackets = text[idx] == "<"
        if has_anglebrackets:
            end_idx = self.md._find_balanced(text, end_idx+1, "<", ">")
        end_idx = self.md._find_balanced(text, end_idx, "(", ")")
        match = self.md._inline_link_title.search(text, idx, end_idx)
        if not match:
            return
        url, title = text[idx:match.start()], match.group("title")
        if has_anglebrackets:
            url = self.md._strip_anglebrackets.sub(r'\1', url)
        return text, url, title, end_idx

    def process_link_shortrefs(self, text: str, link_text: str, start_idx: int) -> Tuple[Optional[re.Match], str]:
        '''
        Detects shortref links within a string and converts them to normal references

        Args:
            text: the whole text containing the link
            link_text: the human readable text inside the link
            start_idx: the index of the link within `text`

        Returns:
            A tuple containing:

            1. A potential `re.Match` against the link reference within `text` (will be None if not found)
            2. potentially modified version of the `text` param
        '''
        match = None
        # check if there's no tailing id section
        if link_text and re.match(r'[ ]?(?:\n[ ]*)?(?!\[)', text[start_idx:]):
            # try a match with `[]` inserted into the text
            match = self.md._tail_of_reference_link_re.match(f'{text[:start_idx]}[]{text[start_idx:]}', start_idx)
            if match:
                # if we get a match, we'll have to modify the `text` variable to insert the `[]`
                # but we ONLY want to do that if the link_id is valid. This makes sure that we
                # don't get stuck in any loops and also that when a user inputs `[abc]` we don't
                # output `[abc][]` in the final HTML
                if (match.group("id").lower() or link_text.lower()) in self.md.urls:
                    text = f'{text[:start_idx]}[]{text[start_idx:]}'
                else:
                    match = None

        return match, text

    def parse_ref_anchor_or_ref_image(self, text: str, link_text: str, start_idx: int) -> Optional[Tuple[str, Optional[str], Optional[str], int]]:
        '''
        Parse a string and extract a link from it. This can be a reference anchor or image.

        Args:
            text: the whole text containing the link
            link_text: the human readable text inside the link
            start_idx: the index of the link within `text`

        Returns:
            None if a link was not able to be parsed from `text`.
            If successful, a tuple is returned containing:

            1. potentially modified version of the `text` param
            2. the URL (can be None if the reference doesn't exist)
            3. the title (can be None if not present)
            4. the index where the link ends within text
        '''
        match = None
        if 'link-shortrefs' in self.md.extras:
            match, text = self.process_link_shortrefs(text, link_text, start_idx)

        match = match or self.md._tail_of_reference_link_re.match(text, start_idx)
        if not match:
            # text isn't markup
            return

        link_id = match.group("id").lower() or link_text.lower()  # for links like [this][]

        url = self.md.urls.get(link_id)
        title = self.md.titles.get(link_id)
        url_end_idx = match.end()

        return text, url, title, url_end_idx

    def process_image(self, url: str, title_attr: str, link_text: str) -> Tuple[str, int]:
        '''
        Takes a URL, title and link text and returns an HTML `<img>` tag

        Args:
            url: the image URL/src
            title_attr: a string containing the title attribute of the tag (eg: `' title="..."'`)
            link_text: the human readable text portion of the link

        Returns:
            A tuple containing:

            1. The HTML string
            2. The length of the opening HTML tag in the string. For `<img>` it's the whole string.
               This section will be skipped by the link processor
        '''
        img_class_str = self.md._html_class_str_from_tag("img")
        result = (
            f'<img src="{self.md._protect_url(url)}"'
            f' alt="{self.md._hash_span(_xml_escape_attr(link_text))}"'
            f'{title_attr}{img_class_str}{self.md.empty_element_suffix}'
        )
        return result, len(result)

    def process_anchor(self, url: str, title_attr: str, link_text: str) -> Tuple[str, int]:
        '''
        Takes a URL, title and link text and returns an HTML `<a>` tag

        Args:
            url: the URL
            title_attr: a string containing the title attribute of the tag (eg: `' title="..."'`)
            link_text: the human readable text portion of the link

        Returns:
            A tuple containing:

            1. The HTML string
            2. The length of the opening HTML tag in the string. This section will be skipped
               by the link processor
        '''
        if self.md.safe_mode and not self.md._safe_href.match(url):
            result_head = f'<a href="#"{title_attr}>'
        else:
            result_head = f'<a href="{self.md._protect_url(url)}"{title_attr}>'

        return f'{result_head}{link_text}</a>', len(result_head)

    def run(self, text: str):
        MAX_LINK_TEXT_SENTINEL = 3000  # markdown2 issue 24

        # `anchor_allowed_pos` is used to support img links inside
        # anchors, but not anchors inside anchors. An anchor's start
        # pos must be `>= anchor_allowed_pos`.
        anchor_allowed_pos = 0

        curr_pos = 0

        while True:
            # The next '[' is the start of:
            # - an inline anchor:   [text](url "title")
            # - a reference anchor: [text][id]
            # - an inline img:      ![text](url "title")
            # - a reference img:    ![text][id]
            # - a footnote ref:     [^id]
            #   (Only if 'footnotes' extra enabled)
            # - a footnote defn:    [^id]: ...
            #   (Only if 'footnotes' extra enabled) These have already
            #   been stripped in _strip_footnote_definitions() so no
            #   need to watch for them.
            # - a link definition:  [id]: url "title"
            #   These have already been stripped in
            #   _strip_link_definitions() so no need to watch for them.
            # - not markup:         [...anything else...
            try:
                start_idx = text.index('[', curr_pos)
            except ValueError:
                break
            text_length = len(text)

            # Find the matching closing ']'.
            # Markdown.pl allows *matching* brackets in link text so we
            # will here too. Markdown.pl *doesn't* currently allow
            # matching brackets in img alt text -- we'll differ in that
            # regard.
            bracket_depth = 0

            for p in range(
                start_idx + 1,
                min(start_idx + MAX_LINK_TEXT_SENTINEL, text_length)
            ):
                ch = text[p]
                if ch == ']':
                    bracket_depth -= 1
                    if bracket_depth < 0:
                        break
                elif ch == '[':
                    bracket_depth += 1
            else:
                # Closing bracket not found within sentinel length.
                # This isn't markup.
                curr_pos = start_idx + 1
                continue
            link_text = text[start_idx + 1: p]

            # Fix for issue 341 - Injecting XSS into link text
            if self.md.safe_mode:
                link_text = self.md._hash_html_spans(link_text)
                link_text = self.md._unhash_html_spans(link_text)

            # Possibly a footnote ref?
            if "footnotes" in self.md.extras and link_text.startswith("^"):
                normed_id = re.sub(r'\W', '-', link_text[1:])
                if normed_id in self.md.footnotes:
                    result = (
                        f'<sup class="footnote-ref" id="fnref-{normed_id}">'
                        # insert special footnote marker that's easy to find and match against later
                        f'<a href="#fn-{normed_id}">{self.md._footnote_marker}-{normed_id}</a></sup>'
                    )
                    text = text[:start_idx] + result + text[p+1:]
                else:
                    # This id isn't defined, leave the markup alone.
                    curr_pos = p + 1
                continue

            # Now determine what this is by the remainder.
            p += 1

            # -- Extract the URL, title and end index from the link

            # inline anchor or inline img
            if text[p:p + 1] == '(':
                if not self.options.get('inline', True):
                    curr_pos = start_idx + 1
                    continue

                parsed = self.parse_inline_anchor_or_image(text, link_text, p)
                if not parsed:
                    # text isn't markup
                    curr_pos = start_idx + 1
                    continue

                text, url, title, url_end_idx = parsed
                url = self.md._unhash_html_spans(url, code=True)
            # reference anchor or reference img
            else:
                if not self.options.get('ref', True):
                    curr_pos = start_idx + 1
                    continue

                parsed = self.parse_ref_anchor_or_ref_image(text, link_text, p)
                if not parsed:
                    curr_pos = start_idx + 1
                    continue

                text, url, title, url_end_idx = parsed
                if url is None:
                    # This id isn't defined, leave the markup alone.
                    # set current pos to end of link title and continue from there
                    curr_pos = p
                    continue

            # -- Encode and hash the URL and title to avoid conflicts with italics/bold

            url = (
                url
                .replace('*', self.md._escape_table['*'])
                .replace('_', self.md._escape_table['_'])
            )
            if title:
                title = (
                    _xml_escape_attr(title)
                    .replace('*', self.md._escape_table['*'])
                    .replace('_', self.md._escape_table['_'])
                )
                title_str = f' title="{title}"'
            else:
                title_str = ''

            # -- Process the anchor/image

            is_img = start_idx > 0 and text[start_idx-1] == "!"
            if is_img:
                if 'img' not in self.options.get('tags', ['img']):
                    curr_pos = start_idx + 1
                    continue

                start_idx -= 1
                result, skip = self.process_image(url, title_str, link_text)
            elif start_idx >= anchor_allowed_pos:
                if 'a' not in self.options.get('tags', ['a']):
                    curr_pos = start_idx + 1
                    continue

                result, skip = self.process_anchor(url, title_str, link_text)
            else:
                # anchor not allowed here/invalid markup
                curr_pos = start_idx + 1
                continue

            if "smarty-pants" in self.md.extras:
                result = result.replace('"', self.md._escape_table['"'])

            # <img> allowed from curr_pos onwards, <a> allowed from anchor_allowed_pos onwards.
            # this means images can exist within `<a>` tags but anchors can only come after the
            # current anchor has been closed
            curr_pos = start_idx + skip
            anchor_allowed_pos = start_idx + len(result)
            text = text[:start_idx] + result + text[url_end_idx:]

        return text

    def test(self, text):
        return '(' in text or '[' in text


# User facing extras
# ----------------------------------------------------------


class Admonitions(Extra):
    '''
    Enable parsing of RST admonitions
    '''

    name = 'admonitions'
    order = (Stage.BLOCK_GAMUT, Stage.LINK_DEFS), ()

    admonitions = r'admonition|attention|caution|danger|error|hint|important|note|tip|warning'

    admonitions_re = re.compile(r'''
        ^(\ *)\.\.\ (%s)::\ *                # $1 leading indent, $2 the admonition
        (.*)?                                # $3 admonition title
        ((?:\s*\n\1\ {3,}.*)+?)              # $4 admonition body (required)
        (?=\s*(?:\Z|\n{4,}|\n\1?\ {0,2}\S))  # until EOF, 3 blank lines or something less indented
        ''' % admonitions,
        re.IGNORECASE | re.MULTILINE | re.VERBOSE
    )

    def test(self, text):
        return self.admonitions_re.search(text) is not None

    def sub(self, match: re.Match) -> str:
        lead_indent, admonition_name, title, body = match.groups()

        admonition_type = '<strong>%s</strong>' % admonition_name

        # figure out the class names to assign the block
        if admonition_name.lower() == 'admonition':
            admonition_class = 'admonition'
        else:
            admonition_class = 'admonition %s' % admonition_name.lower()

        # titles are generally optional
        if title:
            title = '<em>%s</em>' % title

        # process the admonition body like regular markdown
        body = self.md._run_block_gamut("\n%s\n" % self.md._uniform_outdent(body)[1])

        # indent the body before placing inside the aside block
        admonition = self.md._uniform_indent(
            '{}\n{}\n\n{}\n'.format(admonition_type, title, body),
            self.md.tab, False
        )
        # wrap it in an aside
        admonition = '<aside class="{}">\n{}</aside>'.format(admonition_class, admonition)
        # now indent the whole admonition back to where it started
        return self.md._uniform_indent(admonition, lead_indent, False)

    def run(self, text):
        return self.admonitions_re.sub(self.sub, text)


class Alerts(Extra):
    '''
    Markdown Alerts as per
    https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax#alerts
    '''

    name = 'alerts'
    order = (), (Stage.BLOCK_QUOTES, )

    alert_re = re.compile(r'''
        <blockquote>\s*
        <p>
        \[!(?P<type>NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]
        (?P<closing_tag></p>[ \t]*\n?)?
        (?P<contents>[\s\S]+?)
        </blockquote>
    ''', re.X
    )

    def test(self, text):
        return "<blockquote>" in text

    def sub(self, match: re.Match) -> str:
        typ = match["type"].lower()
        heading = f"<em>{match['type'].title()}</em>"
        contents = match["contents"].strip()
        if match["closing_tag"]:
            return f'<div class="alert {typ}">\n{heading}\n{contents}\n</div>'
        else:
            return f'<div class="alert {typ}">\n{heading}\n<p>{contents}\n</div>'

    def run(self, text):
        return self.alert_re.sub(self.sub, text)


class _BreaksExtraOpts(TypedDict, total=False):
    '''Options for the `Breaks` extra'''
    on_backslash: bool
    '''Replace backslashes at the end of a line with <br>'''
    on_newline: bool
    '''Replace single new line characters with <br> when True'''


class Breaks(Extra):
    name = 'breaks'
    order = (), (Stage.ITALIC_AND_BOLD,)
    options: _BreaksExtraOpts

    def run(self, text):
        on_backslash = self.options.get('on_backslash', False)
        on_newline = self.options.get('on_newline', False)

        if on_backslash and on_newline:
            pattern = r' *\\?'
        elif on_backslash:
            pattern = r'(?: *\\| {2,})'
        elif on_newline:
            pattern = r' *'
        else:
            pattern = r' {2,}'

        break_tag = "<br%s\n" % self.md.empty_element_suffix
        text = re.sub(pattern + r"\n(?!\<(?:\/?(ul|ol|li))\>)", break_tag, text)

        return text


class CodeFriendly(ItalicAndBoldProcessor):
    '''
    Disable _ and __ for em and strong.
    '''
    name = 'code-friendly'

    def sub(self, match: re.Match) -> str:
        syntax = match.group(1)
        text: str = match.string[match.start(): match.end()]
        if '_' in syntax:
            # if using _this_ syntax, hash the whole thing so that it doesn't get processed
            key = _hash_text(text)
            self.hash_table[key] = text
            return key
        elif '_' in text:
            # if the text within the bold/em markers contains '_' then hash those contents to protect them from em_re
            text = text[len(syntax): -len(syntax)]
            key = _hash_text(text)
            self.hash_table[key] = text
            return syntax + key + syntax
        # if no underscores are present, the text is fine and we can just leave it alone
        return super().sub(match)


class FencedCodeBlocks(Extra):
    '''
    Allows a code block to not have to be indented
    by fencing it with '```' on a line before and after. Based on
    <http://github.github.com/github-flavored-markdown/> with support for
    syntax highlighting.
    '''

    name = 'fenced-code-blocks'
    order = (Stage.LINK_DEFS, Stage.BLOCK_GAMUT), (Stage.PREPROCESS,)

    fenced_code_block_re = re.compile(r'''
        (?:\n+|\A\n?|(?<=\n))
        (^[ \t]*`{3,})\s{0,99}?([\w+-]+)?\s{0,99}?\n  # $1 = opening fence (captured for back-referencing), $2 = optional lang
        (.*?)                             # $3 = code block content
        \1[ \t]*\n                      # closing fence
        ''', re.M | re.X | re.S)

    def test(self, text):
        if '```' not in text:
            return False
        if self.md.stage == Stage.PREPROCESS and not self.md.safe_mode:
            return True
        if self.md.stage == Stage.LINK_DEFS and self.md.safe_mode:
            return True
        return self.md.stage == Stage.BLOCK_GAMUT

    def _code_block_with_lexer_sub(
        self,
        codeblock: str,
        leading_indent: str,
        lexer
    ) -> str:
        '''
        Args:
            codeblock: the codeblock to format
            leading_indent: the indentation to prefix the block with
            lexer (pygments.Lexer): the lexer to use
        '''
        formatter_opts = self.md.extras['fenced-code-blocks'] or {}

        def unhash_code(codeblock):
            for key, sanitized in list(self.md.html_spans.items()):
                codeblock = codeblock.replace(key, sanitized)
            replacements = [
                ("&amp;", "&"),
                ("&lt;", "<"),
                ("&gt;", ">")
            ]
            for old, new in replacements:
                codeblock = codeblock.replace(old, new)
            return codeblock
        # remove leading indent from code block
        _, codeblock = self.md._uniform_outdent(codeblock, max_outdent=leading_indent)

        codeblock = unhash_code(codeblock)
        colored = self.md._color_with_pygments(codeblock, lexer,
                                               **formatter_opts)

        # add back the indent to all lines
        return "\n%s\n" % self.md._uniform_indent(colored, leading_indent, True)

    def tags(self, lexer_name: str) -> tuple[str, str]:
        '''
        Returns the tags that the encoded code block will be wrapped in, based
        upon the lexer name.

        This function can be overridden by subclasses to piggy-back off of the
        fenced code blocks syntax (see `Mermaid` extra).

        Returns:
            The opening and closing tags, as strings within a tuple
        '''
        pre_class = self.md._html_class_str_from_tag('pre')
        if "highlightjs-lang" in self.md.extras and lexer_name:
            code_class = ' class="{} language-{}"'.format(lexer_name, lexer_name)
        else:
            code_class = self.md._html_class_str_from_tag('code')
        return ('<pre{}><code{}>'.format(pre_class, code_class), '</code></pre>')

    def sub(self, match: re.Match) -> str:
        lexer_name = match.group(2)
        codeblock = match.group(3)
        codeblock = codeblock[:-1]  # drop one trailing newline

        # Use pygments only if not using the highlightjs-lang extra
        if lexer_name and "highlightjs-lang" not in self.md.extras:
            lexer = self.md._get_pygments_lexer(lexer_name)
            if lexer:
                leading_indent = ' '*(len(match.group(1)) - len(match.group(1).lstrip()))
                return self._code_block_with_lexer_sub(codeblock, leading_indent, lexer)

        # Fenced code blocks need to be outdented before encoding, and then reapplied
        leading_indent = ' ' * (len(match.group(1)) - len(match.group(1).lstrip()))
        if codeblock:
            # only run the codeblock through the outdenter if not empty
            leading_indent, codeblock = self.md._uniform_outdent(codeblock, max_outdent=leading_indent)

        codeblock = self.md._encode_code(codeblock)

        tags = self.tags(lexer_name)

        return "\n{}{}{}\n{}{}\n".format(leading_indent, tags[0], codeblock, leading_indent, tags[1])

    def run(self, text):
        return self.fenced_code_block_re.sub(self.sub, text)


class Latex(Extra):
    '''
    Convert $ and $$ to <math> and </math> tags for inline and block math.
    '''
    name = 'latex'
    order = (Stage.CODE_BLOCKS, FencedCodeBlocks), ()

    _single_dollar_re = re.compile(r'(?<!\$)\$(?!\$)(.*?)\$')
    _double_dollar_re = re.compile(r'\$\$(.*?)\$\$', re.DOTALL)

    # Ways to escape
    _pre_code_block_re = re.compile(r"<pre>(.*?)</pre>", re.DOTALL) # Wraped in <pre>
    _triple_re = re.compile(r'```(.*?)```', re.DOTALL) # Wrapped in a code block ```
    _single_re = re.compile(r'(?<!`)(`)(.*?)(?<!`)\1(?!`)') # Wrapped in a single `

    converter = None
    code_blocks = {}

    def _convert_single_match(self, match):
        return self.converter.convert(match.group(1))

    def _convert_double_match(self, match):
        return self.converter.convert(match.group(1).replace(r"\n", ''), display="block")

    def code_placeholder(self, match):
        placeholder = f"<!--CODE_BLOCK_{len(self.code_blocks)}-->"
        self.code_blocks[placeholder] = match.group(0)
        return placeholder

    def run(self, text):
        try:
            import latex2mathml.converter
            self.converter = latex2mathml.converter
        except ImportError:
            raise ImportError('The "latex" extra requires the "latex2mathml" package to be installed.')

        # Escape by replacing with a code block
        text = self._pre_code_block_re.sub(self.code_placeholder, text)
        text = self._single_re.sub(self.code_placeholder, text)
        text = self._triple_re.sub(self.code_placeholder, text)

        text = self._single_dollar_re.sub(self._convert_single_match, text)
        text = self._double_dollar_re.sub(self._convert_double_match, text)

        # Convert placeholder tag back to original code
        for placeholder, code_block in self.code_blocks.items():
            text = text.replace(placeholder, code_block)

        return text


class LinkPatterns(Extra):
    '''
    Auto-link given regex patterns in text (e.g. bug number
    references, revision number references).
    '''
    name = 'link-patterns'
    order = (Stage.LINKS,), ()
    options: _link_patterns

    _basic_link_re = re.compile(r'!?\[.*?\]\(.*?\)')

    def run(self, text):
        link_from_hash = {}
        for regex, repl in self.options:
            replacements = []
            for match in regex.finditer(text):
                if any(self.md._match_overlaps_substr(text, match, h) for h in link_from_hash):
                    continue

                if callable(repl):
                    href = repl(match)
                else:
                    href = match.expand(repl)
                replacements.append((match.span(), href))
            for (start, end), href in reversed(replacements):

                # Do not match against links inside brackets.
                if text[start - 1:start] == '[' and text[end:end + 1] == ']':
                    continue

                # Do not match against links in the standard markdown syntax.
                if text[start - 2:start] == '](' or text[end:end + 2] == '")':
                    continue

                # Do not match against links which are escaped.
                if text[start - 3:start] == '"""' and text[end:end + 3] == '"""':
                    text = text[:start - 3] + text[start:end] + text[end + 3:]
                    continue

                # search the text for anything that looks like a link
                is_inside_link = False
                for link_re in (self.md._auto_link_re, self._basic_link_re):
                    for match in link_re.finditer(text):
                        if any((r[0] <= start and end <= r[1]) for r in match.regs):
                            # if the link pattern start and end pos is within the bounds of
                            # something that looks like a link, then don't process it
                            is_inside_link = True
                            break
                    else:
                        continue
                    break

                if is_inside_link:
                    continue

                escaped_href = (
                    href.replace('"', '&quot;')  # b/c of attr quote
                        # To avoid markdown <em> and <strong>:
                        .replace('*', self.md._escape_table['*'])
                        .replace('_', self.md._escape_table['_']))
                link = '<a href="{}">{}</a>'.format(escaped_href, text[start:end])
                hash = _hash_text(link)
                link_from_hash[hash] = link
                text = text[:start] + hash + text[end:]
        for hash, link in list(link_from_hash.items()):
            text = text.replace(hash, link)
        return text

    def test(self, text):
        return True


class MarkdownInHTML(Extra):
    '''
    Allow the use of `markdown="1"` in a block HTML tag to
    have markdown processing be done on its contents. Similar to
    <http://michelf.com/projects/php-markdown/extra/#markdown-attr> but with
    some limitations.
    '''
    name = 'markdown-in-html'
    order = (), (Stage.HASH_HTML,)

    def run(self, text):
        def callback(block):
            indent, block = self.md._uniform_outdent(block)
            block = self.md._hash_html_block_sub(block)
            block = self.md._uniform_indent(block, indent, include_empty_lines=True, indent_empty_lines=False)
            return block

        return self.md._strict_tag_block_sub(text, self.md._block_tags_a, callback, True)

    def test(self, text):
        return True


class _MarkdownFileLinksExtraOpts(_LinkProcessorExtraOpts, total=False):
    '''Options for the `MarkdownFileLinks` extra'''
    link_defs: bool
    '''Whether to convert link definitions as well. Default: True'''


class MarkdownFileLinks(LinkProcessor):
    '''
    Replace links to `.md` files with `.html` links
    '''

    name = 'markdown-file-links'
    order = (Stage.LINKS,), (Stage.LINK_DEFS,)
    options: _MarkdownFileLinksExtraOpts

    def __init__(self, md: Markdown, options: Optional[dict]):
        # override LinkProcessor defaults
        options = {'tags': ['a'], 'ref': False, **(options or {})}
        super().__init__(md, options)

    def parse_inline_anchor_or_image(self, text: str, _link_text: str, start_idx: int):
        result = super().parse_inline_anchor_or_image(text, _link_text, start_idx)
        if not result or not result[1] or not result[1].endswith('.md'):
            # return None for invalid markup, or links that don't end with '.md'
            # so that we don't touch them, and other extras can process them freely
            return
        url = result[1].removesuffix('.md') + '.html'
        return result[0], url, *result[2:]

    def run(self, text: str):
        if Stage.LINKS > self.md.order > Stage.LINK_DEFS and self.options.get('link_defs', True):
            # running just after link defs have been stripped
            for key, url in self.md.urls.items():
                if url.endswith('.md'):
                    self.md.urls[key] = url.removesuffix('.md') + '.html'

        return super().run(text)

    def test(self, text):
        return super().test(text) and '.md' in text


class Mermaid(FencedCodeBlocks):
    name = 'mermaid'
    order = (FencedCodeBlocks,), ()

    def tags(self, lexer_name):
        if lexer_name == 'mermaid':
            return ('<pre class="mermaid-pre"><div class="mermaid">', '</div></pre>')
        return super().tags(lexer_name)


class MiddleWordEm(ItalicAndBoldProcessor):
    '''
    Allows or disallows emphasis syntax in the middle of words,
    defaulting to allow. Disabling this means that `this_text_here` will not be
    converted to `this<em>text</em>here`.
    '''
    name = 'middle-word-em'
    order = (CodeFriendly,), (Stage.ITALIC_AND_BOLD,)

    def __init__(self, md: Markdown, options: Union[dict, bool, None]):
        '''
        Args:
            md: the markdown instance
            options: can be bool for backwards compatibility but will be converted to a dict
                in the constructor. All options are:
                - allowed (bool): whether to allow emphasis in the middle of a word.
                    If `options` is a bool it will be placed under this key.
        '''
        if isinstance(options, bool):
            options = {'allowed': options}
        else:
            options = options or {}
        options.setdefault('allowed', True)
        super().__init__(md, options)

        self.liberal_em_re = self.em_re
        if not options['allowed']:
            self.em_re = re.compile(r'(?<=\b)%s(?=\b)' % self.em_re.pattern, self.em_re.flags)
            self.liberal_em_re = re.compile(
                r'''
                    (                # \1 - must be a single em char in the middle of a word
                        (?<![*_\s])  # cannot be preceeded by em character or whitespace (must be in middle of word)
                        [*_]         # em character
                        (?![*_])     # cannot be followed by another em char
                    )
                    (?=\S)           # em opening must be followed by non-whitespace text
                    (.*?\S)          # the emphasized text
                    \1               # closing char
                    (?!\s|$)         # must not be followed by whitespace (middle of word) or EOF
                '''
                , re.S | re.X)

    def run(self, text):
        if self.options['allowed']:
            # if middle word em is allowed, do nothing. This extra's only use is to prevent them
            return text

        # run strong and whatnot first
        # this also will process all strict ems
        text = super().run(text)
        if self.md.order < self.md.stage:
            # hash all non-valid ems
            text = self.liberal_em_re.sub(self.sub_hash, text)
        return text

    def sub(self, match: re.Match) -> str:
        syntax = match.group(1)
        if len(syntax) != 1:
            # strong syntax
            return super().sub(match)
        return '<em>%s</em>' % match.group(2)


class Numbering(Extra):
    '''
    Support of generic counters.  Non standard extension to
    allow sequential numbering of figures, tables, equations, exhibits etc.
    '''

    name = 'numbering'
    order = (Stage.LINK_DEFS,), ()

    def run(self, text):
        # First pass to define all the references
        regex_defns = re.compile(r'''
            \[\#(\w+) # the counter.  Open square plus hash plus a word \1
            ([^@]*)   # Some optional characters, that aren't an @. \2
            @(\w+)       # the id.  Should this be normed? \3
            ([^\]]*)\]   # The rest of the text up to the terminating ] \4
            ''', re.VERBOSE)
        regex_subs = re.compile(r"\[@(\w+)\s*\]")  # [@ref_id]
        counters = {}
        references = {}
        replacements = []
        definition_html = '<figcaption class="{}" id="counter-ref-{}">{}{}{}</figcaption>'
        reference_html = '<a class="{}" href="#counter-ref-{}">{}</a>'
        for match in regex_defns.finditer(text):
            # We must have four match groups otherwise this isn't a numbering reference
            if len(match.groups()) != 4:
                continue
            counter = match.group(1)
            text_before = match.group(2).strip()
            ref_id = match.group(3)
            text_after = match.group(4)
            number = counters.get(counter, 1)
            references[ref_id] = (number, counter)
            replacements.append((match.start(0),
                                 definition_html.format(counter,
                                                        ref_id,
                                                        text_before,
                                                        number,
                                                        text_after),
                                 match.end(0)))
            counters[counter] = number + 1
        for repl in reversed(replacements):
            text = text[:repl[0]] + repl[1] + text[repl[2]:]

        # Second pass to replace the references with the right
        # value of the counter
        # Fwiw, it's vaguely annoying to have to turn the iterator into
        # a list and then reverse it but I can't think of a better thing to do.
        for match in reversed(list(regex_subs.finditer(text))):
            number, counter = references.get(match.group(1), (None, None))
            if number is not None:
                repl = reference_html.format(counter,
                                             match.group(1),
                                             number)
            else:
                repl = reference_html.format(match.group(1),
                                             'countererror',
                                             '?' + match.group(1) + '?')
            if "smarty-pants" in self.md.extras:
                repl = repl.replace('"', self.md._escape_table['"'])

            text = text[:match.start()] + repl + text[match.end():]
        return text


class PyShell(Extra):
    '''
    Treats unindented Python interactive shell sessions as <code>
    blocks.
    '''

    name = 'pyshell'
    order = (), (Stage.LISTS,)

    def test(self, text):
        return ">>>" in text

    def sub(self, match: re.Match) -> str:
        if "fenced-code-blocks" in self.md.extras:
            dedented = _dedent(match.group(0))
            return self.md.extra_classes['fenced-code-blocks'].run("```pycon\n" + dedented + "```\n")

        lines = match.group(0).splitlines(0)
        _dedentlines(lines)
        indent = ' ' * self.md.tab_width
        s = ('\n'  # separate from possible cuddled paragraph
             + indent + ('\n'+indent).join(lines)
             + '\n')
        return s

    def run(self, text):
        less_than_tab = self.md.tab_width - 1
        _pyshell_block_re = re.compile(r"""
            ^([ ]{0,%d})>>>[ ].*\n  # first line
            ^(\1[^\S\n]*\S.*\n)*    # any number of subsequent lines with at least one character
            (?=^\1?\n|\Z)           # ends with a blank line or end of document
            """ % less_than_tab, re.M | re.X)

        return _pyshell_block_re.sub(self.sub, text)


class SmartyPants(Extra):
    '''
    Replaces ' and " with curly quotation marks or curly
    apostrophes.  Replaces --, ---, ..., and . . . with en dashes, em dashes,
    and ellipses.
    '''
    name = 'smarty-pants'
    order = (), (Stage.SPAN_GAMUT,)

    _opening_single_quote_re = re.compile(r"(?<!\S)'(?=\S)")
    _opening_double_quote_re = re.compile(r'(?<!\S)"(?=\S)')
    _closing_single_quote_re = re.compile(r"(?<=\S)'")
    _closing_double_quote_re = re.compile(r'(?<=\S)"(?=(\s|,|;|\.|\?|!|$))')
    # "smarty-pants" extra: Very liberal in interpreting a single prime as an
    # apostrophe; e.g. ignores the fact that "round", "bout", "twer", and
    # "twixt" can be written without an initial apostrophe. This is fine because
    # using scare quotes (single quotation marks) is rare.
    _apostrophe_year_re = re.compile(r"'(\d\d)(?=(\s|,|;|\.|\?|!|$))")
    _contractions = ["tis", "twas", "twer", "neath", "o", "n",
        "round", "bout", "twixt", "nuff", "fraid", "sup"]


    def contractions(self, text: str) -> str:
        text = self._apostrophe_year_re.sub(r"&#8217;\1", text)
        for c in self._contractions:
            text = text.replace("'%s" % c, "&#8217;%s" % c)
            text = text.replace("'%s" % c.capitalize(),
                "&#8217;%s" % c.capitalize())
        return text

    def run(self, text):
        """Fancifies 'single quotes', "double quotes", and apostrophes.
        Converts --, ---, and ... into en dashes, em dashes, and ellipses.

        Inspiration is: <http://daringfireball.net/projects/smartypants/>
        See "test/tm-cases/smarty_pants.text" for a full discussion of the
        support here and
        <http://code.google.com/p/python-markdown2/issues/detail?id=42> for a
        discussion of some diversion from the original SmartyPants.
        """
        if "'" in text:  # guard for perf
            text = self.contractions(text)
            text = self._opening_single_quote_re.sub("&#8216;", text)
            text = self._closing_single_quote_re.sub("&#8217;", text)

        if '"' in text:  # guard for perf
            text = self._opening_double_quote_re.sub("&#8220;", text)
            text = self._closing_double_quote_re.sub("&#8221;", text)

        text = text.replace("---", "&#8212;")
        text = text.replace("--", "&#8211;")
        text = text.replace("...", "&#8230;")
        text = text.replace(" . . . ", "&#8230;")
        text = text.replace(". . .", "&#8230;")

        # TODO: Temporary hack to fix https://github.com/trentm/python-markdown2/issues/150
        if "footnotes" in self.md.extras and "footnote-ref" in text:
            # Quotes in the footnote back ref get converted to "smart" quotes
            # Change them back here to ensure they work.
            text = text.replace('class="footnote-ref&#8221;', 'class="footnote-ref"')

        return text

    def test(self, text):
        return any(i in text for i in (
            "'",
            '"',
            '--',
            '...',
            '. . .'
        ))


class Strike(Extra):
    '''
    Text inside of double tilde is ~~strikethrough~~
    '''
    name = 'strike'
    order = (Stage.ITALIC_AND_BOLD,), ()

    _strike_re = re.compile(r"~~(?=\S)(.+?)(?<=\S)~~", re.S)

    def run(self, text):
        return self._strike_re.sub(r"<s>\1</s>", text)

    def test(self, text):
        return '~~' in text


class Tables(Extra):
    '''
    Tables using the same format as GFM
    <https://help.github.com/articles/github-flavored-markdown#tables> and
    PHP-Markdown Extra <https://michelf.ca/projects/php-markdown/extra/#table>.
    '''
    name = 'tables'
    order = (), (Stage.LISTS,)

    def run(self, text):
        """Copying PHP-Markdown and GFM table syntax. Some regex borrowed from
        https://github.com/michelf/php-markdown/blob/lib/Michelf/Markdown.php#L2538
        """
        less_than_tab = self.md.tab_width - 1
        table_re = re.compile(r'''
                (?:(?<=\n)|\A\n?)             # leading blank line

                ^[ ]{0,%d}                      # allowed whitespace
                (.*[|].*)[ ]*\n                   # $1: header row (at least one pipe)

                ^[ ]{0,%d}                      # allowed whitespace
                (                               # $2: underline row
                    # underline row with leading bar
                    (?:  \|\ *:?-+:?\ *  )+  \|? \s?[ ]*\n
                    |
                    # or, underline row without leading bar
                    (?:  \ *:?-+:?\ *\|  )+  (?:  \ *:?-+:?\ *  )? \s?[ ]*\n
                )

                (                               # $3: data rows
                    (?:
                        ^[ ]{0,%d}(?!\ )         # ensure line begins with 0 to less_than_tab spaces
                        .*\|.*[ ]*\n
                    )*
                )
            ''' % (less_than_tab, less_than_tab, less_than_tab), re.M | re.X)
        return table_re.sub(self.sub, text)

    def sub(self, match: re.Match) -> str:
        trim_space_re = r'^\s+|\s+$'
        trim_bar_re = r'^\||\|$'
        split_bar_re = r'^\||(?<![\`\\])\|'
        escape_bar_re = r'\\\|'

        head, underline, body = match.groups()

        # Determine aligns for columns.
        cols = [re.sub(escape_bar_re, '|', cell.strip()) for cell in re.split(split_bar_re, re.sub(trim_bar_re, "", re.sub(trim_space_re, "", underline)))]
        align_from_col_idx = {}
        for col_idx, col in enumerate(cols):
            if col[0] == ':' and col[-1] == ':':
                align_from_col_idx[col_idx] = ' style="text-align:center;"'
            elif col[0] == ':':
                align_from_col_idx[col_idx] = ' style="text-align:left;"'
            elif col[-1] == ':':
                align_from_col_idx[col_idx] = ' style="text-align:right;"'

        # thead
        hlines = ['<table%s>' % self.md._html_class_str_from_tag('table'), '<thead%s>' % self.md._html_class_str_from_tag('thead'), '<tr>']
        cols = [re.sub(escape_bar_re, '|', cell.strip()) for cell in re.split(split_bar_re, re.sub(trim_bar_re, "", re.sub(trim_space_re, "", head)))]
        for col_idx, col in enumerate(cols):
            hlines.append('  <th{}>{}</th>'.format(
                align_from_col_idx.get(col_idx, ''),
                self.md._run_span_gamut(col)
            ))
        hlines.append('</tr>')
        hlines.append('</thead>')

        # tbody
        body = body.strip('\n')
        if body:
            hlines.append('<tbody>')
            for line in body.split('\n'):
                hlines.append('<tr>')
                cols = [re.sub(escape_bar_re, '|', cell.strip()) for cell in re.split(split_bar_re, re.sub(trim_bar_re, "", re.sub(trim_space_re, "", line)))]
                for col_idx, col in enumerate(cols):
                    hlines.append('  <td{}>{}</td>'.format(
                        align_from_col_idx.get(col_idx, ''),
                        self.md._run_span_gamut(col)
                    ))
                hlines.append('</tr>')
            hlines.append('</tbody>')
        hlines.append('</table>')

        return '\n'.join(hlines) + '\n'


class TelegramSpoiler(Extra):
    name = 'tg-spoiler'
    order = (), (Stage.ITALIC_AND_BOLD,)

    _tg_spoiler_re = re.compile(r"\|\|\s?(.+?)\s?\|\|", re.S)

    def run(self, text):
        return self._tg_spoiler_re.sub(r"<tg-spoiler>\1</tg-spoiler>", text)

    def test(self, text):
        return '||' in text


class Underline(Extra):
    '''
    Text inside of double dash is --underlined--.
    '''
    name = 'underline'
    order = (Stage.ITALIC_AND_BOLD,), ()

    _underline_re = re.compile(r"(?<!<!)--(?!>)(?=\S)(.+?)(?<=\S)(?<!<!)--(?!>)", re.S)

    def run(self, text):
        return self._underline_re.sub(r"<u>\1</u>", text)

    def test(self, text):
        return '--' in text


class _WavedromExtraOpts(TypedDict, total=False):
    '''Options for the `Wavedrom` extra'''
    prefer_embed_svg: bool
    '''
    Use the `wavedrom` library to convert diagrams to SVGs and embed them directly.
    This will only work if the `wavedrom` library has been installed.

    Defaults to `True`
    '''


class Wavedrom(Extra):
    '''
    Support for generating Wavedrom digital timing diagrams
    '''
    name = 'wavedrom'
    order = (Stage.CODE_BLOCKS, FencedCodeBlocks), ()
    options: _WavedromExtraOpts

    def test(self, text):
        match = FencedCodeBlocks.fenced_code_block_re.search(text)
        return match is None or match.group(2) == 'wavedrom'

    def sub(self, match: re.Match) -> str:
        # dedent the block for processing
        lead_indent, waves = self.md._uniform_outdent(match.group(3))
        # default tags to wrap the wavedrom block in
        open_tag, close_tag = '<script type="WaveDrom">\n', '</script>'

        # check if the user would prefer to have the SVG embedded directly
        embed_svg = self.options.get('prefer_embed_svg', True)

        if embed_svg:
            try:
                import wavedrom
                waves = wavedrom.render(waves).tostring()
                open_tag, close_tag = '<div>', '\n</div>'
            except ImportError:
                pass

        # hash SVG to prevent <> chars being messed with
        self.md._escape_table[waves] = _hash_text(waves)

        return self.md._uniform_indent(
            '\n{}{}{}\n'.format(open_tag, self.md._escape_table[waves], close_tag),
            lead_indent, include_empty_lines=True
        )

    def run(self, text):
        return FencedCodeBlocks.fenced_code_block_re.sub(self.sub, text)


class WikiTables(Extra):
    '''
    Google Code Wiki-style tables. See
    <http://code.google.com/p/support/wiki/WikiSyntax#Tables>.
    '''
    name = 'wiki-tables'
    order = (Tables,), ()

    def run(self, text):
        less_than_tab = self.md.tab_width - 1
        wiki_table_re = re.compile(r'''
            (?:(?<=\n\n)|\A\n?)            # leading blank line
            ^([ ]{0,%d})\|\|.+?\|\|[ ]*\n  # first line
            (^\1\|\|.+?\|\|\n)*        # any number of subsequent lines
            ''' % less_than_tab, re.M | re.X)
        return wiki_table_re.sub(self.sub, text)

    def sub(self, match: re.Match) -> str:
        ttext = match.group(0).strip()
        rows = []
        for line in ttext.splitlines(0):
            line = line.strip()[2:-2].strip()
            row = [c.strip() for c in re.split(r'(?<!\\)\|\|', line)]
            rows.append(row)

        hlines = []

        def add_hline(line, indents=0):
            hlines.append((self.md.tab * indents) + line)

        def format_cell(text):
            return self.md._run_span_gamut(re.sub(r"^\s*~", "", cell).strip(" "))

        add_hline('<table%s>' % self.md._html_class_str_from_tag('table'))
        # Check if first cell of first row is a header cell. If so, assume the whole row is a header row.
        if rows and rows[0] and re.match(r"^\s*~", rows[0][0]):
            add_hline('<thead%s>' % self.md._html_class_str_from_tag('thead'), 1)
            add_hline('<tr>', 2)
            for cell in rows[0]:
                add_hline(f"<th>{format_cell(cell)}</th>", 3)
            add_hline('</tr>', 2)
            add_hline('</thead>', 1)
            # Only one header row allowed.
            rows = rows[1:]
        # If no more rows, don't create a tbody.
        if rows:
            add_hline('<tbody>', 1)
            for row in rows:
                add_hline('<tr>', 2)
                for cell in row:
                    add_hline(f'<td>{format_cell(cell)}</td>', 3)
                add_hline('</tr>', 2)
            add_hline('</tbody>', 1)
        add_hline('</table>')
        return '\n'.join(hlines) + '\n'

    def test(self, text):
        return '||' in text


# Register extras
Admonitions.register()
Alerts.register()
Breaks.register()
CodeFriendly.register()
FencedCodeBlocks.register()
Latex.register()
LinkPatterns.register()
MarkdownInHTML.register()
MarkdownFileLinks.register()
MiddleWordEm.register()
Mermaid.register()
Numbering.register()
PyShell.register()
SmartyPants.register()
Strike.register()
Tables.register()
TelegramSpoiler.register()
Underline.register()
Wavedrom.register()
WikiTables.register()


# ----------------------------------------------------------


# ---- internal support functions


def calculate_toc_html(toc: Union[list[tuple[int, str, str]], None]) -> Optional[str]:
    """Return the HTML for the current TOC.

    This expects the `_toc` attribute to have been set on this instance.
    """
    if toc is None:
        return None

    def indent():
        return '  ' * (len(h_stack) - 1)
    lines = []
    h_stack = [0]   # stack of header-level numbers
    for level, id, name in toc:
        if level > h_stack[-1]:
            lines.append("%s<ul>" % indent())
            h_stack.append(level)
        elif level == h_stack[-1]:
            lines[-1] += "</li>"
        else:
            while level < h_stack[-1]:
                h_stack.pop()
                if not lines[-1].endswith("</li>"):
                    lines[-1] += "</li>"
                lines.append("%s</ul></li>" % indent())
        lines.append('{}<li><a href="#{}">{}</a>'.format(
            indent(), id, name))
    while len(h_stack) > 1:
        h_stack.pop()
        if not lines[-1].endswith("</li>"):
            lines[-1] += "</li>"
        lines.append("%s</ul>" % indent())
    return '\n'.join(lines) + '\n'


class UnicodeWithAttrs(str):
    """A subclass of unicode used for the return value of conversion to
    possibly attach some attributes. E.g. the "toc_html" attribute when
    the "toc" extra is used.
    """
    metadata: Optional[dict[str, str]] = None
    toc_html: Optional[str] = None

## {{{ http://code.activestate.com/recipes/577257/ (r1)
_slugify_strip_re = re.compile(r'[^\w\s-]')
_slugify_hyphenate_re = re.compile(r'[-\s]+')
def _slugify(value: str) -> str:
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.

    From Django's "django/template/defaultfilters.py".
    """
    import unicodedata
    value = unicodedata.normalize('NFKD', value).encode('utf-8', 'ignore').decode()
    value = _slugify_strip_re.sub('', value).strip().lower()
    return _slugify_hyphenate_re.sub('-', value)
## end of http://code.activestate.com/recipes/577257/ }}}


# From http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52549
def _curry(function: Callable, *args, **kwargs) -> Callable:
    def result(*rest, **kwrest):
        combined = kwargs.copy()
        combined.update(kwrest)
        return function(*args + rest, **combined)
    return result


# Recipe: regex_from_encoded_pattern (1.0)
def _regex_from_encoded_pattern(s: str) -> re.Pattern:
    """'foo'    -> re.compile(re.escape('foo'))
       '/foo/'  -> re.compile('foo')
       '/foo/i' -> re.compile('foo', re.I)
    """
    if s.startswith('/') and s.rfind('/') != 0:
        # Parse it: /PATTERN/FLAGS
        idx = s.rfind('/')
        _, flags_str = s[1:idx], s[idx+1:]
        flag_from_char = {
            "i": re.IGNORECASE,
            "l": re.LOCALE,
            "s": re.DOTALL,
            "m": re.MULTILINE,
            "u": re.UNICODE,
        }
        flags = 0
        for char in flags_str:
            try:
                flags |= flag_from_char[char]
            except KeyError:
                raise ValueError("unsupported regex flag: '%s' in '%s' "
                                 "(must be one of '%s')"
                                 % (char, s, ''.join(list(flag_from_char.keys()))))
        return re.compile(s[1:idx], flags)
    else:  # not an encoded regex
        return re.compile(re.escape(s))


# Recipe: dedent (0.1.2)
def _dedentlines(lines: list[str], tabsize: int = 8, skip_first_line: bool = False) -> list[str]:
    """_dedentlines(lines, tabsize=8, skip_first_line=False) -> dedented lines

        "lines" is a list of lines to dedent.
        "tabsize" is the tab width to use for indent width calculations.
        "skip_first_line" is a boolean indicating if the first line should
            be skipped for calculating the indent width and for dedenting.
            This is sometimes useful for docstrings and similar.

    Same as dedent() except operates on a sequence of lines. Note: the
    lines list is modified **in-place**.
    """
    DEBUG = False
    if DEBUG:
        print("dedent: dedent(..., tabsize=%d, skip_first_line=%r)"\
              % (tabsize, skip_first_line))
    margin = None
    for i, line in enumerate(lines):
        if i == 0 and skip_first_line:
            continue
        indent = 0
        for ch in line:
            if ch == ' ':
                indent += 1
            elif ch == '\t':
                indent += tabsize - (indent % tabsize)
            elif ch in '\r\n':
                continue  # skip all-whitespace lines
            else:
                break
        else:
            continue  # skip all-whitespace lines
        if DEBUG:
            print("dedent: indent=%d: %r" % (indent, line))
        if margin is None:
            margin = indent
        else:
            margin = min(margin, indent)
    if DEBUG:
        print("dedent: margin=%r" % margin)

    if margin is not None and margin > 0:
        for i, line in enumerate(lines):
            if i == 0 and skip_first_line:
                continue
            removed = 0
            for j, ch in enumerate(line):
                if ch == ' ':
                    removed += 1
                elif ch == '\t':
                    removed += tabsize - (removed % tabsize)
                elif ch in '\r\n':
                    if DEBUG:
                        print("dedent: %r: EOL -> strip up to EOL" % line)
                    lines[i] = lines[i][j:]
                    break
                else:
                    raise ValueError("unexpected non-whitespace char %r in "
                                     "line %r while removing %d-space margin"
                                     % (ch, line, margin))
                if DEBUG:
                    print("dedent: %r: %r -> removed %d/%d"\
                          % (line, ch, removed, margin))
                if removed == margin:
                    lines[i] = lines[i][j+1:]
                    break
                elif removed > margin:
                    lines[i] = ' '*(removed-margin) + lines[i][j+1:]
                    break
            else:
                if removed:
                    lines[i] = lines[i][removed:]
    return lines


def _dedent(text: str, tabsize: int = 8, skip_first_line: bool = False) -> str:
    """_dedent(text, tabsize=8, skip_first_line=False) -> dedented text

        "text" is the text to dedent.
        "tabsize" is the tab width to use for indent width calculations.
        "skip_first_line" is a boolean indicating if the first line should
            be skipped for calculating the indent width and for dedenting.
            This is sometimes useful for docstrings and similar.

    textwrap.dedent(s), but don't expand tabs to spaces
    """
    lines = text.splitlines(True)
    _dedentlines(lines, tabsize=tabsize, skip_first_line=skip_first_line)
    return ''.join(lines)


class _memoized:
    """Decorator that caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned, and
    not re-evaluated.

    http://wiki.python.org/moin/PythonDecoratorLibrary
    """
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        try:
            return self.cache[args]
        except KeyError:
            self.cache[args] = value = self.func(*args)
            return value
        except TypeError:
            # uncachable -- for instance, passing a list as an argument.
            # Better to not cache than to blow up entirely.
            return self.func(*args)

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__


def _xml_oneliner_re_from_tab_width(tab_width: int) -> re.Pattern:
    """Standalone XML processing instruction regex."""
    return re.compile(r"""
        (?:
            (?<=\n\n)       # Starting after a blank line
            |               # or
            \A\n?           # the beginning of the doc
        )
        (                           # save in $1
            [ ]{0,%d}
            (?:
                <\?\w+\b\s+.*?\?>   # XML processing instruction
                |
                <\w+:\w+\b\s+.*?/>  # namespaced single tag
            )
            [ \t]*
            (?=\n{2,}|\Z)       # followed by a blank line or end of document
        )
        """ % (tab_width - 1), re.X)
_xml_oneliner_re_from_tab_width = _memoized(_xml_oneliner_re_from_tab_width)


def _hr_tag_re_from_tab_width(tab_width: int) -> re.Pattern:
    return re.compile(r"""
        (?:
            (?<=\n\n)       # Starting after a blank line
            |               # or
            \A\n?           # the beginning of the doc
        )
        (                       # save in \1
            [ ]{0,%d}
            <(hr)               # start tag = \2
            \b                  # word break
            ([^<>])*?           #
            /?>                 # the matching end tag
            [ \t]*
            (?=\n{2,}|\Z)       # followed by a blank line or end of document
        )
        """ % (tab_width - 1), re.X)
_hr_tag_re_from_tab_width = _memoized(_hr_tag_re_from_tab_width)


def _xml_escape_attr(attr: str, skip_single_quote: bool = True) -> str:
    """Escape the given string for use in an HTML/XML tag attribute.

    By default this doesn't bother with escaping `'` to `&#39;`, presuming that
    the tag attribute is surrounded by double quotes.
    """
    escaped = _AMPERSAND_RE.sub('&amp;', attr)

    escaped = (attr
        .replace('"', '&quot;')
        .replace('<', '&lt;')
        .replace('>', '&gt;'))
    if not skip_single_quote:
        escaped = escaped.replace("'", "&#39;")
    return escaped


def _xml_encode_email_char_at_random(ch: str) -> str:
    r = random()
    # Roughly 10% raw, 45% hex, 45% dec.
    # '@' *must* be encoded. I [John Gruber] insist.
    # Issue 26: '_' must be encoded.
    if r > 0.9 and ch not in "@_":
        return ch
    elif r < 0.45:
        # The [1:] is to drop leading '0': 0x63 -> x63
        return '&#%s;' % hex(ord(ch))[1:]
    else:
        return '&#%s;' % ord(ch)


def _html_escape_url(
    attr: str,
    safe_mode: Union[_safe_mode, bool, None] = False,
    charset: Optional[str] = None
):
    """
    Replace special characters that are potentially malicious in url string.

    Args:
        charset: don't escape characters from this charset. Currently the only
            exception is for '+' when charset=='base64'
    """
    escaped = (attr
        .replace('"', '&quot;')
        .replace('<', '&lt;')
        .replace('>', '&gt;'))
    if safe_mode:
        escaped = escaped.replace("'", "&#39;")
    return escaped


# ---- mainline

class _NoReflowFormatter(argparse.RawDescriptionHelpFormatter):
    """An argparse formatter that does NOT reflow the description."""
    def format_description(self, description):
        return description or ""


def _test():
    import doctest
    doctest.testmod()


def main(argv=None):
    if argv is None:
        argv = sys.argv
    if not logging.root.handlers:
        logging.basicConfig()

    parser = argparse.ArgumentParser(
        prog="markdown2", description=cmdln_desc, usage='%(prog)s [PATHS...]',
        formatter_class=_NoReflowFormatter
    )
    parser.add_argument('--version', action='version',
                        version=f'%(prog)s {__version__}')
    parser.add_argument('paths', nargs='*',
                        help=(
                            'optional list of files to convert.'
                            'If none are given, stdin will be used'
                        ))
    parser.add_argument("-v", "--verbose", dest="log_level",
                      action="store_const", const=logging.DEBUG,
                      help="more verbose output")
    parser.add_argument("--encoding",
                      help="specify encoding of text content")
    parser.add_argument("--html4tags", action="store_true", default=False,
                      help="use HTML 4 style for empty element tags")
    parser.add_argument("-s", "--safe", metavar="MODE", dest="safe_mode",
                      help="sanitize literal HTML: 'escape' escapes "
                           "HTML meta chars, 'replace' replaces with an "
                           "[HTML_REMOVED] note")
    parser.add_argument("-x", "--extras", action="append",
                      help="Turn on specific extra features (not part of "
                           "the core Markdown spec). See above.")
    parser.add_argument("--use-file-vars",
                      help="Look for and use Emacs-style 'markdown-extras' "
                           "file var to turn on extras. See "
                           "<https://github.com/trentm/python-markdown2/wiki/Extras>")
    parser.add_argument("--link-patterns-file",
                      help="path to a link pattern file")
    parser.add_argument("--self-test", action="store_true",
                      help="run internal self-tests (some doctests)")
    parser.add_argument("--compare", action="store_true",
                      help="run against Markdown.pl as well (for testing)")
    parser.add_argument('--output', type=str, help='output to a file instead of stdout')
    parser.set_defaults(log_level=logging.INFO, compare=False,
                        encoding="utf-8", safe_mode=None, use_file_vars=False)
    opts = parser.parse_args()
    paths = opts.paths
    log.setLevel(opts.log_level)

    if opts.self_test:
        return _test()

    if opts.extras:
        extras = {}
        for s in opts.extras:
            splitter = re.compile("[,;: ]+")
            for e in splitter.split(s):
                if '=' in e:
                    ename, earg = e.split('=', 1)
                    try:
                        earg = int(earg)
                    except ValueError:
                        pass
                else:
                    ename, earg = e, None
                extras[ename] = earg
    else:
        extras = None

    if opts.link_patterns_file:
        link_patterns = []
        f = open(opts.link_patterns_file)
        try:
            for i, line in enumerate(f.readlines()):
                if not line.strip():
                    continue
                if line.lstrip().startswith("#"):
                    continue
                try:
                    pat, href = line.rstrip().rsplit(None, 1)
                except ValueError:
                    raise MarkdownError("%s:%d: invalid link pattern line: %r"
                                        % (opts.link_patterns_file, i+1, line))
                link_patterns.append(
                    (_regex_from_encoded_pattern(pat), href))
        finally:
            f.close()
    else:
        link_patterns = None

    from os.path import abspath, dirname, exists, join
    markdown_pl = join(dirname(dirname(abspath(__file__))), "test",
                       "Markdown.pl")
    if not paths:
        paths = ['-']
    for path in paths:
        if path == '-':
            text = sys.stdin.read()
        else:
            fp = codecs.open(path, 'r', opts.encoding)
            text = fp.read()
            fp.close()
        if opts.compare:
            from subprocess import PIPE, Popen
            print("==== Markdown.pl ====")
            p = Popen('perl %s' % markdown_pl, shell=True, stdin=PIPE, stdout=PIPE, close_fds=True)
            p.stdin.write(text.encode('utf-8'))
            p.stdin.close()
            perl_html = p.stdout.read().decode('utf-8')
            sys.stdout.write(perl_html)
            print("==== markdown2.py ====")
        html = markdown(text,
            html4tags=opts.html4tags,
            safe_mode=opts.safe_mode,
            extras=extras, link_patterns=link_patterns,
            use_file_vars=opts.use_file_vars,
            cli=True)
        if opts.output:
            with open(opts.output, 'w') as f:
                f.write(html)
        else:
            sys.stdout.write(html)
        if extras and "toc" in extras:
            log.debug("toc_html: " +
                str(html.toc_html.encode(sys.stdout.encoding or "utf-8", 'xmlcharrefreplace')))
        if opts.compare:
            test_dir = join(dirname(dirname(abspath(__file__))), "test")
            if exists(join(test_dir, "test_markdown2.py")):
                sys.path.insert(0, test_dir)
                from test_markdown2 import norm_html_from_html
                norm_html = norm_html_from_html(html)
                norm_perl_html = norm_html_from_html(perl_html)
            else:
                norm_html = html
                norm_perl_html = perl_html
            print("==== match? %r ====" % (norm_perl_html == norm_html))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
