"""
Conveniently get data from text
"""

import os
import re
from itertools import groupby
import sys
import warnings

from .util import noquotes, ensure_text, CSTRIP, _PY2


if not _PY2:
    basestring = str


__all__ = 'lines text textlines textline words paras'.split()


def lines(source, noblanks=True, dedent=True, lstrip=False, rstrip=True,
          expandtabs=False, cstrip=True, join=False):
    """
    Grab lines from a string. Discard initial and final lines if blank.

    :param str|lines source:  Text (or list of text lines) to be processed
    :param bool dedent:   a common prefix should be stripped from each line (default `True`)
    :param bool noblanks: allow no blank lines at all (default `True`)
    :param bool lstrip:   all left space be stripped from each line (default `False`);
                     dedent and lstrip are mutually exclusive
    :param bool rstrip:   all right space be stripped from each line (default `True`)
    :param Union[bool,int] expandtabs: should all tabs be expanded? if int, by how much?
    :param bool cstrip:   strips comment strings from # to end of each line (like Python itself)
    :param bool|str join:     if False, no effect; otherwise a string used to join the lines
    :return: a list of strings
    :rtype: list
    """

    text = ensure_text(source)

    if cstrip:
        text = CSTRIP.sub('', text)

    if expandtabs:
        text = text.expandtabs() if expandtabs is False else text.expandtabs(expandtabs)

    textlines = text.splitlines()

    # remove blank lines if noblanks
    if noblanks:
        textlines = [line for line in textlines if line.strip() != '']
    else:
        # even if intermediate blank lines ok, first and last are due to Python
        # formatting
        if textlines and textlines[0].strip() == "":
            textlines.pop(0)
        if textlines and textlines[-1].strip() == "":
            textlines.pop()

        # TODO: decided if these should be while loops, eating all prefix/suffix blank lines

    if dedent and not lstrip:
        if expandtabs:
            nonblanklines = [line for line in textlines if line.strip() != ""]
        else:
            # if not expanding all tabs, expand tabs at least for purpose of finding common prefix
            nonblanklines = [line.expandtabs() for line in textlines if line.strip() != ""]
        prefix = os.path.commonprefix(nonblanklines)
        prelen, maxprelen = 0, len(prefix)
        while prelen < maxprelen and prefix[prelen] == ' ':
            prelen += 1
        if prelen:
            textlines = [line[prelen:] for line in textlines]

    # perform requested left and right space stripping (must be done
    # late so as to not interfere with dedent's common prefix detection)
    if lstrip and rstrip:
        textlines = [line.strip() for line in textlines]
    elif lstrip:
        textlines = [line.lstrip() for line in textlines]
    elif rstrip:
        textlines = [line.rstrip() for line in textlines]

    if join is False:
        return textlines
    else:
        join = '' if join is True else join
        return join.join(textlines)


def text(source, **kwargs):
    """
    Like ``lines()``, but returns result as unified text. Useful primarily
    because of the nice cleanups ``lines()`` does.

    :param str|lines source:  Text (or list of text lines) to be processed
    :param str join: String to join lines with. Typically newline for line-oriented
        text but change to " " for a single continous line.
    :return: the cleaned string
    :rtype: str
    """
    kwargs.setdefault('join', '\n')
    return lines(source, **kwargs)


def textlines(*args, **kwargs):
    """
    Deprecated alias for ``test``. Use it instead.
    """
    warnings.warn('Depreacted alias for text(). Use it instead.', DeprecationWarning)
    return text(*args, **kwargs)


def textline(source, cstrip=True):
    """
    Like ``text()``, but returns result as unified string that is not
    line-oriented. Really a special case of ``text()``

    :param str|list source:
    :param bool cstrip: Should comments be stripped? (default: ``True``)
    :return: the cleaned string
    :rtype: str
    """
    pars = paras(source, keep_blanks=False, join=" ", cstrip=cstrip)
    return "\n\n".join(pars)


# define word regular expression and pre-define quotes
WORDRE = re.compile(r"""\s*(?P<word>"[^"]*"|'[^']*'|\S+)\s*""")


def words(source, cstrip=True, sep=None):
    """
    Returns a sequence of words, like qw() in Perl. Similar to s.split(),
    except that it respects quoted spans for the occasional word (really,
    phrase) with spaces included.) If the ``sep`` argument is provided,
    words are split on that boundary (rather like ``str.split()``). Either 
    the standard space and possibly-quoted word behavior should be used,
    or the explicit separator. They don't cooperate well.
    
    Like ``lines``, removes comment strings by
    default.


    :param str|list source: Text (or list of text lines) to gather words from
    :param bool cstrip: Should comments be stripped? (default: ``True``)
    :param Optional[str] sep: Optional explicit separator.
    :return: list of words/phrases
    :rtype: list
    """

    text = ensure_text(source)

    if cstrip:
        text = CSTRIP.sub('', text)

    if sep is None:
        text = text.strip()
        parts = re.findall(WORDRE, text)
        return [noquotes(p) for p in parts]
    else:
        parts = text.split(sep)
        return [p.strip() for p in parts]


def paras(source, keep_blanks=False, join=False, cstrip=True):
    """
    Given a string or list of text lines, return a list of lists where each
    sub list is a paragraph (list of non-blank lines). If the source is a
    string, use ``lines`` to split into lines. Optionally can also keep the
    runs of blanks, and/or join the lines in each paragraph with a desired
    separator (likely a newline if you want to preserve multi-line structure
    in the resulting string, or " " if you don't).  Like ``words``,
    ``lines``, and ``textlines``, will also strip comments by default.

    :param str|list source: Text (or list of text lines) from which paras are to be gathered
    :param keep_blanks: Should internal blank lines be retained (default: ``False``)
    :param bool|str join: Should paras be joined into a string? (default: ``False``).
    :param bool cstrip: Should comments be stripped? (default: ``True``)
    :return: list of strings (each a paragraph)
    :rtype: list
    """

    # make sure we have lines, with suitable cleanups
    # note that lines() will guarantee ensure_text()
    sourcelines = lines(source, noblanks=False, cstrip=cstrip)

    # get paragraphs
    results = []
    line_not_blank = lambda l: l.strip() != ""
    for non_blank, run in groupby(sourcelines, line_not_blank):
        if non_blank or keep_blanks:
            run_list = list(run)
            payload = join.join(run_list) if join is not False else run_list
            results.append(payload)
    return results
