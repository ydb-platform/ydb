# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals, print_function, division

import sys
import logging
import time
import codecs
import operator

import pymorphy2
from pymorphy2.cache import lru_cache, memoized_with_single_argument
from pymorphy2.utils import get_mem_usage
from pymorphy2.tokenizers import simple_word_tokenize

PY2 = sys.version_info[0] == 2

# Hacks are here to make docstring compatible with both
# docopt and sphinx.ext.autodoc.
head = """

Pymorphy2 is a morphological analyzer / inflection engine for Russian language.
"""
__doc__ = """
Usage::

    pymorphy parse [options] [<input>]
    pymorphy dict meta [--lang <lang> | --dict <path>]
    pymorphy dict mem_usage [--lang <lang> | --dict <path>] [--verbose]
    pymorphy -h | --help
    pymorphy --version

Options::

    -l --lemmatize      Include normal forms (lemmas)
    -s --score          Include non-contextual P(tag|word) scores
    -t --tag            Include tags
    --thresh <NUM>      Drop all results with estimated P(tag|word) less
                        than a threshold [default: 0.0]
    --tokenized         Assume that input text is already tokenized:
                        one token per line.
    -c --cache <SIZE>   Cache size, in entries. Set it to 0 to disable
                        cache; use 'unlim' value for unlimited cache
                        size [default: 20000]
    --lang <lang>       Language to use. Allowed values: ru, uk [default: ru]
    --dict <path>       Dictionary folder path
    -v --verbose        Be more verbose
    -h --help           Show this help

"""
DOC = head + __doc__.replace('::\n', ':')

# TODO:
#   -i --inline         Don't start each output result with a new line
#   --format <format>   Result format. Allowed values: text, json [default: text]
#   --nonlex            Parse non-lexical tokens


logger = logging.getLogger('pymorphy2')


# ============================== Entry point ============================
def main(argv=None):
    """
    Pymorphy CLI interface dispatcher.
    """

    from docopt import docopt
    args = docopt(DOC, argv, version=pymorphy2.__version__)

    path = args['--dict']
    lang = args['--lang']

    if args['parse']:
        morph = pymorphy2.MorphAnalyzer(path=path, lang=lang)
        in_file = _open_for_read(args['<input>'])

        if any([args['--score'], args['--lemmatize'], args['--tag']]):
            score, lemmatize, tag = args['--score'], args['--lemmatize'], args['--tag']
        else:
            score, lemmatize, tag = True, True, True

        if PY2:
            out_file = codecs.getwriter('utf8')(sys.stdout)
        else:
            out_file = sys.stdout

        return parse(
            morph=morph,
            in_file=in_file,
            out_file=out_file,
            tokenize=not args['--tokenized'],
            score=score,
            normal_form=lemmatize,
            tag=tag,
            newlines=True,  # not args['--inline'],
            cache_size=args['--cache'],
            thresh=float(args['--thresh']),
        )

    if args['dict']:
        logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.DEBUG if args['--verbose'] else logging.INFO)
        logger.debug(args)

        if args['mem_usage']:
            return show_dict_mem_usage(lang, path, args['--verbose'])
        elif args['meta']:
            return show_dict_meta(lang, path)


def _open_for_read(fn):
    """ Open a file for reading """
    if fn in ['-', '', None]:
        if PY2:
            return codecs.getreader('utf8')(sys.stdin)
        else:
            return sys.stdin
    if PY2:
        return codecs.open(fn, 'rt', encoding='utf8')
    else:
        return open(fn, 'rt', encoding='utf8')


# ============================ Commands ===========================

def show_dict_mem_usage(lang, dict_path=None, verbose=False):
    """
    Show dictionary memory usage.
    """
    initial_mem = get_mem_usage()
    initial_time = time.time()

    morph = pymorphy2.MorphAnalyzer(path=dict_path, lang=lang)

    end_time = time.time()
    mem_usage = get_mem_usage()

    logger.info(
        'Memory usage: %0.1fM dictionary, %0.1fM total (load time %0.2fs)',
        (mem_usage-initial_mem)/(1024*1024), mem_usage/(1024*1024), end_time-initial_time
    )


def show_dict_meta(lang, dict_path=None):
    morph = pymorphy2.MorphAnalyzer(path=dict_path, lang=lang)

    for key, value in morph.dictionary.meta.items():
        logger.info("%s: %s", key, value)


def parse(morph, in_file, out_file, tokenize, score, normal_form, tag,
          newlines, cache_size, thresh):
    """
    Parse text from in_file; write output to out_file.
    Both ``in_file`` and ``out_file`` must support unicode.

    * If `tokenize` is False assume text is already tokenized - a token per
    new line.
    * If `score` is True, include score in the output.
    * If `normal_form` is True, include normal form in the output.
    * If `tag` is True, include tags in the output.
    * If `newline` is True, write each result on a new line.
    * `cache_size` is a maximum number of entries in internal cache.
    * `thresh` is a minimum allowed parse score

    """
    iter_tokens = _iter_tokens_tokenize if tokenize else _iter_tokens_notokenize

    parser = _TokenParserFormatter(
        morph=morph,
        score=score,
        normal_form=normal_form,
        tag=tag,
        newlines=newlines,
        thresh=thresh,
    )

    _parse = parser.parse
    if cache_size == 'unlim':
        _parse = memoized_with_single_argument({})(_parse)
    else:
        cache_size = int(cache_size)
        if cache_size:
            _parse = lru_cache(cache_size)(_parse)
    _write = out_file.write

    for token in iter_tokens(in_file):
        _write(_parse(token))


class _TokenParserFormatter(object):
    """
    This class defines its `parse` method based on arguments passed.
    Some ugly code is to make all ifs work only once, not for each token.
    """

    tpl_newline = "%s{%s}\n"
    tpl_no_newline = "%s{%s} "
    or_sep = "|"

    def __init__(self, morph, score, normal_form, tag, newlines, thresh):
        tpl = self.tpl_newline if newlines else self.tpl_no_newline
        morph_tag = morph.tag
        morph_parse = morph.parse
        join = self.or_sep.join

        if not normal_form and not tag:
            raise ValueError("Empty output is requested")

        if not normal_form and not score and not thresh:
            # morph.tag method is enough
            self.parse = lambda tok: tpl % (tok, join(str(t) for t in morph_tag(tok)))
            return

        if normal_form:
            if tag:
                if score:
                    def _parse_token(tok):
                        seq = [
                            "%s:%0.3f=%s" % (p.normal_form, p.score, p.tag)
                            for p in morph_parse(tok) if p.score >= thresh
                        ]
                        return tpl % (tok, join(seq))
                else:
                    def _parse_token(tok):
                        seq = [
                            "%s:%s" % (p.normal_form, p.tag)
                            for p in morph_parse(tok) if p.score >= thresh
                        ]
                        return tpl % (tok, join(seq))
            else:
                val = operator.itemgetter(1)
                def _parse_token(tok):
                    lemmas = {}
                    for p in morph_parse(tok):
                        lemmas[p.normal_form] = lemmas.get(p.normal_form, 0) + p.score

                    items = sorted(
                        [(lemma, w) for (lemma, w) in lemmas.items() if w >= thresh],
                        key=val, reverse=True
                    )
                    if score:
                        seq = ["%s:%0.3f" % (lemma, w) for (lemma, w) in items]
                    else:
                        seq = [lemma for (lemma, w) in items]

                    return tpl % (tok, join(seq))
        else:
            if score:
                def _parse_token(tok):
                    seq = [
                        "%0.3f=%s" % (p.score, p.tag)
                        for p in morph_parse(tok) if p.score >= thresh
                    ]
                    return tpl % (tok, join(seq))
            else:
                def _parse_token(tok):
                    seq = [
                        "%s" % p.tag
                        for p in morph_parse(tok) if p.score >= thresh
                    ]
                    return tpl % (tok, join(seq))

        self.parse = _parse_token


def _iter_tokens_tokenize(fp):
    """ Return an iterator of input tokens; each line is tokenized """
    return (token for line in fp for token in simple_word_tokenize(line))


def _iter_tokens_notokenize(fp):
    """ Return an iterator of input tokens; each line is a single token """
    return (line for line in (line.strip() for line in fp) if line)
