import logging
import operator
import sys
import time
from functools import lru_cache

try:
    import click
except ModuleNotFoundError as e:
    raise ModuleNotFoundError('''pymorphy3's command line tools require Click which is not currently installed. Try installing Click via "pip install click".''') from e

import pymorphy3
from pymorphy3.tokenizers import simple_word_tokenize
from pymorphy3.utils import get_mem_usage

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

# TODO:
#   -i --inline         Don't start each output result with a new line
#   --format <format>   Result format. Allowed values: text, json [default: text]
#   --nonlex            Parse non-lexical tokens


logger = logging.getLogger('pymorphy3')


# ============================== Entry point ============================
@click.group()
@click.help_option('-h', '--help')
@click.version_option(version=pymorphy3.__version__, message='%(version)s')
def main(argv=None):
    """
    pymorphy3 is a morphological analyzer / inflection engine for Russian language.
    """

@main.command(name='parse', context_settings={'show_default': True})
@click.option('--dict', 'path', help='Dictionary folder path')
@click.option('--lang', default='ru', help='Language to use. Allowed values: ru, uk')
@click.option('-l', '--lemmatize', is_flag=True, help='Include normal forms (lemmas)')
@click.option('-s', '--score', is_flag=True, help='Include non-contextual P(tag|word) scores')
@click.option('-t', '--tag', is_flag=True, help='Include tags')
@click.option('--tokenized', is_flag=True, help='Assume that input text is already tokenized: one token per line.')
@click.option('-c', '--cache', default=20000, help="Cache size, in entries. Set it to 0 to disable cache; use 'unlim' value for unlimited cache size")
@click.option('--thresh', default=0.0, help='Drop all results with estimated P(tag|word) less than a threshold')
@click.argument('input_', metavar='INPUT', required=False)
def cli_parse(path, lang, score, lemmatize, tag, tokenized, cache, thresh, input_):
    morph = pymorphy3.MorphAnalyzer(path=path, lang=lang)
    in_file = _open_for_read(input_)

    if not any([score, lemmatize, tag]):
        score, lemmatize, tag = True, True, True

    out_file = sys.stdout

    return parse(
        morph=morph,
        in_file=in_file,
        out_file=out_file,
        tokenize=not tokenized,
        score=score,
        normal_form=lemmatize,
        tag=tag,
        newlines=True,  # not args['--inline'],
        cache_size=cache,
        thresh=thresh,
    )
@main.group(name='dict')
def cli_dict():
    pass

@cli_dict.command(name='mem_usage', context_settings={'show_default': True})
@click.option('--lang', default='ru', help='Language to use. Allowed values: ru, uk')
@click.option('--dict', 'path', help='Dictionary folder path')
@click.option('-v', '--verbose', is_flag=True, help='Be more verbose')
def cli_dict_mem_usage(lang, path, verbose):
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.debug([lang, path, verbose])

    return show_dict_mem_usage(lang, path, verbose)

@cli_dict.command(name='meta', context_settings={'show_default': True})
@click.option('--lang', default='ru', help='Language to use. Allowed values: ru, uk')
@click.option('--dict', 'path', help='Dictionary folder path')
def cli_dict_meta(lang, path):
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
    logger.debug([lang, path])

    return show_dict_meta(lang, path)


def _open_for_read(fn):
    """ Open a file for reading """
    if fn in ['-', '', None]:
        return sys.stdin
    return open(fn, 'rt', encoding='utf8')


# ============================ Commands ===========================

def show_dict_mem_usage(lang, dict_path=None, verbose=False):
    """
    Show dictionary memory usage.
    """
    initial_mem = get_mem_usage()
    initial_time = time.time()

    end_time = time.time()
    mem_usage = get_mem_usage()

    logger.info(
        'Memory usage: %0.1fM dictionary, %0.1fM total (load time %0.2fs)',
        (mem_usage-initial_mem)/(1024*1024), mem_usage/(1024*1024), end_time-initial_time
    )


def show_dict_meta(lang, dict_path=None):
    morph = pymorphy3.MorphAnalyzer(path=dict_path, lang=lang)

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
        _parse = lru_cache(maxsize=None)(_parse)
    else:
        cache_size = int(cache_size)
        if cache_size:
            _parse = lru_cache(cache_size)(_parse)
    _write = out_file.write

    for token in iter_tokens(in_file):
        _write(_parse(token))


class _TokenParserFormatter:
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
                            f"{p.normal_form}:{p.score:0.3f}={p.tag}"
                            for p in morph_parse(tok) if p.score >= thresh
                        ]
                        return tpl % (tok, join(seq))
                else:
                    def _parse_token(tok):
                        seq = [
                            f"{p.normal_form}:{p.tag}"
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
                        seq = [f"{lemma}:{w:0.3f}" for (lemma, w) in items]
                    else:
                        seq = [lemma for (lemma, w) in items]

                    return tpl % (tok, join(seq))
        else:
            if score:
                def _parse_token(tok):
                    seq = [
                        f"{p.score:0.3f}={p.tag}"
                        for p in morph_parse(tok) if p.score >= thresh
                    ]
                    return tpl % (tok, join(seq))
            else:
                def _parse_token(tok):
                    seq = [
                        str(p.tag)
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

if __name__ == '__main__':
    main()
