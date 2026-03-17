
import re
import logging
from contextlib import contextmanager

from razdel.substring import find_substrings


def dot_sentenize_(text):
    previous = 0
    for match in re.finditer(r'([.?!…])\s+', text, re.U):
        delimiter = match.group(1)
        start = match.start()
        yield text[previous:start] + delimiter
        previous = match.end()
    if previous < len(text):
        yield text[previous:]


def dot_sentenize(text):
    chunks = dot_sentenize_(text)
    return find_substrings(chunks, text)


@contextmanager
def no_logger(logger):
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = False


LOGGER = logging.getLogger()


def deepmipt_sentenize(text):
    from rusenttokenize import ru_sent_tokenize

    with no_logger(LOGGER):
        chunks = ru_sent_tokenize(text)
    return find_substrings(chunks, text)


def nltk_sentenize(text):
    from nltk import sent_tokenize

    chunks = sent_tokenize(text, 'russian')
    return find_substrings(chunks, text)


def segtok_sentenize(text):
    from segtok.segmenter import split_single

    chunks = split_single(text)
    return find_substrings(chunks, text)


MOSES_SENT = None


def moses_sentenize(text):
    from mosestokenizer import MosesSentenceSplitter

    global MOSES_SENT
    if not MOSES_SENT:
        MOSES_SENT = MosesSentenceSplitter('ru')

    chunks = MOSES_SENT([text])
    return find_substrings(chunks, text)


TOKEN = re.compile(r'([^\W\d]+|\d+|[^\w\s])', re.U)


def space_tokenize(text):
    chunks = re.split(r'\s+', text)
    return find_substrings(chunks, text)


def re_tokenize(text):
    chunks = TOKEN.findall(text)
    return find_substrings(chunks, text)


def nltk_tokenize(text):
    from nltk.tokenize import word_tokenize

    chunks = word_tokenize(text, 'russian')
    return find_substrings(chunks, text)


NLP = None


def spacy_tokenize(text):
    from spacy.lang.ru import Russian

    global NLP
    if not NLP:
        NLP = Russian()

    doc = NLP(text)
    chunks = [token.text for token in doc]
    return find_substrings(chunks, text)


NLP2 = None


def spacy_tokenize2(text):
    from spacy.lang.ru import Russian
    from spacy_russian_tokenizer import (
        RussianTokenizer,
        MERGE_PATTERNS,
        SYNTAGRUS_RARE_CASES
    )

    global NLP2
    if not NLP2:
        NLP2 = Russian()
        NLP2.add_pipe(
            RussianTokenizer(NLP2, MERGE_PATTERNS + SYNTAGRUS_RARE_CASES),
            name='russian_tokenizer'
        )

    doc = NLP2(text)
    chunks = [token.text for token in doc]
    return find_substrings(chunks, text)


# {'analysis': [], 'text': 'Mystem'},
#  {'text': ' — '},
#  {'analysis': [{'lex': 'консольный'}], 'text': 'консольная'},
#  {'text': ' '},
#  {'analysis': [{'lex': 'программа'}], 'text': 'программа'},
#  {'text': '\n'}]


MYSTEM = None


def parse_mystem(data):
    for item in data:
        text = item['text'].strip()
        if text:
            yield text


def mystem_tokenize(text):
    from pymystem3 import Mystem

    global MYSTEM
    if not MYSTEM:
        MYSTEM = Mystem(
            grammar_info=False,
            entire_input=True,
            disambiguation=False,
            weight=False
        )

    data = MYSTEM.analyze(text)
    chunks = parse_mystem(data)
    return find_substrings(chunks, text)


MOSES_TOK = None


def moses_tokenize(text):
    from mosestokenizer import MosesTokenizer

    global MOSES_TOK
    if not MOSES_TOK:
        MOSES_TOK = MosesTokenizer('ru')
        # disable
        MOSES_TOK.argv.append('-no-escape')  # " -> &quot;
        MOSES_TOK.argv.remove('-a')  # - -> @-@
        MOSES_TOK.restart()

    chunks = MOSES_TOK(text)
    return find_substrings(chunks, text)


def segtok_tokenize(text):
    from segtok.tokenizer import word_tokenizer

    chunks = word_tokenizer(text)
    return find_substrings(chunks, text)
