"""
:mod:`pymorphy3.opencorpora_dict.preprocess` is a
module for preprocessing parsed OpenCorpora dictionaries.

The presence of this module means that pymorphy3 dictionaries are
not fully compatible with OpenCorpora.
"""
import collections
import logging
from functools import lru_cache

from pymorphy3.utils import with_progress

logger = logging.getLogger(__name__)


def simplify_tags(parsed_dict, skip_space_ambiguity=True):
    """
    This function simplifies tags in :param:`parsed_dict`.
    :param:`parsed_dict` is modified inplace.
    """
    logger.info("simplifying tags: looking for tag spellings")
    spellings = _get_tag_spellings(parsed_dict)

    logger.info("simplifying tags: looking for spelling duplicates "
                "(skip_space_ambiguity: %s)", skip_space_ambiguity)
    tag_replaces = _get_duplicate_tag_replaces(spellings, skip_space_ambiguity)
    logger.debug("%d duplicate tags will be removed", len(tag_replaces))

    logger.info("simplifying tags: fixing")
    for lex_id in with_progress(parsed_dict.lexemes, "Simplifying tags"):
        new_lexeme = [
            (word, _simplify_tag(tag, tag_replaces))
            for word, tag in parsed_dict.lexemes[lex_id]
        ]
        parsed_dict.lexemes[lex_id] = new_lexeme


def drop_unsupported_parses(parsed_dict):
    """
    Remove unsupported parses from OpenCorpora dictionary.

    In particular, lexemes with Init tags are removed
    because pymorphy3 handles them differently.
    """
    logger.info("dropping unsupported parses")
    for lex_id in parsed_dict.lexemes:
        parsed_dict.lexemes[lex_id] = [
            (word, tag) for word, tag in parsed_dict.lexemes[lex_id]
            if 'Init' not in tag
        ]


@lru_cache(maxsize=None)
def tag2grammemes(tag_str):
    """ Given tag string, return tag grammemes """
    return _split_grammemes(replace_redundant_grammemes(tag_str))


@lru_cache(maxsize=None)
def replace_redundant_grammemes(tag_str):
    """ Replace 'loc1', 'gen1' and 'acc1' grammemes in ``tag_str`` """
    return tag_str.replace('loc1', 'loct').replace('gen1', 'gent').replace('acc1', 'accs')


def _split_grammemes(tag_str):
    return frozenset(tag_str.replace(' ', ',', 1).split(','))


def _get_tag_spellings(parsed_dict):
    """
    Return a dict where keys are sets of grammemes found in dictionary
    and values are counters of all tag spellings for these grammemes.
    """
    spellings = collections.defaultdict(lambda: collections.defaultdict(int))
    for tag in _itertags(parsed_dict):
        spellings[tag2grammemes(tag)][tag] += 1
    return spellings


def _get_duplicate_tag_replaces(spellings, skip_space_ambiguity):
    replaces = {}
    for grammemes in spellings:
        tags = spellings[grammemes]
        if _is_ambiguous(tags.keys(), skip_space_ambiguity):
            items = sorted(tags.items(), key=lambda it: it[1], reverse=True)
            top_tag = items[0][0]
            for tag, count in items[1:]:
                replaces[tag] = top_tag
    return replaces


def _is_ambiguous(tags, skip_space_ambiguity=True):
    """
    >>> _is_ambiguous(['NOUN sing,masc'])
    False
    >>> _is_ambiguous(['NOUN sing,masc', 'NOUN masc,sing'])
    True
    >>> _is_ambiguous(['NOUN masc,sing', 'NOUN,masc sing'])
    False
    >>> _is_ambiguous(['NOUN masc,sing', 'NOUN,masc sing'], skip_space_ambiguity=False)
    True
    """
    if len(tags) < 2:
        return False

    if skip_space_ambiguity:
        # if space position differs then skip this ambiguity
        # XXX: this doesn't handle cases when space position difference
        # is not the only ambiguity
        space_pos = [tag.index(' ') if ' ' in tag else None
                     for tag in map(str, tags)]
        if len(space_pos) == len(set(space_pos)):
            return False

    return True


def _simplify_tag(tag, tag_replaces):
    tag = replace_redundant_grammemes(tag)
    return tag_replaces.get(tag, tag)


def _itertags(parsed_dict):
    for lex_id in with_progress(parsed_dict.lexemes, "Looking for tag spellings"):
        for word, tag in parsed_dict.lexemes[lex_id]:
            yield tag
