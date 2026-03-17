# -*- coding: utf-8 -*-
"""
:mod:`pymorphy2.opencorpora_dict.compile` is a
module for converting OpenCorpora dictionaries
to pymorphy2 representation.
"""
from __future__ import absolute_import, unicode_literals
import os
import logging
import collections
import itertools
import array
import operator

try:
    izip = itertools.izip
except AttributeError:
    izip = zip

from pymorphy2 import dawg
from pymorphy2.utils import (
    longest_common_substring,
    largest_elements,
    with_progress
)

logger = logging.getLogger(__name__)


CompiledDictionary = collections.namedtuple(
    'CompiledDictionary',
    'gramtab suffixes paradigms words_dawg prediction_suffixes_dawgs parsed_dict compile_options'
)

_pick_second_item = operator.itemgetter(1)


def convert_to_pymorphy2(opencorpora_dict_path, out_path, source_name,
                         language_code, overwrite=False, compile_options=None):
    """
    Convert a dictionary from OpenCorpora XML format to
    Pymorphy2 compacted format.

    ``out_path`` should be a name of folder where to put dictionaries.
    """
    from .parse import parse_opencorpora_xml
    from .preprocess import simplify_tags, drop_unsupported_parses
    from .storage import save_compiled_dict

    dawg.assert_can_create()
    if not _create_out_path(out_path, overwrite):
        return

    parsed_dict = parse_opencorpora_xml(opencorpora_dict_path)
    simplify_tags(parsed_dict)
    drop_unsupported_parses(parsed_dict)
    compiled_dict = compile_parsed_dict(parsed_dict, compile_options)
    save_compiled_dict(compiled_dict, out_path,
                       source_name=source_name, language_code=language_code)


def compile_parsed_dict(parsed_dict, compile_options=None):
    """
    Return compacted dictionary data.
    """
    options = dict(
        min_ending_freq=2,
        min_paradigm_popularity=3,
        max_suffix_length=5,
    )
    options.update(compile_options or {})
    paradigm_prefixes = options["paradigm_prefixes"]

    gramtab = []
    paradigms = []
    words = []

    tag_ids = dict()
    paradigm_ids = dict()

    logger.info("inlining lexeme derivational rules")
    lexemes = _join_lexemes(parsed_dict.lexemes, parsed_dict.links)
    logger.info("lexemes after link inlining: %s", len(lexemes))

    logger.info('building paradigms')
    logger.debug("%20s %15s %15s %15s", "word", "len(gramtab)", "len(words)", "len(paradigms)")

    paradigm_popularity = collections.defaultdict(int)

    for index, lexeme in enumerate(lexemes):
        stem, paradigm = _to_paradigm(lexeme, paradigm_prefixes)

        # build gramtab
        for suff, tag, pref in paradigm:
            if tag not in tag_ids:
                tag_ids[tag] = len(gramtab)
                gramtab.append(tag)

        # build paradigm index
        if paradigm not in paradigm_ids:
            paradigm_ids[paradigm] = len(paradigms)
            paradigms.append(
                tuple([(suff, tag_ids[tag], pref) for suff, tag, pref in paradigm])
            )

        para_id = paradigm_ids[paradigm]
        paradigm_popularity[para_id] += 1

        for idx, (suff, tag, pref) in enumerate(paradigm):
            form = pref+stem+suff
            words.append(
                (form, (para_id, idx))
            )

        if not (index % 10000):
            word = paradigm[0][2] + stem + paradigm[0][0]
            logger.debug("%20s %15s %15s %15s", word, len(gramtab), len(words), len(paradigms))


    logger.debug("%20s %15s %15s %15s", "total:", len(gramtab), len(words), len(paradigms))
    logger.debug("linearizing paradigms")

    def get_form(para):
        return list(next(izip(*para)))

    forms = [get_form(para) for para in paradigms]
    suffixes = sorted(set(list(itertools.chain(*forms))))
    suffix_ids = dict(
        (suff, index)
        for index, suff in enumerate(suffixes)
    )

    paradigm_prefix_ids = dict(
        (pref, idx) for idx, pref in enumerate(paradigm_prefixes)
    )
    def fix_strings(paradigm):
        """ Replace suffix and prefix with the respective id numbers. """
        para = []
        for suff, tag, pref in paradigm:
            para.append(
                (suffix_ids[suff], tag, paradigm_prefix_ids[pref])
            )
        return para

    paradigms = (fix_strings(para) for para in paradigms)
    paradigms = [_linearized_paradigm(paradigm) for paradigm in paradigms]

    logger.debug('calculating prediction data..')
    suffixes_dawgs_data = _suffixes_prediction_data(
        words=words,
        paradigm_popularity=paradigm_popularity,
        gramtab=gramtab,
        paradigms=paradigms,
        suffixes=suffixes,
        min_ending_freq=options["min_ending_freq"],
        min_paradigm_popularity=options["min_paradigm_popularity"],
        max_suffix_length=options["max_suffix_length"],
        paradigm_prefixes=paradigm_prefixes,
    )

    logger.debug('building word DAFSA')
    words_dawg = dawg.WordsDawg(words)

    del words

    prediction_suffixes_dawgs = []
    for prefix_id, dawg_data in enumerate(suffixes_dawgs_data):
        logger.debug('building prediction_suffixes DAFSA #%d' % prefix_id)
        prediction_suffixes_dawgs.append(dawg.PredictionSuffixesDAWG(dawg_data))

    return CompiledDictionary(
        gramtab=tuple(gramtab),
        suffixes=suffixes,
        paradigms=paradigms,
        words_dawg=words_dawg,
        prediction_suffixes_dawgs=prediction_suffixes_dawgs,
        parsed_dict=parsed_dict,
        compile_options=options,
    )


def _join_lexemes(lexemes, links):
    """
    Combine linked lexemes to a single lexeme.
    """

#    <link_types>
#    <type id="1">ADJF-ADJS</type>
#    <type id="2">ADJF-COMP</type>
#    <type id="3">INFN-VERB</type>
#    <type id="4">INFN-PRTF</type>
#    <type id="5">INFN-GRND</type>
#    <type id="6">PRTF-PRTS</type>
#    <type id="7">NAME-PATR</type>
#    <type id="8">PATR_MASC-PATR_FEMN</type>
#    <type id="9">SURN_MASC-SURN_FEMN</type>
#    <type id="10">SURN_MASC-SURN_PLUR</type>
#    <type id="11">PERF-IMPF</type>
#    <type id="12">ADJF-SUPR_ejsh</type>
#    <type id="13">PATR_MASC_FORM-PATR_MASC_INFR</type>
#    <type id="14">PATR_FEMN_FORM-PATR_FEMN_INFR</type>
#    <type id="15">ADJF_eish-SUPR_nai_eish</type>
#    <type id="16">ADJF-SUPR_ajsh</type>
#    <type id="17">ADJF_aish-SUPR_nai_aish</type>
#    <type id="18">ADJF-SUPR_suppl</type>
#    <type id="19">ADJF-SUPR_nai</type>
#    <type id="20">ADJF-SUPR_slng</type>
#    <type id="21">FULL-CONTRACTED</type>
#    <type id="22">NORM-ORPHOVAR</type>
#    <type id="23">CARDINAL-ORDINAL</type>      # e.g. первый - один
#    <type id="24">SBST_MASC-SBST_FEMN</type>   # e.g. подсудимый - подсудимая
#    <type id="25">SBST_MASC-SBST_PLUR</type>   # e.g. подсудимый - подсудимые
#    <type id="26">ADVB-COMP</type>             # ??
#    <type id="27">ADJF_TEXT-ADJF_NUMBER</type> # e.g. первый - 1-й
#    </link_types>

    EXCLUDED_LINK_TYPES = set(['7', '21', '23', '27'])
#    ALLOWED_LINK_TYPES = set(['3', '4', '5'])

    moves = dict()

    def move_lexeme(from_id, to_id):
        lm = lexemes[str(from_id)]

        while to_id in moves:
            to_id = moves[to_id]

        lexemes[str(to_id)].extend(lm)
        del lm[:]
        moves[from_id] = to_id

    for link_start, link_end, type_id in links:
        if type_id in EXCLUDED_LINK_TYPES:
            continue

#        if type_id not in ALLOWED_LINK_TYPES:
#            continue

        move_lexeme(link_end, link_start)

    lex_ids = sorted(lexemes.keys(), key=int)
    return [lexemes[lex_id] for lex_id in lex_ids if lexemes[lex_id]]


def _to_paradigm(lexeme, paradigm_prefixes):
    """
    Extract (stem, paradigm) pair from lexeme (which is a list of
    (word_form, tag) tuples). Paradigm is a list of suffixes with
    associated tags and prefixes.
    """
    forms, tags = list(zip(*lexeme))

    if len(forms) == 1:
        stem = forms[0]
        prefixes = ['']
    else:
        stem = longest_common_substring(forms)
        prefixes = [form[:form.index(stem)] for form in forms]

        # only allow prefixes from PARADIGM_PREFIXES
        if any(pref not in paradigm_prefixes for pref in prefixes):
            # With right PARADIGM_PREFIXES empty stem is fine;
            # os.path.commonprefix doesn't return anything useful
            # for prediction.
            # stem = os.path.commonprefix(forms)
            stem = ""
            prefixes = [''] * len(tags)

    suffixes = (
        form[len(pref)+len(stem):]
        for form, pref in zip(forms, prefixes)
    )
    return stem, tuple(zip(suffixes, tags, prefixes))


def _suffixes_prediction_data(words, paradigm_popularity, gramtab, paradigms, suffixes,
                              min_ending_freq, min_paradigm_popularity, max_suffix_length,
                              paradigm_prefixes):

    logger.debug('calculating prediction data: removing non-productive paradigms..')
    productive_paradigms = _popular_keys(paradigm_popularity, min_paradigm_popularity)

    # ["suffix"] => number of occurrences
    # this is for removing non-productive suffixes
    ending_counts = collections.defaultdict(int)

    # [form_prefix_id]["suffix"]["POS"][(para_id, idx)] => number or occurrences
    # this is for selecting most popular parses
    prefix_endings = {}
    for form_prefix_id in range(len(paradigm_prefixes)):
        prefix_endings[form_prefix_id] = collections.defaultdict(
                                    lambda: collections.defaultdict(
                                        lambda: collections.defaultdict(int)))

    logger.debug('calculating prediction data: checking word endings..')
    for word, (para_id, idx) in with_progress(words, "Checking word endings"):

        if para_id not in productive_paradigms:
            continue

        paradigm = paradigms[para_id]

        form_count = len(paradigm) // 3

        tag = gramtab[paradigm[form_count + idx]]
        form_prefix_id = paradigm[2*form_count + idx]
        form_prefix = paradigm_prefixes[form_prefix_id]
        form_suffix = suffixes[paradigm[idx]]

        assert len(word) >= len(form_prefix+form_suffix), word
        assert word.startswith(form_prefix), word
        assert word.endswith(form_suffix), word

        if len(word) == len(form_prefix) + len(form_suffix):
            # pseudo-paradigms are useless for prediction
            continue

        POS = tuple(tag.replace(' ', ',', 1).split(','))[0]

        for i in range(max(len(form_suffix), 1), max_suffix_length+1): #was: 1,2,3,4,5
            word_end = word[-i:]
            ending_counts[word_end] += 1
            prefix_endings[form_prefix_id][word_end][POS][(para_id, idx)] += 1

    dawgs_data = []

    for form_prefix_id in sorted(prefix_endings.keys()):
        logger.debug('calculating prediction data: preparing DAFSA #%d..' % form_prefix_id)
        endings = prefix_endings[form_prefix_id]
        dawgs_data.append(
            _get_suffixes_dawg_data(endings, ending_counts, min_ending_freq)
        )

    return dawgs_data


def _get_suffixes_dawg_data(endings, ending_counts, min_ending_freq):
    counted_suffixes_dawg_data = []

    for ending in endings:
        if ending_counts[ending] < min_ending_freq:
            continue

        for POS in endings[ending]:

            common_form_counts = largest_elements(
                iterable=endings[ending][POS].items(),
                key=_pick_second_item,
                n=1,
            )

            for form, cnt in common_form_counts:
                # form is a `(para_id, idx)` tuple here
                # XXX: shouldn't we use inverted cnt to make the results
                # sorted high to low?
                counted_suffixes_dawg_data.append(
                    (ending, (cnt,) + form)
                )

    return counted_suffixes_dawg_data


def _popular_keys(counter, threshold):
    return set(key for (key, count) in counter.items() if count >= threshold)


def _linearized_paradigm(paradigm):
    """
    Convert ``paradigm`` (a list of tuples with numbers)
    to 1-dimensional array.array (for reduced memory usage).
    """
    return array.array(str("H"), list(itertools.chain(*zip(*paradigm))))


def _create_out_path(out_path, overwrite=False):
    try:
        logger.debug("Creating output folder %s", out_path)
        os.mkdir(out_path)
    except OSError:
        if overwrite:
            logger.info("Output folder already exists, overwriting..")
        else:
            logger.warning("Output folder already exists!")
            return False
    return True

