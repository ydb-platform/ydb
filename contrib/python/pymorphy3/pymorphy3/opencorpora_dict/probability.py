"""
Module for estimating P(t|w) from partially annotated OpenCorpora XML dump
and saving this information to a file.

This module requires NLTK 3.x, opencorpora-tools>=0.4.4 and dawg >= 0.7
packages for probability estimation and resulting file creation.
"""
import logging
import os

from pymorphy3 import MorphAnalyzer
from pymorphy3.dawg import ConditionalProbDistDAWG
from pymorphy3.opencorpora_dict.preprocess import tag2grammemes
from pymorphy3.opencorpora_dict.storage import update_meta
from pymorphy3.utils import with_progress


def add_conditional_tag_probability(corpus_filename, out_path, min_word_freq,
                                    logger=None, morph=None):
    """ Add P(t|w) estimates to a compiled dictionary """

    if morph is None:
        morph = MorphAnalyzer(out_path, probability_estimator_cls=None)

    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info("Estimating P(t|w) from %s", corpus_filename)
    cpd, cfd = estimate_conditional_tag_probability(morph, corpus_filename, logger)

    logger.info("Encoding P(t|w) as DAWG")
    d = build_cpd_dawg(morph, cpd, int(min_word_freq))
    dawg_filename = os.path.join(out_path, 'p_t_given_w.intdawg')
    d.save(dawg_filename)

    logger.info("Updating meta information")
    meta_filename = os.path.join(out_path, 'meta.json')
    update_meta(meta_filename, [
        ('P(t|w)', True),
        ('P(t|w)_unique_words', len(cpd.conditions())),
        ('P(t|w)_outcomes', cfd.N()),
        ('P(t|w)_min_word_freq', int(min_word_freq)),
    ])
    logger.info('\nDone.')


def estimate_conditional_tag_probability(morph, corpus_filename, logger=None):
    """
    Estimate P(t|w) based on OpenCorpora xml dump.

    Probability is estimated based on counts of disambiguated
    ambiguous words, using simple Laplace smoothing.
    """
    import nltk
    import opencorpora

    if logger is None:
        logger = logging.getLogger(__name__)

    class _ConditionalProbDist(nltk.ConditionalProbDist):
        """
        This ConditionalProbDist subclass passes 'condition' variable to
        probdist_factory. See https://github.com/nltk/nltk/issues/500
        """
        def __init__(self, cfdist, probdist_factory):
            self._probdist_factory = probdist_factory
            for condition in cfdist:
                self[condition] = probdist_factory(cfdist[condition], condition)

    reader = opencorpora.CorpusReader(corpus_filename)

    disambig_words = list(
        with_progress(
            _disambiguated_words(reader),
            "Reading disambiguated words from corpus"
        )
    )

    disambig_words = with_progress(disambig_words, "Filtering out non-ambiguous words")
    ambiguous_words = [
        (w, gr) for (w, gr) in (
            (w.lower(), tag2grammemes(t))
            for (w, t) in disambig_words
            if len(morph.tag(w)) > 1
        ) if gr != {'UNKN'}
    ]

    logger.info("Computing P(t|w)")

    def probdist_factory(fd, condition):
        bins = max(len(morph.tag(condition)), fd.B())
        return nltk.LaplaceProbDist(fd, bins=bins)

    cfd = nltk.ConditionalFreqDist(ambiguous_words)
    cpd = _ConditionalProbDist(cfd, probdist_factory)
    return cpd, cfd


def build_cpd_dawg(morph, cpd, min_word_freq):
    """
    Return conditional tag probability information encoded as DAWG.

    For each "interesting" word and tag the resulting DAWG
    stores ``"word:tag"`` key with ``probability*1000000`` integer value.
    """
    words = [word for (word, fd) in cpd.items()
             if fd.freqdist().N() >= min_word_freq]

    prob_data = filter(
        lambda rec: not _all_the_same(rec[1]),
        ((word, _tag_probabilities(morph, word, cpd)) for word in words)
    )
    dawg_data = (
        ((word, tag), prob)
        for word, probs in prob_data
        for tag, prob in probs.items()
    )
    return ConditionalProbDistDAWG(dawg_data)


def _disambiguated_words(reader):
    return (
        (word, parses[0][1])
        for (word, parses) in reader.iter_parsed_words()
        if len(parses) == 1
    )


def _all_the_same(probs):
    return len(set(probs.values())) <= 1


def _parse_probabilities(morph, word, cpd):
    """
    Return probabilities of word parses
    according to CustomConditionalProbDist ``cpd``.
    """
    parses = morph.parse(word)
    probabilities = [cpd[word].prob(p.tag.grammemes) for p in parses]
    return list(zip(parses, probabilities))


def _tag_probabilities(morph, word, cpd):
    return dict(
        (p.tag, prob)
        for (p, prob) in _parse_probabilities(morph, word, cpd)
    )
