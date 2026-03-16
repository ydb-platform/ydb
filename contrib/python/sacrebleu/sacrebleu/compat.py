# -*- coding: utf-8 -*-

from typing import Union, Iterable, List
from argparse import Namespace

from .tokenizers import DEFAULT_TOKENIZER, TOKENIZERS
from .metrics import BLEU, CHRF, TER, BLEUScore, CHRFScore, TERScore


######################################################################
# Backward compatibility functions for old style API access (< 1.4.11)
######################################################################
def corpus_bleu(sys_stream: Union[str, Iterable[str]],
                ref_streams: Union[str, List[Iterable[str]]],
                smooth_method='exp',
                smooth_value=None,
                force=False,
                lowercase=False,
                tokenize=DEFAULT_TOKENIZER,
                use_effective_order=False) -> BLEUScore:
    """Produces BLEU scores along with its sufficient statistics from a source against one or more references.

    :param sys_stream: The system stream (a sequence of segments)
    :param ref_streams: A list of one or more reference streams (each a sequence of segments)
    :param smooth_method: The smoothing method to use
    :param smooth_value: For 'floor' smoothing, the floor to use
    :param force: Ignore data that looks already tokenized
    :param lowercase: Lowercase the data
    :param tokenize: The tokenizer to use
    :return: a `BLEUScore` object
    """
    args = Namespace(
        smooth_method=smooth_method, smooth_value=smooth_value, force=force,
        short=False, lc=lowercase, tokenize=tokenize)

    metric = BLEU(args)
    return metric.corpus_score(
        sys_stream, ref_streams, use_effective_order=use_effective_order)


def raw_corpus_bleu(sys_stream,
                    ref_streams,
                    smooth_value=None) -> BLEUScore:
    """Convenience function that wraps corpus_bleu().
    This is convenient if you're using sacrebleu as a library, say for scoring on dev.
    It uses no tokenization and 'floor' smoothing, with the floor default to 0 (no smoothing).

    :param sys_stream: the system stream (a sequence of segments)
    :param ref_streams: a list of one or more reference streams (each a sequence of segments)
    :return: Returns a `BLEUScore` object.
    """
    return corpus_bleu(
        sys_stream, ref_streams, smooth_method='floor',
        smooth_value=smooth_value, force=True, tokenize='none',
        use_effective_order=True)


def sentence_bleu(hypothesis: str,
                  references: List[str],
                  smooth_method: str = 'floor',
                  smooth_value: float = None,
                  use_effective_order: bool = True) -> BLEUScore:
    """
    Computes BLEU on a single sentence pair.

    Disclaimer: computing BLEU on the sentence level is not its intended use,
    BLEU is a corpus-level metric.

    :param hypothesis: Hypothesis string.
    :param references: List of reference strings.
    :param smooth_method: The smoothing method to use
    :param smooth_value: For 'floor' smoothing, the floor value to use.
    :param use_effective_order: Account for references that are shorter than the largest n-gram.
    :return: Returns a `BLEUScore` object.
    """
    args = Namespace(
        smooth_method=smooth_method, smooth_value=smooth_value, force=False,
        short=False, lc=False, tokenize=DEFAULT_TOKENIZER)

    metric = BLEU(args)
    return metric.sentence_score(
        hypothesis, references, use_effective_order=use_effective_order)


def corpus_chrf(hypotheses: Iterable[str],
                references: List[Iterable[str]],
                order: int = CHRF.ORDER,
                beta: float = CHRF.BETA,
                remove_whitespace: bool = True) -> CHRFScore:
    """
    Computes ChrF on a corpus.

    :param hypotheses: Stream of hypotheses.
    :param references: Stream of references.
    :param order: Maximum n-gram order.
    :param beta: Defines importance of recall w.r.t precision. If beta=1, same importance.
    :param remove_whitespace: Whether to delete all whitespace from hypothesis and reference strings.
    :return: A `CHRFScore` object.
    """
    args = Namespace(
        chrf_order=order, chrf_beta=beta, chrf_whitespace=not remove_whitespace, short=False)
    metric = CHRF(args)
    return metric.corpus_score(hypotheses, references)


def sentence_chrf(hypothesis: str,
                  references: List[str],
                  order: int = CHRF.ORDER,
                  beta: float = CHRF.BETA,
                  remove_whitespace: bool = True) -> CHRFScore:
    """
    Computes ChrF on a single sentence pair.

    :param hypothesis: Hypothesis string.
    :param references: Reference string(s).
    :param order: Maximum n-gram order.
    :param beta: Defines importance of recall w.r.t precision. If beta=1, same importance.
    :param remove_whitespace: Whether to delete whitespaces from hypothesis and reference strings.
    :return: A `CHRFScore` object.
    """
    args = Namespace(
        chrf_order=order, chrf_beta=beta, chrf_whitespace=not remove_whitespace, short=False)
    metric = CHRF(args)
    return metric.sentence_score(hypothesis, references)


def corpus_ter(hypotheses: Iterable[str],
               references: List[Iterable[str]],
               normalized: bool = False,
               no_punct: bool = False,
               asian_support: bool = False,
               case_sensitive: bool = False) -> TERScore:
    """
    Computes TER on a corpus.

    :param hypotheses: Stream of hypotheses.
    :param references: Stream of references.
    :param normalized: Enable character normalization.
    :param no_punct: Remove punctuation.
    :param asian_support: Enable special treatment of Asian characters.
    :param case_sensitive: Enable case sensitivity.
    :return: A `TERScore` object.
    """
    args = Namespace(
        normalized=normalized, no_punct=no_punct,
        asian_support=asian_support, case_sensitive=case_sensitive)
    metric = TER(args)
    return metric.corpus_score(hypotheses, references)


def sentence_ter(hypothesis: str,
                 references: List[str],
                 normalized: bool = False,
                 no_punct: bool = False,
                 asian_support: bool = False,
                 case_sensitive: bool = False) -> TERScore:
    """
    Computes TER on a single sentence pair.

    :param hypothesis: Hypothesis string.
    :param references: Reference string(s).
    :param normalized: Enable character normalization.
    :param no_punct: Remove punctuation.
    :param asian_support: Enable special treatment of Asian characters.
    :param case_sensitive: Enable case sensitivity.
    :return: A `TERScore` object.
    """
    args = Namespace(
        normalized=normalized, no_punct=no_punct,
        asian_support=asian_support, case_sensitive=case_sensitive)
    metric = TER(args)
    return metric.sentence_score(hypothesis, references)
