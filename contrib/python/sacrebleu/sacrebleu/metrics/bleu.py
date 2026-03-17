# -*- coding: utf-8 -*-

import math
import logging
from collections import Counter
from itertools import zip_longest
from typing import List, Iterable, Union

from ..tokenizers import TOKENIZERS
from ..utils import my_log
from .base import BaseScore, Signature

sacrelogger = logging.getLogger('sacrebleu')

class BLEUSignature(Signature):
    def __init__(self, args):
        super().__init__(args)

        self._abbr.update({
            'smooth': 's',
            'case': 'c',
            'tok': 'tok',
            'numrefs': '#',
        })

        self.info.update({
            'smooth': self.args['smooth_method'],
            'case': 'lc' if self.args['lc'] else 'mixed',
            'tok': TOKENIZERS[self.args['tokenize']]().signature(),
            'numrefs': self.args.get('num_refs', '?'),
        })


class BLEUScore(BaseScore):
    """A convenience class to represent BLEU scores (without signature)."""
    def __init__(self, score, counts, totals, precisions, bp, sys_len, ref_len):
        super().__init__(score)

        self.prefix = 'BLEU'
        self.bp = bp
        self.counts = counts
        self.totals = totals
        self.sys_len = sys_len
        self.ref_len = ref_len
        self.precisions = precisions
        self.prec_str = "/".join(["{:.1f}".format(p) for p in self.precisions])

    def format(self, width=2, score_only=False, signature=''):
        if score_only:
            return '{0:.{1}f}'.format(self.score, width)

        prefix = "{}+{}".format(self.prefix, signature) if signature else self.prefix

        s = '{pr} = {sc:.{w}f} {prec} (BP = {bp:.3f} ratio = {r:.3f} hyp_len = {sl:d} ref_len = {rl:d})'.format(
            pr=prefix,
            sc=self.score,
            w=width,
            prec=self.prec_str,
            bp=self.bp,
            r=self.sys_len / self.ref_len,
            sl=self.sys_len,
            rl=self.ref_len)
        return s


class BLEU:
    NGRAM_ORDER = 4

    SMOOTH_DEFAULTS = {
        'floor': 0.0,
        'add-k': 1,
        'exp': None,    # No value is required
        'none': None,   # No value is required
    }

    def __init__(self, args):
        self.name = 'bleu'
        self.force = args.force
        self.lc = args.lc
        self.smooth_value = args.smooth_value
        self.smooth_method = args.smooth_method
        self.tokenizer = TOKENIZERS[args.tokenize]()
        self.signature = BLEUSignature(args)

        # Sanity check
        assert self.smooth_method in self.SMOOTH_DEFAULTS.keys(), \
            "Unknown smooth_method '{}'".format(self.smooth_method)

    @staticmethod
    def extract_ngrams(line, min_order=1, max_order=NGRAM_ORDER) -> Counter:
        """Extracts all the ngrams (min_order <= n <= max_order) from a sequence of tokens.

        :param line: A segment containing a sequence of words.
        :param min_order: Minimum n-gram length (default: 1).
        :param max_order: Maximum n-gram length (default: NGRAM_ORDER).
        :return: a dictionary containing ngrams and counts
        """

        ngrams = Counter() # type: Counter
        tokens = line.split()
        for n in range(min_order, max_order + 1):
            for i in range(0, len(tokens) - n + 1):
                ngram = ' '.join(tokens[i: i + n])
                ngrams[ngram] += 1

        return ngrams

    @staticmethod
    def reference_stats(refs, output_len):
        """Extracts reference statistics for a given segment.

        :param refs: A list of segment tokens.
        :param output_len: Hypothesis length for this segment.
        :return: a tuple of (ngrams, closest_diff, closest_len)
        """

        ngrams = Counter()
        closest_diff = None
        closest_len = None

        for ref in refs:
            tokens = ref.split()
            reflen = len(tokens)
            diff = abs(output_len - reflen)
            if closest_diff is None or diff < closest_diff:
                closest_diff = diff
                closest_len = reflen
            elif diff == closest_diff:
                if reflen < closest_len:
                    closest_len = reflen

            ngrams_ref = BLEU.extract_ngrams(ref)
            for ngram in ngrams_ref.keys():
                ngrams[ngram] = max(ngrams[ngram], ngrams_ref[ngram])

        return ngrams, closest_diff, closest_len

    @staticmethod
    def compute_bleu(correct: List[int],
                     total: List[int],
                     sys_len: int,
                     ref_len: int,
                     smooth_method: str = 'none',
                     smooth_value=None,
                     use_effective_order=False) -> BLEUScore:
        """Computes BLEU score from its sufficient statistics. Adds smoothing.

        Smoothing methods (citing "A Systematic Comparison of Smoothing Techniques for Sentence-Level BLEU",
        Boxing Chen and Colin Cherry, WMT 2014: http://aclweb.org/anthology/W14-3346)

        - exp: NIST smoothing method (Method 3)
        - floor: Method 1
        - add-k: Method 2 (generalizing Lin and Och, 2004)
        - none: do nothing.

        :param correct: List of counts of correct ngrams, 1 <= n <= NGRAM_ORDER
        :param total: List of counts of total ngrams, 1 <= n <= NGRAM_ORDER
        :param sys_len: The cumulative system length
        :param ref_len: The cumulative reference length
        :param smooth: The smoothing method to use
        :param smooth_value: The smoothing value for `floor` and `add-k` methods. `None` falls back to default value.
        :param use_effective_order: If true, use the length of `correct` for the n-gram order instead of NGRAM_ORDER.
        :return: A BLEU object with the score (100-based) and other statistics.
        """
        assert smooth_method in BLEU.SMOOTH_DEFAULTS.keys(), \
            "Unknown smooth_method '{}'".format(smooth_method)

        # Fetch the default value for floor and add-k
        if smooth_value is None:
            smooth_value = BLEU.SMOOTH_DEFAULTS[smooth_method]

        precisions = [0.0 for x in range(BLEU.NGRAM_ORDER)]

        smooth_mteval = 1.
        effective_order = BLEU.NGRAM_ORDER
        for n in range(1, BLEU.NGRAM_ORDER + 1):
            if smooth_method == 'add-k' and n > 1:
                correct[n-1] += smooth_value
                total[n-1] += smooth_value
            if total[n-1] == 0:
                break

            if use_effective_order:
                effective_order = n

            if correct[n-1] == 0:
                if smooth_method == 'exp':
                    smooth_mteval *= 2
                    precisions[n-1] = 100. / (smooth_mteval * total[n-1])
                elif smooth_method == 'floor':
                    precisions[n-1] = 100. * smooth_value / total[n-1]
            else:
                precisions[n-1] = 100. * correct[n-1] / total[n-1]

        # If the system guesses no i-grams, 1 <= i <= NGRAM_ORDER, the BLEU
        # score is 0 (technically undefined). This is a problem for sentence
        # level BLEU or a corpus of short sentences, where systems will get
        # no credit if sentence lengths fall under the NGRAM_ORDER threshold.
        # This fix scales NGRAM_ORDER to the observed maximum order.
        # It is only available through the API and off by default

        if sys_len < ref_len:
            bp = math.exp(1 - ref_len / sys_len) if sys_len > 0 else 0.0
        else:
            bp = 1.0

        score = bp * math.exp(
            sum(map(my_log, precisions[:effective_order])) / effective_order)

        return BLEUScore(
            score, correct, total, precisions, bp, sys_len, ref_len)

    def sentence_score(self, hypothesis: str,
                       references: List[str],
                       use_effective_order: bool = True) -> BLEUScore:
        """
        Computes BLEU on a single sentence pair.

        Disclaimer: computing BLEU on the sentence level is not its intended use,
        BLEU is a corpus-level metric.

        :param hypothesis: Hypothesis string.
        :param references: List of reference strings.
        :param use_effective_order: Account for references that are shorter than the largest n-gram.
        :return: a `BLEUScore` object containing everything you'd want
        """
        assert not isinstance(references, str), "sentence_score needs a list of references, not a single string"
        return self.corpus_score(hypothesis, [[ref] for ref in references],
                                 use_effective_order=use_effective_order)

    def corpus_score(self, sys_stream: Union[str, Iterable[str]],
                     ref_streams: Union[str, List[Iterable[str]]],
                     use_effective_order: bool = False) -> BLEUScore:
        """Produces BLEU scores along with its sufficient statistics from a source against one or more references.

        :param sys_stream: The system stream (a sequence of segments)
        :param ref_streams: A list of one or more reference streams (each a sequence of segments)
        :param use_effective_order: Account for references that are shorter than the largest n-gram.
        :return: a `BLEUScore` object containing everything you'd want
        """

        # Add some robustness to the input arguments
        if isinstance(sys_stream, str):
            sys_stream = [sys_stream]

        if isinstance(ref_streams, str):
            ref_streams = [[ref_streams]]

        sys_len = 0
        ref_len = 0

        correct = [0 for n in range(self.NGRAM_ORDER)]
        total = [0 for n in range(self.NGRAM_ORDER)]

        # look for already-tokenized sentences
        tokenized_count = 0

        fhs = [sys_stream] + ref_streams
        for lines in zip_longest(*fhs):
            if None in lines:
                raise EOFError("Source and reference streams have different lengths!")

            if self.lc:
                lines = [x.lower() for x in lines]

            if not (self.force or self.tokenizer.signature() == 'none') and lines[0].rstrip().endswith(' .'):
                tokenized_count += 1

                if tokenized_count == 100:
                    sacrelogger.warning('That\'s 100 lines that end in a tokenized period (\'.\')')
                    sacrelogger.warning('It looks like you forgot to detokenize your test data, which may hurt your score.')
                    sacrelogger.warning('If you insist your data is detokenized, or don\'t care, you can suppress this message with \'--force\'.')

            output, *refs = [self.tokenizer(x.rstrip()) for x in lines]

            output_len = len(output.split())
            ref_ngrams, closest_diff, closest_len = BLEU.reference_stats(refs, output_len)

            sys_len += output_len
            ref_len += closest_len

            sys_ngrams = BLEU.extract_ngrams(output)
            for ngram in sys_ngrams.keys():
                n = len(ngram.split())
                correct[n-1] += min(sys_ngrams[ngram], ref_ngrams.get(ngram, 0))
                total[n-1] += sys_ngrams[ngram]

        # Get BLEUScore object
        score = self.compute_bleu(
            correct, total, sys_len, ref_len,
            smooth_method=self.smooth_method, smooth_value=self.smooth_value,
            use_effective_order=use_effective_order)

        return score
