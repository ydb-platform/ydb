#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Rico Sennrich

"""Compute chrF3 for machine translation evaluation

Reference:
Maja Popović (2015). chrF: character n-gram F-score for automatic MT evaluation. In Proceedings of the Tenth Workshop on Statistical Machine Translationn, pages 392–395, Lisbon, Portugal.
"""

from __future__ import print_function, unicode_literals, division

import sys
import codecs
import io
import argparse

from collections import defaultdict

# hack for python2/3 compatibility
from io import open
argparse.open = open

def create_parser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="learn BPE-based word segmentation")

    parser.add_argument(
        '--ref', '-r', type=argparse.FileType('r'), required=True,
        metavar='PATH',
        help="Reference file")
    parser.add_argument(
        '--hyp', type=argparse.FileType('r'), metavar='PATH',
        default=sys.stdin,
        help="Hypothesis file (default: stdin).")
    parser.add_argument(
        '--beta', '-b', type=float, default=3,
        metavar='FLOAT',
        help="beta parameter (default: '%(default)s')")
    parser.add_argument(
        '--ngram', '-n', type=int, default=6,
        metavar='INT',
        help="ngram order (default: '%(default)s')")
    parser.add_argument(
        '--space', '-s', action='store_true',
        help="take spaces into account (default: '%(default)s')")
    parser.add_argument(
        '--precision', action='store_true',
        help="report precision (default: '%(default)s')")
    parser.add_argument(
        '--recall', action='store_true',
        help="report recall (default: '%(default)s')")

    return parser

def extract_ngrams(words, max_length=4, spaces=False):

    if not spaces:
        words = ''.join(words.split())
    else:
        words = words.strip()

    results = defaultdict(lambda: defaultdict(int))
    for length in range(max_length):
        for start_pos in range(len(words)):
            end_pos = start_pos + length + 1
            if end_pos <= len(words):
                results[length][tuple(words[start_pos: end_pos])] += 1
    return results


def get_correct(ngrams_ref, ngrams_test, correct, total):

    for rank in ngrams_test:
        for chain in ngrams_test[rank]:
            total[rank] += ngrams_test[rank][chain]
            if chain in ngrams_ref[rank]:
                correct[rank] += min(ngrams_test[rank][chain], ngrams_ref[rank][chain])

    return correct, total


def f1(correct, total_hyp, total_ref, max_length, beta=3, smooth=0):

    precision = 0
    recall = 0

    for i in range(max_length):
      if total_hyp[i] + smooth and total_ref[i] + smooth:
        precision += (correct[i] + smooth) / (total_hyp[i] + smooth)
        recall += (correct[i] + smooth) / (total_ref[i] + smooth)

    precision /= max_length
    recall /= max_length

    return (1 + beta**2) * (precision*recall) / ((beta**2 * precision) + recall), precision, recall

def main(args):

    correct = [0]*args.ngram
    total = [0]*args.ngram
    total_ref = [0]*args.ngram
    for line in args.ref:
      line2 = args.hyp.readline()

      ngrams_ref = extract_ngrams(line, max_length=args.ngram, spaces=args.space)
      ngrams_test = extract_ngrams(line2, max_length=args.ngram, spaces=args.space)

      get_correct(ngrams_ref, ngrams_test, correct, total)

      for rank in ngrams_ref:
          for chain in ngrams_ref[rank]:
              total_ref[rank] += ngrams_ref[rank][chain]

    chrf, precision, recall = f1(correct, total, total_ref, args.ngram, args.beta)

    print('chrF3: {0:.4f}'.format(chrf))
    if args.precision:
        print('chrPrec: {0:.4f}'.format(precision))
    if args.recall:
        print('chrRec: {0:.4f}'.format(recall))

if __name__ == '__main__':

    # python 2/3 compatibility
    if sys.version_info < (3, 0):
        sys.stderr = codecs.getwriter('UTF-8')(sys.stderr)
        sys.stdout = codecs.getwriter('UTF-8')(sys.stdout)
        sys.stdin = codecs.getreader('UTF-8')(sys.stdin)
    else:
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', write_through=True, line_buffering=True)

    parser = create_parser()
    args = parser.parse_args()

    main(args)
