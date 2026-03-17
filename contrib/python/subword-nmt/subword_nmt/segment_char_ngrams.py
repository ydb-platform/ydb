#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Rico Sennrich

from __future__ import unicode_literals, division

import sys
import codecs
import argparse

# hack for python2/3 compatibility
from io import open
argparse.open = open

def create_parser(subparsers=None):

    if subparsers:
        parser = subparsers.add_parser('segment-char-ngrams',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="segment rare words into character n-grams")
    else:
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description="segment rare words into character n-grams")

    parser.add_argument(
        '--input', '-i', type=argparse.FileType('r'), default=sys.stdin,
        metavar='PATH',
        help="Input file (default: standard input).")
    parser.add_argument(
        '--vocab', type=argparse.FileType('r'), metavar='PATH',
        required=True,
        help="Vocabulary file.")
    parser.add_argument(
        '--shortlist', type=int, metavar='INT', default=0,
        help="do not segment INT most frequent words in vocabulary (default: '%(default)s')).")
    parser.add_argument(
        '-n', type=int, metavar='INT', default=2,
        help="segment rare words into character n-grams of size INT (default: '%(default)s')).")
    parser.add_argument(
        '--output', '-o', type=argparse.FileType('w'), default=sys.stdout,
        metavar='PATH',
        help="Output file (default: standard output)")
    parser.add_argument(
        '--separator', '-s', type=str, default='@@', metavar='STR',
        help="Separator between non-final subword units (default: '%(default)s'))")

    return parser

def segment_char_ngrams(args):

    vocab = [line.split()[0] for line in args.vocab if len(line.split()) == 2]
    vocab = dict((y,x) for (x,y) in enumerate(vocab))

    for line in args.input:
      for word in line.split():
        if word not in vocab or vocab[word] > args.shortlist:
          i = 0
          while i*args.n < len(word):
            args.output.write(word[i*args.n:i*args.n+args.n])
            i += 1
            if i*args.n < len(word):
              args.output.write(args.separator)
            args.output.write(' ')
        else:
          args.output.write(word + ' ')
      args.output.write('\n')


if __name__ == '__main__':

    # python 2/3 compatibility
    if sys.version_info < (3, 0):
        sys.stderr = codecs.getwriter('UTF-8')(sys.stderr)
        sys.stdout = codecs.getwriter('UTF-8')(sys.stdout)
        sys.stdin = codecs.getreader('UTF-8')(sys.stdin)
    else:
        sys.stderr = codecs.getwriter('UTF-8')(sys.stderr.buffer)
        sys.stdout = codecs.getwriter('UTF-8')(sys.stdout.buffer)
        sys.stdin = codecs.getreader('UTF-8')(sys.stdin.buffer)

    parser = create_parser()
    args = parser.parse_args()

    if sys.version_info < (3, 0):
        args.separator = args.separator.decode('UTF-8')

    # read/write files as UTF-8
    args.vocab = codecs.open(args.vocab.name, encoding='utf-8')
    if args.input.name != '<stdin>':
        args.input = codecs.open(args.input.name, encoding='utf-8')
    if args.output.name != '<stdout>':
        args.output = codecs.open(args.output.name, 'w', encoding='utf-8')

    segment_char_ngrams(args)