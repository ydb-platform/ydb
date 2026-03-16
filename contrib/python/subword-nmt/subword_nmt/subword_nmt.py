#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import sys
import codecs
import argparse

from .learn_bpe import learn_bpe
from .apply_bpe import BPE, read_vocabulary
from .get_vocab import get_vocab
from .learn_joint_bpe_and_vocab import learn_joint_bpe_and_vocab

from .learn_bpe import create_parser as create_learn_bpe_parser
from .apply_bpe import create_parser as create_apply_bpe_parser
from .get_vocab import create_parser as create_get_vocab_parser
from .learn_joint_bpe_and_vocab import create_parser as create_learn_joint_bpe_and_vocab_parser

# hack for python2/3 compatibility
argparse.open = io.open

def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description="subword-nmt: unsupervised word segmentation for neural machine translation and text generation ")
    subparsers = parser.add_subparsers(dest='command',
                                       help="""command to run. Run one of the commands with '-h' for more info.

learn-bpe: learn BPE merge operations on input text.
apply-bpe: apply given BPE operations to input text.
get-vocab: extract vocabulary and word frequencies from input text.
learn-joint-bpe-and-vocab: executes recommended workflow for joint BPE.""")

    learn_bpe_parser = create_learn_bpe_parser(subparsers)
    apply_bpe_parser = create_apply_bpe_parser(subparsers)
    get_vocab_parser = create_get_vocab_parser(subparsers)
    learn_joint_bpe_and_vocab_parser = create_learn_joint_bpe_and_vocab_parser(subparsers)

    args = parser.parse_args()

    if args.command == 'learn-bpe':
        # read/write files as UTF-8
        if args.input.name != '<stdin>':
            args.input = codecs.open(args.input.name, encoding='utf-8')
        if args.output.name != '<stdout>':
            args.output = codecs.open(args.output.name, 'w', encoding='utf-8')

        learn_bpe(args.input, args.output, args.symbols, args.min_frequency, args.verbose, 
                  is_dict=args.dict_input, total_symbols=args.total_symbols)
    elif args.command == 'apply-bpe':
        # read/write files as UTF-8
        args.codes = codecs.open(args.codes.name, encoding='utf-8')
        if args.input.name != '<stdin>':
            args.input = codecs.open(args.input.name, encoding='utf-8')
        if args.output.name != '<stdout>':
            args.output = codecs.open(args.output.name, 'w', encoding='utf-8')
        if args.vocabulary:
            args.vocabulary = codecs.open(args.vocabulary.name, encoding='utf-8')

        if args.vocabulary:
            vocabulary = read_vocabulary(args.vocabulary, args.vocabulary_threshold)
        else:
            vocabulary = None

        if sys.version_info < (3, 0):
            args.separator = args.separator.decode('UTF-8')
            if args.glossaries:
                args.glossaries = [g.decode('UTF-8') for g in args.glossaries]

        bpe = BPE(args.codes, args.merges, args.separator, vocabulary, args.glossaries)

        for line in args.input:
            args.output.write(bpe.process_line(line, args.dropout))

    elif args.command == 'get-vocab':
        if args.input.name != '<stdin>':
            args.input = codecs.open(args.input.name, encoding='utf-8')
        if args.output.name != '<stdout>':
            args.output = codecs.open(args.output.name, 'w', encoding='utf-8')
        get_vocab(args.input, args.output)
    elif args.command == 'learn-joint-bpe-and-vocab':
        learn_joint_bpe_and_vocab(args)
        if sys.version_info < (3, 0):
            args.separator = args.separator.decode('UTF-8')
    else:
        raise Exception('Invalid command provided')


# python 2/3 compatibility
if sys.version_info < (3, 0):
    sys.stderr = codecs.getwriter('UTF-8')(sys.stderr)
    sys.stdout = codecs.getwriter('UTF-8')(sys.stdout)
    sys.stdin = codecs.getreader('UTF-8')(sys.stdin)
else:
    sys.stderr = codecs.getwriter('UTF-8')(sys.stderr.buffer)
    sys.stdout = codecs.getwriter('UTF-8')(sys.stdout.buffer)
    sys.stdin = codecs.getreader('UTF-8')(sys.stdin.buffer)
