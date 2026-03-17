
import sys
import argparse
from random import seed, sample as sample_

from razdel import (
    sentenize,
    tokenize
)

from .partition import (
    parse_partitions,
    format_partitions,
    update_partitions
)
from .gen import (
    generate_partition_precision_tests,
    generate_partition_recall_tests
)
from .zoo import (
    dot_sentenize,
    deepmipt_sentenize,
    nltk_sentenize,
    segtok_sentenize,
    moses_sentenize,

    space_tokenize,
    re_tokenize,
    nltk_tokenize,
    spacy_tokenize,
    spacy_tokenize2,
    mystem_tokenize,
    moses_tokenize,
)


ZOO = {
    'dot_sentenize': dot_sentenize,
    'deepmipt_sentenize': deepmipt_sentenize,
    'nltk_sentenize': nltk_sentenize,
    'segtok_sentenize': segtok_sentenize,
    'moses_sentenize': moses_sentenize,
    'sentenize': sentenize,

    'space_tokenize': space_tokenize,
    're_tokenize': re_tokenize,
    'nltk_tokenize': nltk_tokenize,
    'spacy_tokenize': spacy_tokenize,
    'spacy_tokenize2': spacy_tokenize2,
    'mystem_tokenize': mystem_tokenize,
    'moses_tokenize': moses_tokenize,
    'tokenize': tokenize,
}


def stdin_lines():
    for line in sys.stdin:
        yield line.rstrip('\n')


def stdout_lines(lines):
    for line in lines:
        print(line)


def gen_(partitions, precision, recall):
    for partition in partitions:
        if precision:
            for test in generate_partition_precision_tests(partition):
                yield test
        if recall:
            for test in generate_partition_recall_tests(partition):
                yield test


def gen(args):
    precision = args.precision
    recall = args.recall
    if not precision and not recall:
        precision = True
        recall = True
    lines = stdin_lines()
    partitions = parse_partitions(lines)
    tests = gen_(partitions, precision, recall)
    lines = format_partitions(tests)
    stdout_lines(lines)


def sample(args):
    seed(args.seed)
    lines = list(stdin_lines())
    lines = sample_(lines, args.size)
    stdout_lines(lines)


def show_(guess, etalon):
    print('---etalon')
    for _ in etalon:
        print('>', _.text)
    print('---guess')
    for _ in guess:
        print('>', _.text)
    print()


def diff_(tests, segment, show):
    for test in tests:
        guess = list(segment(test.text))
        etalon = list(test.substrings)
        if guess != etalon:
            if show:
                show_(guess, etalon)
            else:
                yield test


def diff(args):
    segment = ZOO[args.segment]
    lines = stdin_lines()
    partitions = parse_partitions(lines)
    tests = diff_(partitions, segment, args.show)
    lines = format_partitions(tests)
    stdout_lines(lines)


def up(args):
    segment = ZOO[args.segment]
    lines = stdin_lines()
    partitions = parse_partitions(lines)
    partitions = update_partitions(partitions, segment)
    lines = format_partitions(partitions)
    stdout_lines(lines)


def main():
    parser = argparse.ArgumentParser(prog='razdel-ctl')
    parser.set_defaults(function=None)

    subs = parser.add_subparsers()

    sub = subs.add_parser('gen')
    sub.set_defaults(function=gen)
    sub.add_argument('--precision', action='store_true')
    sub.add_argument('--recall', action='store_true')

    sub = subs.add_parser('sample')
    sub.set_defaults(function=sample)
    sub.add_argument('size', type=int)
    sub.add_argument('--seed', type=int, default=1)

    sub = subs.add_parser('diff')
    sub.set_defaults(function=diff)
    sub.add_argument('segment', choices=ZOO)
    sub.add_argument('--show', action='store_true')

    sub = subs.add_parser('up')
    sub.set_defaults(function=up)
    sub.add_argument('segment', choices=ZOO)

    args = sys.argv[1:]
    args = parser.parse_args(args)
    if not args.function:
        parser.print_help()
        parser.exit()
    try:
        args.function(args)
    except BrokenPipeError:
        pass


if __name__ == '__main__':
    main()
