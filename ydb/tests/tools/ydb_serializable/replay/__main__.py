import json
from argparse import ArgumentParser
from ydb.tests.tools.ydb_serializable.lib import (
    History,
    DummyLogger,
    SerializabilityChecker,
)


def main():
    parser = ArgumentParser()
    parser.add_argument('-f', '--file', required=True)
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    with open(args.file, 'r', encoding='utf8') as f:
        history = History.from_json(json.load(f))

    logger = DummyLogger()
    checker = SerializabilityChecker(logger=logger, debug=args.debug, explain=True)
    history.apply_to(checker)

    print(
        '%d nodes (%d committed, %d aborted)' % (
            len(checker.nodes),
            len(checker.committed),
            len(checker.aborted)
        )
    )

    checker.verify()


if __name__ == '__main__':
    main()
