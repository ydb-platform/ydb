from MeCab import Tagger
import sys
import fileinput


def parse():
    """Parse input from stdin.

    This is a simple wrapper for mecab-python3 so you can test it from the
    command line.  Like the mecab binary, it treats each line of stdin as one
    sentence. You can pass tagger arguments here too.
    """

    args = ' '.join(sys.argv[1:])
    tagger = Tagger(args)

    for line in fileinput.input([]):
        # strip the newline on output
        print(tagger.parse(line.strip())[:-1])


def info():
    """Print configuration info."""
    args = ' '.join(sys.argv[1:])
    tagger = Tagger(args)
    di = tagger.dictionary_info()
    # TODO get the package version here too
    print("mecab-py dictionary info:")
    print("-----")
    while di:
        print('version:'.ljust(10), di.version)
        print('size:'.ljust(10), di.size)
        print('charset:'.ljust(10), di.charset)
        print('filename:'.ljust(10), di.filename)
        print("-----")
        di = di.next
