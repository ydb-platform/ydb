import os
import sys

import process_command_files as pcf

# /script/move.py <src-1> <tgt-1> <src-2> <tgt-2> ... <src-n> <tgt-n>
# renames src-1 to tgt-1, src-2 to tgt-2, ..., src-n to tgt-n.


def main():
    args = pcf.get_args(sys.argv[1:])
    assert len(args) % 2 == 0, (len(args), args)

    copied = set()

    for index in range(0, len(args), 2):
        assert args[index] not in copied, "Double input detected for file: {}".format(args[index])

        os.rename(args[index], args[index + 1])
        copied.add(args[index])


if __name__ == '__main__':
    main()
