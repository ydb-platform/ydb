from __future__ import print_function

import sys

import pyperclip
from .api import parse


def main():
    if sys.stdin.isatty():
        if len(sys.argv) > 1:
            # If an argument is passed
            result = parse(sys.argv[1])
        else:
            # Otherwise pull from clipboard
            result = parse(pyperclip.paste())
    else:
        result = parse(sys.stdin.read())
    print("\n" + result)
