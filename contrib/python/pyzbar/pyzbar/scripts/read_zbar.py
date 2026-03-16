#!/usr/bin/env python
from __future__ import print_function

import argparse
import sys

import pyzbar
from pyzbar.pyzbar import decode


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Reads barcodes in images, using the zbar library'
    )
    parser.add_argument('image', nargs='+')
    parser.add_argument(
        '-v', '--version', action='version',
        version='%(prog)s ' + pyzbar.__version__
    )
    args = parser.parse_args(args)

    from PIL import Image

    for image in args.image:
        for barcode in decode(Image.open(image)):
            print(barcode.data)


if __name__ == '__main__':
    main()
