import argparse

from .cmd import setup

parser = argparse.ArgumentParser(description="Generate Open API Specification")
setup(parser)
args = parser.parse_args()
args.func(args)
