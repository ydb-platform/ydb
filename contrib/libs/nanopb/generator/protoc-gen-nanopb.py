#!/usr/bin/env python3
# This file is used to invoke nanopb_generator.py as a plugin
# to protoc on Linux and other *nix-style systems.
# Use it like this:
# protoc --plugin=protoc-gen-nanopb=..../protoc-gen-nanopb --nanopb_out=dir foo.proto

from . import nanopb_generator


def main():
    # Assume we are running as a plugin under protoc.
    nanopb_generator.main_plugin()
