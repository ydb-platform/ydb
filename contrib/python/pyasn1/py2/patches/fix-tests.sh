#!/bin.sh

find tests -type f -exec sed --in-place 's/from tests.base/from __tests__.base/g' '{}' ';'
