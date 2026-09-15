#!/bin/sh

find . -type f -exec sed --in-place 's/_generated.h"/.fbs.h"/' '{}' ';'
find . -type f -name '*_generated.h' -exec rm '{}' ';'
