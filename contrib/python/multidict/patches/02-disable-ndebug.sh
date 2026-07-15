#!/bin/sh
# Disable the _md_check_consistency debug helpers in hashtable.h.
#
# Upstream gates these blocks behind `#ifndef NDEBUG`. Under the Arcadia build
# NDEBUG is not always defined, so the asserts compile in and break the build.
# Replacing the guard with `#if 0` keeps the blocks permanently disabled.
#
# Both `#ifndef NDEBUG` occurrences in this header guard the same helper, so an
# unconditional replacement is safe.
# See NOCDEV-18704, DEVTOOLSSUPPORT-88661, NOCDEVDUTY-5981
set -e

# macOS ships BSD sed (incompatible `-i` syntax); use GNU sed (gsed) there.
if [ "$(uname)" = "Darwin" ]; then
    SED=gsed
else
    SED=sed
fi

"$SED" -i 's/^#ifndef NDEBUG$/#if 0/' multidict/_multilib/hashtable.h
