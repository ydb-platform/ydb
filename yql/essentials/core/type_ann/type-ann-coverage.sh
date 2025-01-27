#!/bin/bash
#
# Script to collect the type annotation callbacks, being uncovered
# by minirun test suite. Pass the filename as the first argument
# to store the collected list to the particular file.
set -eu

ARC_ROOT=$(arc rev-parse --show-toplevel)
REPORT_ROOT=$(mktemp --tmpdir -d yql-essentials-core-type_ann-coverage-XXXXXXX)
# Save the list with uncovered callbacks into the file, that is
# given by the first parameter of this script; otherwise, save
# the list into the temporary file.
UNCOVERED_FILE=${1:-$(mktemp --tmpdir yql-essentials-core-type_ann-uncovered-XXXXXXX.list)}
# File with the list of the callbacks to be ignored by coverage.
UNCOVERED_IGNORE=$(realpath $0 | sed -e 's/\.sh/\.ignore/')
if [ ! -r $UNCOVERED_IGNORE ]; then
    cat <<NOIGNORE
==================================================================
[FATAL] Ignore file is missing: $UNCOVERED_IGNORE
------------------------------------------------------------------
NB: If no uncovered type annotation callbacks ought to be ignored,
just "touch" the empty file and do not remove it in future.
==================================================================
NOIGNORE
    exit 1
fi

# Run the command to collect code coverage over the sources in
# /yql/essentials/core/type_ann by the minirun test suite.
# XXX: Here are the rationales for the particular options:
# * --clang-coverage -- collect the code coverage only for C++
#                       sources;
# * --coverage-prefix-filter -- collect the code coverage only
#                       for the dedicated source files;
# * --coverage-report --output $REPORT_ROOT -- build the HTML
#                       report for the collected code coverage;
# See more info here: https://docs.yandex-team.ru/devtools/test/coverage.
ya make -tA                                                    \
    -C ${ARC_ROOT}/yql/essentials/tests/sql/minirun            \
    -C ${ARC_ROOT}/yql/essentials/tests/s-expressions/minirun  \
    --build profile                                            \
    --clang-coverage                                           \
    --coverage-prefix-filter yql/essentials/core/type_ann      \
    --coverage-report                                          \
    --output $REPORT_ROOT                                      \
    --test-disable-timeout

# Find an anchor to uncovered line in HTML report, ...
UNCOVERED_ANCHOR="<td class='uncovered-line'><pre>0</pre></td>"
# ... find the return type of the type annotation callback,
# preceding the target function name ...
RETURN_TYPE="IGraphTransformer::TStatus"
# XXX: See more info re \K here: https://perldoc.perl.org/perlre#%5CK.
CALLBACK_PREFIX="<td class='code'><pre>\s*$RETURN_TYPE\s*\K"
# ... and find the parameters types of the type annotation
# callback, following the target function name.
INPUT_TYPE="const TExprNode::TPtr&amp; input"
OUTPUT_TYPE="TExprNode::TPtr&amp; output"
CONTEXT_TYPE="(?:TExtContext|TContext)&amp; ctx"
CALLBACK_SUFFIX="(?=\($INPUT_TYPE,\s*$OUTPUT_TYPE,\s*$CONTEXT_TYPE\))"
grep -oP "$UNCOVERED_ANCHOR$CALLBACK_PREFIX(\w+)$CALLBACK_SUFFIX" \
    -r $REPORT_ROOT/coverage.report/                              \
    --no-filename                                                 \
    | grep -vf $UNCOVERED_IGNORE                                  \
    | tee -a $UNCOVERED_FILE

rm -rf $REPORT_ROOT
echo "The list of the uncovered functions: $UNCOVERED_FILE"
# Make script fail if uncovered list is not empty.
test ! -s $UNCOVERED_FILE
