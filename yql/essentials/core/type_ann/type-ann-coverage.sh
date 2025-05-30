#!/bin/bash
#
# Script to collect the type annotation callbacks, being uncovered
# by minirun test suite. Pass the coverage report root directory
# as the first argument, and the filename as the second argument
# to store the collected list to the particular file.
set -eu

REPORT_ROOT=$1
# Check whether the desired prefix (i.e. yql/essentials/core/type_ann)
# is collected (see collect-coverage.sh for the contract).
COLLECTED_PREFIX=yql/essentials/core/type_ann
COLLECTED_ANCHOR=${REPORT_ROOT}/collect-coverage.sh/${COLLECTED_PREFIX}/collected
if [ ! -f ${COLLECTED_ANCHOR} ]; then
    cat <<NOANCHOR
==================================================================
[FATAL] Anchor file is missing: ${COLLECTED_ANCHOR}
------------------------------------------------------------------
NB: Looks like the particular prefix (i.e. ${COLLECTED_PREFIX})
is not set in collect-coverage.sh.
==================================================================
NOANCHOR
    exit 1
fi

# Save the list with uncovered callbacks into the file, that is
# given by the first parameter of this script; otherwise, save
# the list into the temporary file.
UNCOVERED_FILE=${2:-$(mktemp --tmpdir yql-essentials-core-type_ann-uncovered-XXXXXXX.list)}
# File with the list of the callbacks to be ignored by coverage.
UNCOVERED_IGNORE=$(realpath $0 | sed -e 's/\.sh/\.ignore/')
if [ ! -r ${UNCOVERED_IGNORE} ]; then
    cat <<NOIGNORE
==================================================================
[FATAL] Ignore file is missing: ${UNCOVERED_IGNORE}
------------------------------------------------------------------
NB: If no uncovered type annotation callbacks ought to be ignored,
just "touch" the empty file and do not remove it in future.
==================================================================
NOIGNORE
    exit 1
fi

# Find an anchor to uncovered line in HTML report, ...
UNCOVERED_ANCHOR="<td class='uncovered-line'><pre>0</pre></td>"
# ... find the return type of the type annotation callback,
# preceding the target function name ...
RETURN_TYPE="IGraphTransformer::TStatus"
# XXX: See more info re \K here: https://perldoc.perl.org/perlre#%5CK.
CALLBACK_PREFIX="<td class='code'><pre>\s*${RETURN_TYPE}\s*\K"
# ... and find the parameters types of the type annotation
# callback, following the target function name.
INPUT_TYPE="const TExprNode::TPtr&amp; input"
OUTPUT_TYPE="TExprNode::TPtr&amp; output"
CONTEXT_TYPE="(?:TExtContext|TContext)&amp; ctx"
CALLBACK_SUFFIX="(?=\(${INPUT_TYPE},\s*${OUTPUT_TYPE},\s*${CONTEXT_TYPE}\))"
grep -oP "${UNCOVERED_ANCHOR}${CALLBACK_PREFIX}(\w+)${CALLBACK_SUFFIX}" \
    -r ${REPORT_ROOT}/coverage.report/                                  \
    --no-filename                                                       \
    | grep -vf ${UNCOVERED_IGNORE}                                      \
    | tee -a ${UNCOVERED_FILE}

echo "The list of the uncovered functions: $UNCOVERED_FILE"
# Make script fail if uncovered list is not empty.
test ! -s $UNCOVERED_FILE
