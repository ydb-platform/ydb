#!/bin/bash
#
# Script to collect the coverage report for the specified contents
# in yql/essentials/core (i.e. type annotation machinery, optimizers)
# to the directory, given as the first argument.
set -eu

ARC_ROOT=$(arc rev-parse --show-toplevel)
# Save the coverage report with to the given directory; otherwise,
# save the list into the temporary file.
REPORT_ROOT=${1:-$(mktemp --tmpdir -d yql-essentials-core-coverage-XXXXXXX)}
# Define the array of the prefixes to be collected to the coverage
# report.
COVERAGE_PREFIXES=(
    'yql/essentials/core/type_ann'
)
# XXX: Join the prefixes listed above by a pipe (|) to make a
# valid regexp alternatives for COVERAGE_TARGET_REGEXP parameter.
COVERAGE_PREFIX_ALTERNATIVES=$(IFS='|'; echo "${COVERAGE_PREFIXES[*]}")

# Run the command to collect code coverage over the sources in
# /yql/essentials/core/ by the minirun test suite.
# XXX: Here are the rationales for the particular options:
# * --clang-coverage -- collect the code coverage only for C++
#                       sources;
# * --coverage-report --output $REPORT_ROOT -- build the HTML
#                       report for the collected code coverage;
# * -DCOVERAGE_TARGET_REGEXP -- collect the code coverage only
#                       for the dedicated source files;
# XXX: --coverage-prefix-filter doesn't work properly if several
# paths are given, but fortunately, there is a recipe for C++
# coverage in DEVTOOLSSUPPORT-52275#67093d22473c6c1da0bd17fd, that
# uses -DCOVERAGE_TARGET_REGEXP and allows to specify several
# paths by regexp.
# See more info here: https://docs.yandex-team.ru/devtools/test/coverage.
ya make -tA                                                    \
    -C ${ARC_ROOT}/yql/essentials/tests/sql/minirun            \
    -C ${ARC_ROOT}/yql/essentials/tests/s-expressions/minirun  \
    -DCOVERAGE_TARGET_REGEXP="($COVERAGE_PREFIX_ALTERNATIVES)" \
    --build profile                                            \
    --clang-coverage                                           \
    --coverage-report                                          \
    --output ${REPORT_ROOT}                                    \
    --test-disable-timeout

# Create an anchor for each prefix, to ensure the analyzer part
# that the coverage for particular prefixes is collected.
for prefix in ${COVERAGE_PREFIXES[@]}; do
    COLLECTED_PREFIX=${REPORT_ROOT}/$(basename $0)/$prefix
    mkdir -p ${COLLECTED_PREFIX}
    touch ${COLLECTED_PREFIX}/collected
done

echo "The coverage report is stored in ${REPORT_ROOT}"
