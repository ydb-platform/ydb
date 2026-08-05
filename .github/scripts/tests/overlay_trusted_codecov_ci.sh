#!/usr/bin/env bash
# Overlay coverage CI helpers from a checkout of the PR base (main) into the
# current workspace. Invoked only from cpp_codecov.yml on pull_request* events;
# the script itself must come from the trusted base checkout, not the PR tree.
set -euo pipefail

src="${1:-_trusted_coverage_ci}"
if [ ! -d "$src/.github/actions/run_clang_codecov" ]; then
  echo "Trusted coverage CI checkout missing under ${src}" >&2
  exit 1
fi

rm -rf .github/actions/run_clang_codecov
cp -a "$src/.github/actions/run_clang_codecov" .github/actions/

rm -rf .github/actions/setup_ci_ydb_service_account_key_file_credentials
cp -a "$src/.github/actions/setup_ci_ydb_service_account_key_file_credentials" .github/actions/

mkdir -p .github/scripts/tests
for f in codecov_suites.py detect_codecov_matrix.py export_coverage_lcov.py generate_coverage_landing.py; do
  cp "$src/.github/scripts/tests/$f" ".github/scripts/tests/$f"
done
