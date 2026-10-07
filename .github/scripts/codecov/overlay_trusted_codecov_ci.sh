#!/usr/bin/env bash
# Overlay coverage CI helpers from a checkout of the PR base (main) into the
# current workspace. Invoked only from cpp_codecov.yml on pull_request_target;
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

# Replace the whole Python import directory. Copying only known files would let
# a PR leave a sibling module (for example json.py) that shadows a trusted import.
rm -rf .github/scripts/codecov
mkdir -p .github/scripts
cp -a "$src/.github/scripts/codecov" .github/scripts/
