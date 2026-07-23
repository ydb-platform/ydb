#!/usr/bin/env bash
# Overlay CI tooling (.github from the workflow/tooling branch) onto the
# checked-out tree, but keep mute lists and TESTOWNERS from the code checkout.
#
# Debug and production runs must evaluate mutes/owners against the ref under
# test, not against whatever mute state happens to sit on the tooling branch.
set -euo pipefail

TOOLING_ROOT="${1:-.ci_tooling}"
KEEP_DIR=$(mktemp -d)
trap 'rm -rf "$KEEP_DIR"' EXIT

mkdir -p "$KEEP_DIR/config"
for f in muted_ya.txt muted_ya_asan.txt muted_ya_msan.txt muted_ya_tsan.txt; do
  if [ -f ".github/config/$f" ]; then
    cp -a ".github/config/$f" "$KEEP_DIR/config/$f"
  fi
done
if [ -f .github/TESTOWNERS ]; then
  cp -a .github/TESTOWNERS "$KEEP_DIR/TESTOWNERS"
fi

cp -a "$TOOLING_ROOT/.github/." .github/

for f in muted_ya.txt muted_ya_asan.txt muted_ya_msan.txt muted_ya_tsan.txt; do
  if [ -f "$KEEP_DIR/config/$f" ]; then
    mkdir -p .github/config
    cp -a "$KEEP_DIR/config/$f" ".github/config/$f"
  fi
done
if [ -f "$KEEP_DIR/TESTOWNERS" ]; then
  cp -a "$KEEP_DIR/TESTOWNERS" .github/TESTOWNERS
fi

rm -rf "$TOOLING_ROOT"
echo "Overlaid CI tooling from $TOOLING_ROOT (mute lists + TESTOWNERS kept from checkout)"
