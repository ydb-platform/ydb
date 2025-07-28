#!/bin/bash

# Script to backport HTTP library commits from main to stable-25-1
# Usage: ./backport-http-commits.sh

set -e

echo "Starting HTTP library backport process..."

# Create backport branch from stable-25-1
git checkout stable-25-1
git pull origin stable-25-1
git checkout -b http-actors-backport-to-stable-25-1

echo "Cherry-picking commits in chronological order..."

# List of commits to cherry-pick (in chronological order)
commits=(
    "caaa1f0879a0db70605f689ce51d974db976d4a7"  # switch to metrics interface (#13257)
    "c0dd9dae8e25028a7fc8ff0f92c8c02a8d699020"  # Make use of poller actor straight in http subsystem (#13316)
    # Skip f877bdb3481b8ec28d297a692b415c914a8f3140 - Refactor shared threads (#13980) - causes extensive conflicts
    "7f87055b80b505431a9d7d56f94d67c9e0523b5d"  # improve http tests and buffer handling (#17341)
    "d7474a11057e44774a4289068e1b667a71f25ce5"  # set of fixes/improvements for http proxy (#17998)
    "34fb4f1c1b5dc6d10bbd7b96f0ab80b2ecde70ba"  # add incoming streaming for http proxy (#18211)
    "f6bac8f821c032de5576e6bf76dd807e683dfc33"  # fix crash on double pass away (#19048)
    "f1bd766c37c092f6872ec5dc4f4847f55965dff3"  # improve debugging and accepted (#19425)
    "c90b6725149928afa954a287aaac570593ee66e8"  # improve http trace messages (#19865)
    "49be324ddfeb4b90e800285e0bbd98697b260c41"  # workaround for dumping bad requests with very long url (#20863)
    "4c46c82e6090f5da61de90a4b84a60e1c14f7234"  # fix incoming data chunk processing (#20929)
    "efeaf7e25bf6308ff97895d6b5eea12aaa5b1f17"  # work-around for empty list access (#21260)
    "23ba7c275e7fd4d7cf21daff62c8e5c9eb25580e"  # truncate long http debug messages (#21466)
)

# Set editor to avoid interactive prompts
export EDITOR=echo

failed_commits=()

for commit in "${commits[@]}"; do
    echo "Cherry-picking $commit..."
    if ! git cherry-pick "$commit"; then
        echo "Cherry-pick failed for $commit"
        failed_commits+=("$commit")
        git cherry-pick --abort
    fi
done

echo "Backport process completed!"
echo "Successfully cherry-picked $((${#commits[@]} - ${#failed_commits[@]})) out of ${#commits[@]} commits"

if [ ${#failed_commits[@]} -gt 0 ]; then
    echo "Failed commits (require manual resolution):"
    for failed in "${failed_commits[@]}"; do
        echo "  $failed"
    done
fi

echo "Push the branch and create a PR:"
echo "git push origin http-actors-backport-to-stable-25-1"