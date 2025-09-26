
# Compares build graphs for two given refs in the current directory git repo
# Creates ya.make in the current directory listing affected ydb targets
# Parameters: base_commit_sha head_commit_sha

set -exo pipefail

workdir=$(mktemp -d)
echo Workdir: $workdir
echo Checkout base commit...
git checkout $1
echo Build graph for base commit...
./ya make -Gj0 -ttt ydb --build release -k --cache-tests --build-all > $workdir/graph_base

echo Checkout head commit...
git checkout $2
echo Build graph for head commit...
./ya make -Gj0 -ttt ydb --build release -k --cache-tests --build-all > $workdir/graph_head

echo Build graph for changed nodes...
./ya tool ygdiff --old $workdir/graph_base --new $workdir/graph_head --cut $workdir/graph_diff

echo $workdir/graph_diff
