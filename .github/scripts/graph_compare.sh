
# Compares build graphs for two given refs in the current directory git repo
# Creates ya.make in the current directory listing affected ydb targets
# Parameters: base_commit_sha head_commit_sha

set -exo pipefail

if [[ -z "$YA_MAKE_COMMAND" ]]; then
    echo "YA_MAKE_COMMAND not set"
    exit 1
fi

if [[ -z "$workdir" ]]; then
    workdir=$(mktemp -d)
fi

echo Workdir: $workdir
echo Checkout base commit...
git checkout $1
echo Build graph for base commit...
$YA_MAKE_COMMAND ydb -k -A --cache-tests -Gj0 > $workdir/graph_base.json

echo Checkout head commit...
git checkout $2
echo Build graph for head commit...
$YA_MAKE_COMMAND ydb -k -A --cache-tests -Gj0 > $workdir/graph_head.json

echo Generate diff graph
./ya tool ygdiff --old $workdir/graph_base.json --new $workdir/graph_head.json --cut $workdir/graph_diff.json

echo Create ya.make
echo "" > ya.make

echo Generate list of test shard names from the diff graph
cat $workdir/graph_diff.json | jq -r '.graph[] | select( ."node-type"=="test" and ."kv"."path" != null ) | .kv.path' | sort | uniq > $workdir/testsuites

echo Number of test suites:
cat $workdir/testsuites | wc -l

echo Removing test suite name from the list to get target names...
sed -E 's/\/[^/]*$//g;/^null$/d' $workdir/testsuites > $workdir/ts2

echo Append into ya.make RECURSE_FOR_TESTS to all required tests...
cat $workdir/ts2 | (echo 'RECURSE_FOR_TESTS(';cat;echo ')') >> ya.make

echo Generate list of module names from the diff graph
cat $workdir/graph_diff.json | jq -r '.graph[].target_properties | select( ."module_type" != null and  (."module_dir" | startswith("ydb")) and ( ."module_tag" // "-" | strings | contains("proto") | not )) | .module_dir' | sort | uniq > $workdir/modules

echo Number of modules:
cat $workdir/modules | wc -l

echo Append into ya.make RECURSE to all required modules...
cat $workdir/modules | (echo 'RECURSE(';cat;echo ')') >> ya.make

echo "ya.make content:"
cat ya.make
