
# Compares build graphs for two given refs in the current directory git repo
# Creates ya.make in the current directory listing affected ydb targets
# Parameters: base_commit_sha head_commit_sha

set -ex

workdir=$(mktemp -d)
echo Workdir: $workdir
echo Checkout base commit...
git checkout $1
echo Build graph for base commit...
./ya make -Gj0 -ttt ydb --build release -k --cache-tests --build-all | jq '.graph[]' > $workdir/graph_base

echo Checkout head commit...
git checkout $2
echo Build graph for head commit...
./ya make -Gj0 -ttt ydb --build release -k --cache-tests --build-all | jq '.graph[]' > $workdir/graph_head

echo Generate lists of uids for base and head...
cat $workdir/graph_base | jq '.uid' > $workdir/uid_base
cat $workdir/graph_head | jq '.uid' > $workdir/uid_head

echo Create a list of changed uids in the head graph...
(cat $workdir/uid_head;(cat $workdir/uid_base;cat $workdir/uid_head) | sort | uniq -u) | sort | uniq -d > $workdir/uids_new

echo Create ya.make
echo "" > ya.make

echo Generate list of test shard names from the head graph based on the list of uids...
cat $workdir/graph_head | jq -r --slurpfile uids $workdir/uids_new 'select( ."node-type"=="test") | select( any( .uid; .==$uids[] )) | .kv.path' | sort | uniq > $workdir/testsuites

echo Number of test suites:
cat $workdir/testsuites | wc -l

echo Removing test suite name from the list to get target names...
sed -E 's/\/[^/]*$//g;/^null$/d' $workdir/testsuites > $workdir/ts2

echo Append into ya.make RECURSE_FOR_TESTS to all required tests...
cat $workdir/ts2 | (echo 'RECURSE_FOR_TESTS(';cat;echo ')') >> ya.make

echo Generate list of module names from the head graph based on the list of uids...
cat $workdir/graph_head | jq -r --slurpfile uids $workdir/uids_new 'select( ."target_properties"."module_type" != null) | select( ( ."target_properties"."module_tag" // "-" | strings | contains("proto") ) | not ) | select( any( .uid; .==$uids[] )) | .target_properties.module_dir' | sort | uniq > $workdir/modules

echo Number of modules:
cat $workdir/modules | wc -l

echo Filter only modules in ydb
cat $workdir/modules | { grep "^ydb" || true; } > $workdir/modules2

echo Number of modules:
cat $workdir/modules2 | wc -l

echo Append into ya.make RECURSE to all required modules...
cat $workdir/modules2 | (echo 'RECURSE(';cat;echo ')') >> ya.make

echo "ya.make content:"
cat ya.make
