#!/bin/bash

GIT_URL=$1

set -e
declare -A top_dirs=(
    [ydb/]=1,
    [util/]=1,
    [build/]=1,
    [contrib/]=1,
    [certs/]=1,
    [cmake/]=1,
    [.git/]=1,
    [.github/]=1,
    [library/]=1,
    [tools/]=1,
    [scripts/]=1,
    [yt/]=1,
)

cd $GIT_URL

shopt -s dotglob
shopt -s nullglob
array=(*/)

for dir in "${array[@]}"
do
    if [[ ! ${top_dirs[$dir]} ]]
    then
        echo "$dir is not allowed root level directory."
        exit 1
    fi
done

