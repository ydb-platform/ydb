#!/bin/bash
set -eu

TARGET_INFO_FILE="$(realpath $1)"

if [ ! -f $TARGET_INFO_FILE ]; then
    echo "File $TARGET_INFO_FILE doesn't exist. Creating new one"

    if [ "$2" == "" ]; then
        echo "Expected source folder as the second argument" >&2
        exit
    fi

    if [ ! -d "$2" ]; then
        echo "Expected $2 to be a directory" >&2
        exit
    fi

    FROM=$(realpath $2)
    ARC_ROOT=$(cd $FROM; arc root)
    HEAD=$(cd $FROM; arc rev-parse HEAD)
    # Normalize FROM relativly to the arc root
    FROM=${FROM#$ARC_ROOT/}
    echo "$HEAD;$FROM" > $TARGET_INFO_FILE
    echo "Filled in $TARGET_INFO_FILE"
    exit
fi

IFS=';' read -r BASE_REV INFO_FROM < "$TARGET_INFO_FILE"
TO=$(dirname $(realpath "$TARGET_INFO_FILE"))
HEAD_REV=$(cd $TO; arc rev-parse HEAD)
ARC_ROOT=$(cd $TO; arc root)
FROM="$ARC_ROOT/$INFO_FROM"

echo "Base revision: $BASE_REV"
echo "Head revision: $HEAD_REV"
echo "Arc root: $ARC_ROOT"
echo "Source: $FROM"
echo "Target: $TO"

if [ "$(cd $TO; arc status -s -u all | wc -l)" != "0" ]; then
    echo "Target $TO has uncommited changes" >&2
    exit
fi

cd $ARC_ROOT

CURRENT_BRANCH=$(arc info | grep 'branch:')
CURRENT_BRANCH=${CURRENT_BRANCH#branch: }
PATCH_FILE=$(mktemp)
BRANCH=upgrade-$(date '+%Y-%m-%d-%H-%M-%S')

clean_up () {
    ARG=$?
    echo "Deleting patch file"
    rm $PATCH_FILE
    exit $ARG
}
trap clean_up EXIT

echo "Use $BRANCH temporary branch, $PATCH_FILE patch file"
arc co $BASE_REV -b $BRANCH
rsync -r --delete --filter='. -' -v $TO/ $FROM << EOF
+ /*/
+ *.md
+ toc_*.yaml
- /*
EOF

arc add -A $INFO_FROM

arc diff --cached --relative=$INFO_FROM > $PATCH_FILE
arc reset
arc co $INFO_FROM
arc clean -d
arc st
arc co "$CURRENT_BRANCH"
arc br -D $BRANCH
rsync -r --delete --filter='. -' -v $FROM/ $TO << EOF
+ /*/
+ *.md
+ toc_*.yaml
- /*
EOF

patch -d $TO -p0 -N -E --no-backup-if-mismatch --merge -i $PATCH_FILE -t

echo "$HEAD_REV;$INFO_FROM" > $TARGET_INFO_FILE
