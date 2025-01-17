#!/bin/bash
set -eux

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
    GIT_ROOT=$(cd $FROM; git rev-parse --show-toplevel)
    HEAD=$(cd $FROM; git rev-parse HEAD)
    # Normalize FROM relativly to the git root
    FROM=${FROM#$GIT_ROOT/}
    echo "$HEAD;$FROM" > $TARGET_INFO_FILE
    echo "Filled in $TARGET_INFO_FILE"
    exit
fi

IFS=';' read -r BASE_REV INFO_FROM < "$TARGET_INFO_FILE"
TO=$(dirname $(realpath "$TARGET_INFO_FILE"))
HEAD_REV=$(cd $TO; git rev-parse HEAD)
GIT_ROOT=$(cd $TO; git rev-parse --show-toplevel)
FROM="$GIT_ROOT/$INFO_FROM"

echo "Base revision: $BASE_REV"
echo "Head revision: $HEAD_REV"
echo "Git root: $GIT_ROOT"
echo "Source: $FROM"
echo "Target: $TO"

if [ "$(cd $TO; git status -s -u | wc -l)" != "0" ]; then
    echo "Target $TO has uncommited changes" >&2
    exit
fi

cd $GIT_ROOT

CURRENT_BRANCH=$(git branch --show-current)
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
git checkout -b $BRANCH $BASE_REV
rsync -r --delete --filter='. -' -v $TO/ $FROM << EOF
+ /*/
+ *.md
+ toc_*.yaml
- /*
EOF

git add -A $INFO_FROM

git diff --cached --binary --relative=$INFO_FROM > $PATCH_FILE
git reset --hard
git clean -d -f
git status
git checkout "$CURRENT_BRANCH"
git branch -D $BRANCH
rsync -r --delete --filter='. -' -v $FROM/ $TO << EOF
+ /*/
+ *.md
+ toc_*.yaml
- /*
EOF

patch -d $TO -p1 -N -E --no-backup-if-mismatch --merge -i $PATCH_FILE -t
#git apply --whitespace=nowarn --directory=$TO --reject $PATCH_FILE

echo "$HEAD_REV;$INFO_FROM" > $TARGET_INFO_FILE
