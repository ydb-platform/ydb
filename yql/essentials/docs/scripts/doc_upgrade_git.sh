#!/bin/bash
set -eu

TARGET_INFO_FILE="$1"

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
    GIT_FROM=${FROM#$GIT_ROOT/}
    echo "$HEAD;$GIT_FROM" > $TARGET_INFO_FILE
    echo "Filled in $TARGET_INFO_FILE"
    exit
fi

TARGET_INFO_FILE="$(realpath $TARGET_INFO_FILE)"
IFS=';' read -r BASE_REV GIT_FROM < "$TARGET_INFO_FILE"
TO=$(dirname $(realpath "$TARGET_INFO_FILE"))
HEAD_REV=$(cd $TO; git rev-parse HEAD)
GIT_ROOT=$(cd $TO; git rev-parse --show-toplevel)
FROM="$GIT_ROOT/$GIT_FROM"
GIT_TO=${TO#$GIT_ROOT/}
DATETIME=$(date '+%Y-%m-%d-%H-%M-%S')

echo "Base revision: $BASE_REV"
echo "Head revision: $HEAD_REV"
echo "Git root: $GIT_ROOT"
echo "Source: $GIT_FROM"
echo "Target: $GIT_TO"

if [ "$(cd $TO; git status -s -u | wc -l)" != "0" ]; then
    echo "Target $TO has uncommited changes" >&2
    exit
fi

cd $GIT_ROOT

PATCH_FILE=$(mktemp)
EXPORT_DIR=$(mktemp -d)

echo "Use $EXPORT_DIR export dir"
git clone --no-hardlinks "$GIT_ROOT/.git" "$EXPORT_DIR"
(cd $EXPORT_DIR; git reset --hard $BASE_REV)

rsync -r --delete --filter='. -' "$EXPORT_DIR/$GIT_FROM/" "$TO" << EOF
+ /*/
+ *.md
+ toc_*.yaml
- /*
EOF

rm -rf "$EXPORT_DIR" || true

git add -A $GIT_TO

git diff --cached --binary -R --relative=$GIT_TO > $PATCH_FILE
git reset --hard
git clean -d -f
git status

rsync -r --delete --filter='. -' "$FROM/" "$TO" << EOF
+ /*/
+ *.md
+ toc_*.yaml
P _assets/*
- /*
EOF

patch -d "$TO" -p1 -N -E --no-backup-if-mismatch --merge -i $PATCH_FILE -t || echo "Patch has conflicts. Consider to review them before commit"

if [ "$(cd $TO; git status -s -u | wc -l)" != "0" ]; then
    echo "$HEAD_REV;$GIT_FROM" > $TARGET_INFO_FILE
else
    echo "Nothing changed"
fi

if [ -v KEEP_PATCH ]; then
    mv "$PATCH_FILE" "$TO/$DATETIME.patch"
else
    rm "$PATCH_FILE"
fi
