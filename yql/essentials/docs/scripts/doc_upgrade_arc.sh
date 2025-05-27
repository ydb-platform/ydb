#!/bin/bash
set -eu

TARGET_INFO_FILE="$1"

if [ ! -f $TARGET_INFO_FILE ]; then
    echo "File $TARGET_INFO_FILE doesn't exist. Creating new one"

    if [ "$2" == "" ]; then
        echo "Expected source folder as the second argument" >&2
        exit 1
    fi

    if [ ! -d "$2" ]; then
        echo "Expected $2 to be a directory" >&2
        exit 1
    fi

    FROM=$(realpath $2)
    ARC_ROOT=$(cd $FROM; arc root)
    HEAD=$(cd $FROM; arc rev-parse HEAD)
    # Normalize FROM relativly to the arc root
    ARC_FROM=${FROM#$ARC_ROOT/}
    echo "$HEAD;$ARC_FROM" > $TARGET_INFO_FILE
    echo "Filled in $TARGET_INFO_FILE"
    exit
fi

TARGET_INFO_FILE="$(realpath $TARGET_INFO_FILE)"
IFS=';' read -r BASE_REV ARC_FROM < "$TARGET_INFO_FILE"
TO=$(dirname $(realpath "$TARGET_INFO_FILE"))
HEAD_REV=$(cd $TO; arc rev-parse HEAD)
ARC_ROOT=$(cd $TO; arc root)
FROM="$ARC_ROOT/$ARC_FROM"
ARC_TO=${TO#$ARC_ROOT/}
DATETIME=$(date '+%Y-%m-%d-%H-%M-%S')

echo "Base revision: $BASE_REV"
echo "Head revision: $HEAD_REV"
echo "Arc root: $ARC_ROOT"
echo "Source: $ARC_FROM"
echo "Target: $ARC_TO"

if [ "$(cd $TO; arc status -s -u all | wc -l)" != "0" ]; then
    echo "Target $TO has uncommited changes" >&2
    exit
fi

cd $ARC_ROOT

PATCH_FILE=$(mktemp)
EXPORT_DIR=$(mktemp -d)

echo "Use $EXPORT_DIR export dir"
arc export $BASE_REV "$ARC_FROM" --to "$EXPORT_DIR"
rsync -r --delete --filter='. -' "$EXPORT_DIR/$ARC_FROM/" "$TO" << EOF
+ /*/
+ *.md
+ toc_*.yaml
- /*
EOF

rm -rf "$EXPORT_DIR" || true

arc add -A $ARC_TO

arc diff --cached --reverse --relative=$ARC_TO > $PATCH_FILE
arc reset --hard
arc clean -d
arc status

rsync -r --delete --filter='. -' "$FROM/" "$TO" << EOF
+ /*/
+ *.md
+ toc_*.yaml
P _assets/*
- /*
EOF

patch -d "$TO" -p0 -N -E --no-backup-if-mismatch --merge -i $PATCH_FILE -t || echo "Patch has conflicts. Consider to review them before commit"

if [ "$(cd $TO; arc status -s -u all | wc -l)" != "0" ]; then
    echo "$HEAD_REV;$ARC_FROM" > $TARGET_INFO_FILE
else
    echo "Nothing changed"
fi

if [ -v KEEP_PATCH ]; then
    mv "$PATCH_FILE" "$TO/$DATETIME.patch"
else
    rm "$PATCH_FILE"
fi
