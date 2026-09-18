#!/usr/bin/env bash
set -euo pipefail
fixture_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source_dir=$(git -C "$fixture_dir" rev-parse --show-toplevel)
source_commit=$(git -C "$source_dir" rev-parse HEAD)
git -C "$source_dir" diff --exit-code HEAD -- .
if [[ -n $(git -C "$source_dir" ls-files --others --exclude-standard) ]]; then
    echo 'Commit the source and fixture before building the acceptance image.' >&2
    exit 1
fi
cd "$source_dir"
./ya make --build relwithdebinfo -D FORCE_VCS_INFO_UPDATE=yes ydb/apps/ydbd ydb/apps/ydb ydb/apps/dstool
build_context=$(mktemp -d -t block82-image.XXXXXXXX)
trap 'rm -rf -- "$build_context"' EXIT
mkdir -p "$build_context/bin" "$build_context/fixture" "$build_context/checks"
ydb/apps/ydbd/ydbd -V > "$build_context/fixture/build-info.txt"
if ! rg -Fq "Commit: $source_commit" "$build_context/fixture/build-info.txt"; then
    echo 'The binary VCS metadata does not match the committed source.' >&2
    exit 1
fi
strip --strip-debug -o "$build_context/bin/ydbd" ydb/apps/ydbd/ydbd
strip --strip-debug -o "$build_context/bin/ydb" ydb/apps/ydb/ydb
cp ydb/apps/dstool/ydb-dstool "$build_context/bin/"
cp "$fixture_dir/"*.py "$fixture_dir/config.yaml" "$build_context/fixture/"
cp "$fixture_dir/Dockerfile" "$build_context/"
cp "$fixture_dir/check-legacy-ui.js" ydb/core/viewer/content/viewer.js "$build_context/checks/"
image_tag="local/ydb-block82:$source_commit"
docker build --build-arg "SOURCE_COMMIT=$source_commit" --tag "$image_tag" "$build_context"
image_id=$(docker image inspect --format '{{.Id}}' "$image_tag")
printf 'BLOCK82_IMAGE=%s\nCOMPOSE_PROJECT_NAME=block82-stage4\n' "$image_id" > "$fixture_dir/.env"
printf 'Commit: %s\nImage tag: %s\nImage ID: %s\n' "$source_commit" "$image_tag" "$image_id"
