set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

ARC_ROOT=$(pwd)
GIT_ROOT=~/gh

SRC_BRANCH=$1
if [ -z "$SRC_BRANCH" ]; then
    echo "Using main as default git branch"
    SRC_BRANCH=main
fi

TRG_BRANCH=$2
if [ -z "$TRG_BRANCH" ]; then
    echo "Using current branch as default arcadia branch"
    summary_mask="YDB Import"
else
    summary_mask="YDB Release import to $TRG_BRANCH"
fi

if [ -z "$FORCE_IMPORT" ]; then
    OPEN_PRS=$(arc pr list --json | grep -E '"summary"\s*:\s*"'"$summary_mask" || true )
    if [ -n "$OPEN_PRS" ]; then
        echo "Open/draft PR(s) exist with Summary mask '$summary_mask':"
        echo "$OPEN_PRS"
        exit 1
    fi
fi

if [ -z "$SKIP_GIT_CLONE" ] ; then	
    rm -rf "$GIT_ROOT"
    mkdir -p "$GIT_ROOT"
    git clone -b $SRC_BRANCH --depth 1 https://github.com/ydb-platform/ydb.git "$GIT_ROOT"
fi

if [ -n "$TRG_BRANCH" ]; then
    (cd $ARC_ROOT && arc checkout $TRG_BRANCH && arc pull)
fi

OLDITER=$(cat $ARC_ROOT/contrib/ydb/import_iteration.txt)
NEWITER=$(($OLDITER + 1))

GIT_SHA=$(git --git-dir="$GIT_ROOT/.git" --work-tree="$GIT_ROOT" rev-parse HEAD)
echo "Git commit SHA: $GIT_SHA"

OLD_GIT_SHA=$(cat $ARC_ROOT/contrib/ydb/github_sha.txt)
echo "Previous git commit SHA: $OLD_GIT_SHA"

GIT_YDB=$GIT_ROOT/ydb
source $SCRIPT_DIR/patch_cmakes.sh $GIT_YDB
source $SCRIPT_DIR/patch_ymakes.sh $GIT_YDB
source $SCRIPT_DIR/patch_sources.sh $GIT_YDB
source $SCRIPT_DIR/patch_py_sources.sh $GIT_YDB
source $SCRIPT_DIR/patch_protos.sh $GIT_YDB
source $SCRIPT_DIR/patch_configs.sh $GIT_YDB

LOC_NEW='contrib/ydb'
echo "RSync..."
rsync -c -I -W -r --delete --filter '- **/CMakeList*.txt' --filter '- clang.toolchain' --filter '- **/a.yaml' $GIT_YDB/ $ARC_ROOT/$LOC_NEW
echo "RSync completed"

echo $NEWITER > $ARC_ROOT/contrib/ydb/import_iteration.txt
echo $GIT_SHA > $ARC_ROOT/contrib/ydb/github_sha.txt

arc status
arc commit --all -m "Github changes"

if [ -z "$TRG_BRANCH" ]; then
    # Apply arc patches needed to yql/scripts/build_artifacts.py. Only for trunk
    for hash in $(cat $ARC_ROOT/yql/scripts/arc_stable/gh_patches.txt | cut -f 1 -d ' ');
    do
        echo Applying $hash
        ERROR=$(arc cherry-pick $hash 2>&1 >/dev/null || echo cherry-pick-fail)
        if [ -n "$ERROR" ]; then
            if echo $ERROR | grep -vq 'are already present' ; then
                echo $ERROR
                exit 1
            fi
        fi
    done
else
    if [ -d $GIT_ROOT/yql ]; then
        echo "Copying whole yql directory"
        cp -r $GIT_ROOT/yql $ARC_ROOT
    fi
fi

if [ -n "$ARC_SUBMIT" ]; then
    # Specify pool to make checks faster, see https://st.yandex-team.ru/DEVTOOLSSUPPORT-50329
    label=autocheck/pool/autocheck/precommits/meta_search/meta_infra/kikimr

    if [ -z "$TRG_BRANCH" ]; then
        TRG_BRANCH_OPT=""
    else
        TRG_BRANCH_OPT="--to $TRG_BRANCH"
    fi

    prlink=$(arc submit --publish --all --auto $TRG_BRANCH_OPT -m "$summary_mask $NEWITER" --label "$label" --no-verify)    
    echo $prlink
    echo '{ "url": "'"$prlink"'", "module": "KIKIMR", "text": "PR to contrib/ydb" }' > $RESULT_BADGES_PATH/file1

    pr_id=$(basename "$prlink")
    echo -n "Authorization: OAuth " > auth_header.txt
    arc token show >> auth_header.txt
    # share PR for fixes
    # it is actually illegal to use, see https://st.yandex-team.ru/DEVTOOLSSUPPORT-44186#660e7ef6c1bcf14cf4f96023
    curl --retry-delay 5 --retry 5 -H @auth_header.txt -H "Content-Type: application/json" "https://a.yandex-team.ru/gateway/root/arcanum/SharePR" --data "{\"prId\": \"$pr_id\", \"shareType\": \"for_all\"}"
    rm auth_header.txt
fi
