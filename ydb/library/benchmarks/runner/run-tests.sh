#!/bin/sh -e
if [ "X$1" = "X--help" ]; then
    echo "Usage: $0 [datasize [variant [tasks]]]"
    echo "Also can be specified by envirnoment variables: "
    echo '  $datasize     1, 10, 100; 1 is default]'
    echo '  $variant      h or ds; h is default'
    echo '  $tasks        1 is default'
    echo '  $script_path  default is deduced from argv0'
    echo '  $ydb_path     default is $script_path/../../../..'
    echo '  $rebuild      0 [default] or 1; requires clean git repository and presence of {current branch}-base tag'
    echo 'When $rebuild is 0, requires pre-built $ydb_path/ydb/library/yql/udfs, $ydb_path/ydb/library/yql/tools/dqrun/dqrun and $ydb_path/ydb/library/yql/tools/dqrun/dqrun'
    exit 0
fi
: ${datasize=${1:-1}}
: ${variant=${2:-h}}
: ${tasks=${3:-1}}
: ${script_path=${0%/*}}
: ${ydb_path=$script_path/../../../..}
set -x
dq_path=${ydb_path}/ydb/library/yql/tools/dqrun
if [ -z "${ya_path}" ]; then
    if [ -e $ydb_path/ya ]; then
        ya_path=$ydb_path
    else
        ya_path=$ydb_path/..
    fi
fi
if [ "X${rebuild-0}" != X0 ]; then
    (
    cd $ydb_path
    branch=`git branch --show-current`
    base=`git describe --always ${branch}-base`
    git checkout $base
    (cd ${dq_path} && $ya_path/ya make --build relwithdebinfo && cp -L $dq_path/dqrun $dq_path/dqrun-unspilled)
    git checkout @{-1}
    (cd ${dq_path} && $ya_path/ya make --build relwithdebinfo)
    (cd ${ydb_path}/ydb/library/yql/udfs/common && $ya_path/ya make --build relwithdebinfo datetime datetime2 string re2 set math unicode_base)
    (cd ${ydb_path}/ydb/library/benchmarks/gen_queries && $ya_path/ya make --build relwithdebinfo)
    )
    (cd ${script_path} && ${script_path}/../../../../ya make --build relwithdebinfo)
fi
[ -d tpc/$variant/$datasize ] || {
    echo FIXME; false
    ${ydb_path}/ydb/library/benchmarks/runner/download_files_${variant}_${datasize}.sh
}
if [ x$variant = xds ]; then
    xpragma="--pragma AnsiOptionalAs=1"
else
    xpragma=""
fi

qs=2spilling
ql=3llvm-on
qX=1main
q=1main-no-enable-spilling

[ -f ${ql}-${datasize}-$tasks/$variant/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output ${ql}-${datasize}-$tasks --variant ${variant} --syntax yql --dataset-size $datasize \
        $xpragma \
        #

[ -f ${qs}-${datasize}-$tasks/$variant/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output ${qs}-${datasize}-$tasks --variant ${variant} --syntax yql --dataset-size $datasize \
    --pragma dq.MaxTasksPerStage=$tasks \
    --pragma config.flags=LLVM_OFF \
    --pragma dq.ComputeActorType="async" \
    --pragma dq.UseFinalizeByKey=true \
    $xpragma \
# --pragma dq.UseOOBTransport=true \
#
[ -e ${qX}-${datasize}-$tasks ] || ln -s ${qs}-${datasize}-$tasks ${qX}-${datasize}-$tasks
[ -e ${q}-${datasize}-$tasks ] || ln -s ${qs}-${datasize}-$tasks ${q}-${datasize}-$tasks

outdir=results-`date -u +%Y%m%dT%H%M%S`-${variant}-${datasize}-$tasks
if false; then
echo LLVM && \
command time ${script_path}/runner/runner ${ql}-${datasize}-$tasks/${variant} ${ql}-${datasize}-$tasks/bindings.json $outdir ${dq_path}/dqrun-unspilled -s --fs-cfg ${dq_path}/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/
fi
if false; then
echo main NO LLVM && \
command time ${script_path}/runner/runner ${qX}-${datasize}-$tasks/${variant} ${qX}-${datasize}-$tasks/${variant}/bindings.json $outdir ${dq_path}/dqrun-unspilled --enable-spilling -s --fs-cfg ${dq_path}/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/
fi
echo Spilling && \
command time ${script_path}/runner/runner ${qs}-${datasize}-$tasks/${variant} ${qs}-${datasize}-$tasks/${variant}/bindings.json $outdir ${dq_path}/dqrun -s --enable-spilling --fs-cfg ${dq_path}/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/
echo main NO LLVM no enable spilling && \
command time ${script_path}/runner/runner ${q}-${datasize}-$tasks/${variant} ${q}-${datasize}-$tasks/${variant}/bindings.json $outdir ${dq_path}/dqrun-unspilled -s --fs-cfg ${dq_path}/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/
