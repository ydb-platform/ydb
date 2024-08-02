#!/bin/sh -e
if [ "X$1" = "X--help" ]; then
    echo "Usage: $0 [--perf] [datasize [variant [tasks]]]"
    echo "Also can be specified by envirnoment variables: "
    echo '  $datasize     1, 10, 100; 1 is default'
    echo '  $variant      h or ds; h is default'
    echo '  $tasks        1 is default'
    echo '  $syntax       yql or pg; yql is default'
    echo '  $script_path  default is deduced from argv0'
    echo '  $ydb_path     default is $script_path/../../../..'
    echo '  $rebuild      0 [default] or build type (relwithdebinfo [default if empty]|release|profile); requires clean git repository and presence of {current branch name}-base tag'
    echo 'When $rebuild is 0, requires pre-built $ydb_path/ydb/library/yql/udfs, $ydb_path/ydb/library/yql/tools/dqrun/dqrun-unspilled and $ydb_path/ydb/library/yql/tools/dqrun/dqrun'
    echo '  $decimal      setting to empty string disables DECIMAL support requirement'
    echo '  $cbo          setting to empty string disables CostBasedOptimizer support requirement'
    echo '  $runner_opts  extra options passed to runner'
    echo '  $dqrun_opts   extra options passed to dqrun'
    echo '  $enable_spilling'
    echo '                setting to empty string allows compare runs without --enable-spilling'
    exit 0
fi
if [ "X$1" = "X--perf" ]; then
    perf="$1"
    shift
fi
: ${datasize=${1:-1}}
: ${variant=${2:-h}}
: ${tasks=${3:-1}}
: ${script_path=${0%/*}}
: ${ydb_path=$script_path/../../../..}
: ${enable_spilling=--enable-spilling}
: ${decimal=--decimal=True}
: ${cbo=--pragma=CostBasedOptimizer=native}
if [ -w /proc/self/oom_score_adj ]; then
    # tests sometimes run into OOM; mark ourself as preferred victim
    echo 500 > /proc/self/oom_score_adj
fi
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
    (cd ${dq_path} && $ya_path/ya make --build ${rebuild:-relwithdebinfo} && git describe --always --dirty >$dq_path/dqrun-unspilled.commit && cp -L $dq_path/dqrun $dq_path/dqrun-unspilled)
    git checkout @{-1}
    (cd ${dq_path} && $ya_path/ya make --build ${rebuild:-relwithdebinfo})
    (cd ${ydb_path}/ydb/library/yql/udfs/common && $ya_path/ya make --build ${rebuild:-relwithdebinfo} datetime datetime2 string re2 set math unicode_base)
    (cd ${ydb_path}/ydb/library/benchmarks/gen_queries && $ya_path/ya make --build ${rebuild:-relwithdebinfo})
    )
    (cd ${script_path} && ${script_path}/../../../../ya make --build ${rebuild:-relwithdebinfo})
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

qs=2spilling${syntax+-$syntax}
ql=3llvm-on${syntax+-$syntax}
qX=1main${syntax+-$syntax}
q=1main-no-enable-spilling${syntax+-$syntax}
qL=4main+llvm${syntax+-$syntax}
qsL=5spilling+llvm${syntax+-$syntax}

[ -f ${ql}-${datasize}-$tasks/$variant/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output ${ql}-${datasize}-$tasks --variant ${variant} --syntax ${syntax-yql} --dataset-size $datasize \
        $xpragma \
        $cbo \
        #

[ -f ${qs}-${datasize}-$tasks/$variant/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output ${qs}-${datasize}-$tasks --variant ${variant} --syntax ${syntax-yql} --dataset-size $datasize \
        $decimal \
    --pragma dq.MaxTasksPerStage=$tasks \
    --pragma config.flags=LLVM_OFF \
    --pragma dq.ComputeActorType="async" \
    --pragma dq.UseFinalizeByKey=true \
    --pragma dq.EnableSpillingNodes=All \
    $cbo \
    $xpragma \
    #
[ -e ${qX}-${datasize}-$tasks ] || ln -s ${qs}-${datasize}-$tasks ${qX}-${datasize}-$tasks
[ -e ${q}-${datasize}-$tasks ] || ln -s ${qs}-${datasize}-$tasks ${q}-${datasize}-$tasks
#
[ -f ${qsL}-${datasize}-$tasks/$variant/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output ${qsL}-${datasize}-$tasks --variant ${variant} --syntax ${syntax-yql} --dataset-size $datasize \
        $decimal \
    --pragma dq.MaxTasksPerStage=$tasks \
    --pragma dq.ComputeActorType="async" \
    --pragma dq.UseFinalizeByKey=true \
    --pragma dq.OptLLVM=ON \
    --pragma dq.EnableSpillingNodes=GraceJoin \
    $cbo \
    $xpragma \
#
[ -e ${qL}-${datasize}-$tasks ] || ln -s ${qsL}-${datasize}-$tasks ${qL}-${datasize}-$tasks

outdir=results-`date -u +%Y%m%dT%H%M%S`-${variant}-${datasize}-$tasks
if false; then
start="`cd ${dq_path} && git describe --always --dirty`"
echo LLVM && \
command time ${script_path}/runner/runner $perf $runner_opts --query-dir ${ql}-${datasize}-$tasks/${variant} --bindings ${ql}-${datasize}-$tasks/bindings.json --result-dir $outdir ${dq_path}/dqrun-unspilled -s --fs-cfg ${dq_path}/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/ $dqrun_opts
(echo $start; cd ${dq_path} && git describe --always --dirty) > $outdir/${ql}-${datasize}-$tasks/${variant}/commit
fi
if false; then
echo main NO LLVM && \
command time ${script_path}/runner/runner $perf $runner_opts --query-dir ${qX}-${datasize}-$tasks/${variant} --bindings ${qX}-${datasize}-$tasks/${variant}/bindings.json --result-dir $outdir ${dq_path}/dqrun-unspilled ${enable_spilling} -s --fs-cfg ${dq_path}/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/ $dqrun_opts
(cd ${dq_path}; cat dqrun-unspilled.commit) > $outdir/${qX}-${datasize}-$tasks/${variant}/commit
fi
if [ -z "${skipllvm+set}" ]; then
start="`cd ${dq_path} && git describe --always --dirty`"
echo Spilling+LLVM && \
command time ${script_path}/runner/runner $perf $runner_opts --query-dir ${qsL}-${datasize}-$tasks/${variant} --bindings ${qsL}-${datasize}-$tasks/${variant}/bindings.json --result-dir $outdir ${dq_path}/dqrun$dqsuffix -s ${enable_spilling} --fs-cfg ${dq_path}/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/ $dqrun_opts
(echo $start; cd ${dq_path} && git describe --always --dirty) > $outdir/${qsL}-${datasize}-$tasks/${variant}/commit
if [ -z "${skipmain+set}" ]; then
echo main+LLVM no enable spilling && \
command time ${script_path}/runner/runner $perf $runner_opts --query-dir ${qL}-${datasize}-$tasks/${variant} --bindings ${qL}-${datasize}-$tasks/${variant}/bindings.json --result-dir $outdir ${dq_path}/dqrun-unspilled -s --fs-cfg ${dq_path}/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/ $dqrun_opts
(cd ${dq_path}; cat dqrun-unspilled.commit) > $outdir/${qL}-${datasize}-$tasks/${variant}/commit
fi
fi
if [ -z "${skipnollvm+set}" ]; then
start="`cd ${dq_path} && git describe --always --dirty`"
echo Spilling NO LLVM && \
command time ${script_path}/runner/runner $perf $runner_opts --query-dir ${qs}-${datasize}-$tasks/${variant} --bindings ${qs}-${datasize}-$tasks/${variant}/bindings.json --result-dir $outdir ${dq_path}/dqrun$dqsuffix -s ${enable_spilling} --fs-cfg ${dq_path}/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/ $dqrun_opts
(echo $start; cd ${dq_path} && git describe --always --dirty) > $outdir/${qs}-${datasize}-$tasks/${variant}/commit
if [ -z "${skipmain+set}" ]; then
echo main NO LLVM no enable spilling && \
command time ${script_path}/runner/runner $perf $runner_opts --query-dir ${q}-${datasize}-$tasks/${variant} --bindings ${q}-${datasize}-$tasks/${variant}/bindings.json --result-dir $outdir ${dq_path}/dqrun-unspilled -s --fs-cfg ${dq_path}/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/ $dqrun_opts
(cd ${dq_path}; cat dqrun-unspilled.commit) > $outdir/${q}-${datasize}-$tasks/${variant}/commit
fi
fi
