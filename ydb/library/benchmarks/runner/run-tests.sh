#!/bin/sh -ex
: ${datasize=${1:-1}}
: ${variant=${2:-h}}
: ${script_path=${0%/*}}
: ${ydb_path=$script_path/../../../..}
(cd ${script_path} && $ydb_path/ya make --build relwithdebinfo)
(cd ${ydb_path}/ydb/library/yql/tools/dqrun && $ydb_path/ya make --build relwithdebinfo)
(cd ${ydb_path}/ydb/library/yql/udfs/common && $ydb_path/ya make --build relwithdebinfo datetime datetime2 string re2 set math unicode_base)
(cd ${ydb_path}/ydb/library/benchmarks/gen_queries && $ydb_path/ya make --build relwithdebinfo)
[ -d tpc/$variant/$datasize ] ||
    ${ydb_path}/ydb/library/benchmarks/runner/download_files_${variant}_${datasize}.sh
if [ x$variant = xds ]; then
    xpragma="--pragma AnsiOptionalAs"
else
    xpragma=""
fi
[ -f ql-$datasize/$variant/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output ql-$datasize --variant ${variant} --syntax yql --dataset-size $datasize \
        $xpragma \
        #
[ -f q-$datasize/$variant/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output q-$datasize --variant ${variant} --syntax yql --dataset-size $datasize \
    --pragma dq.MaxTasksPerStage=1 \
    --pragma dq.ComputeActorType="async" \
    --pragma config.flags=LLVM_OFF \
    $xpragma \
# --pragma dq.UseFinalizeByKey=true \
# --pragma dq.UseOOBTransport=true \
#
[ -f qs-$datasize/$variant/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output qs-$datasize --variant ${variant} --syntax yql --dataset-size $datasize \
    --pragma dq.MaxTasksPerStage=1 \
    --pragma config.flags=LLVM_OFF \
    --pragma dq.ComputeActorType="async" \
    --pragma dq.UseFinalizeByKey=true \
    $xpragma \
# --pragma dq.UseOOBTransport=true \
#
outdir=results-`date -u +%Y%m%dT%H%M%S`-$datasize
if false; then
echo LLVM && \
command time ${script_path}/runner/runner ql-$datasize/${variant} ql-$datasize/bindings.json $outdir ${ydb_path}/ydb/library/yql/tools/dqrun/dqrun -s --fs-cfg ${ydb_path}/ydb/library/yql/tools/dqrun/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/
fi
echo NO LLVM && \
command time ${script_path}/runner/runner q-$datasize/${variant} q-$datasize/${variant}/bindings.json $outdir ${ydb_path}/ydb/library/yql/tools/dqrun/dqrun -s --fs-cfg ${ydb_path}/ydb/library/yql/tools/dqrun/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/
echo Spilling && \
command time ${script_path}/runner/runner qs-$datasize/${variant} qs-$datasize/${variant}/bindings.json $outdir ${ydb_path}/ydb/library/yql/tools/dqrun/dqrun -s --enable-spilling --fs-cfg ${ydb_path}/ydb/library/yql/tools/dqrun/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/
