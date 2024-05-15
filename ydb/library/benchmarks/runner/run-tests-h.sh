#!/bin/sh -ex
: ${datasize=${1-1}}
: ${script_path=${0%/*}}
: ${ydb_path=$script_path/../../../..}
(cd ${script_path} && $ydb_path/ya make --build relwithdebinfo)
(cd ${ydb_path}/ydb/library/yql/tools/dqrun && $ydb_path/ya make --build relwithdebinfo)
(cd ${ydb_path}/ydb/library/yql/udfs/common && $ydb_path/ya make --build relwithdebinfo datetime datetime2 string re2 set)
(cd ${ydb_path}/ydb/library/benchmarks/gen_queries && $ydb_path/ya make --build relwithdebinfo)
[ -d tpc/h/$datasize ] ||
    ${ydb_path}/ydb/library/benchmarks/runner/download_files_h_${datasize}.sh
[ -f ql-$datasize/h/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output ql-$datasize --variant h --syntax yql --dataset-size $datasize
[ -f q-$datasize/h/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output q-$datasize --variant h --syntax yql --dataset-size $datasize \
    --pragma dq.MaxTasksPerStage=1 \
    --pragma dq.ComputeActorType="async" \
    --pragma config.flags=LLVM_OFF \
# --pragma dq.UseFinalizeByKey=true \
# --pragma dq.UseOOBTransport=true
[ -f qs-$datasize/h/bindings.json ] ||
${ydb_path}/ydb/library/benchmarks/gen_queries/gen_queries \
        --output qs-$datasize --variant h --syntax yql --dataset-size $datasize \
    --pragma dq.MaxTasksPerStage=1 \
    --pragma config.flags=LLVM_OFF \
    --pragma dq.ComputeActorType="async" \
    --pragma dq.UseFinalizeByKey=true \
# --pragma dq.UseOOBTransport=true
outdir=results-`date -u +%Y%m%dT%H%M%S`-$datasize
if false; then
echo LLVM && \
command time ${script_path}/runner/runner ql-$datasize/h ql-$datasize/bindings $outdir ${ydb_path}/ydb/library/yql/tools/dqrun/dqrun -s --fs-cfg ${ydb_path}/ydb/library/yql/tools/dqrun/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/
fi
echo NO LLVM && \
command time ${script_path}/runner/runner q-$datasize/h q-$datasize/h/bindings.json $outdir ${ydb_path}/ydb/library/yql/tools/dqrun/dqrun -s --fs-cfg ${ydb_path}/ydb/library/yql/tools/dqrun/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/
echo Spilling && \
command time ${script_path}/runner/runner qs-$datasize/h qs-$datasize/h/bindings.json $outdir ${ydb_path}/ydb/library/yql/tools/dqrun/dqrun -s --enable-spilling --fs-cfg ${ydb_path}/ydb/library/yql/tools/dqrun/examples/fs.conf --gateways-cfg $script_path/runner/test-gateways.conf --udfs-dir ${ydb_path}/ydb/library/yql/udfs/common/
