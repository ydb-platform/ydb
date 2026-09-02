#include "factories.h"

#include <ydb/library/yql/dq/comp_nodes/dq_block_hash_join.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_combine.h>
#include <ydb/library/yql/dq/comp_nodes/dq_scalar_hash_join.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_memory_quota.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetPerfTestFactory(TComputationNodeFactory customFactory) {
    // Build an effectively unlimited quota so Y_ENSURE(computeCtx.MemoryQuota) passes.
    // TDqMemoryQuota with initialMkqlMemoryLimit=0 calls AllocateQuota(0,false) which
    // trivially succeeds; the null counter is guarded by if(MkqlMemoryQuota) inside.
    auto quotaMgr = std::make_shared<NYql::NDq::TGuaranteeQuotaManager>(Max<ui64>() / 2, (ui64)0);

    // Explicit aggregate init through OutputChunkMaxSize (field 7 of 12).
    // Providing an explicit initializer for that member suppresses the default member
    // initializer  "= GetDqExecutionSettings().FlowControl.MaxOutputChunkSize", so
    // GetDqExecutionSettings() is never ODR-used and dq_compute_actor.cpp.o (which has
    // Wilson tracing deps absent from test binaries) is not extracted by the linker.
    NYql::NDq::TComputeMemoryLimits memLimits{
        /*ChannelBufferSize=*/0,
        /*MkqlLightProgramMemoryLimit=*/0,
        /*MkqlHeavyProgramMemoryLimit=*/0,
        /*MkqlProgramHardMemoryLimit=*/0,
        /*MinMemAllocSize=*/30_MB,
        /*MinMemFreeSize=*/30_MB,
        /*OutputChunkMaxSize=*/2_MB,         // explicit — GetDqExecutionSettings() not ODR-used
        /*ChunkSizeLimit=*/48_MB,
        /*ArrayBufferMinFillPercentage=*/{},  // TMaybe<ui8>: empty
        /*BufferPageAllocSize=*/{},           // TMaybe<size_t>: empty
        /*MemoryQuotaManager=*/quotaMgr,
        /*ChannelQuotaManager=*/{},
    };

    ::NMonitoring::TDynamicCounters::TCounterPtr nullCounter;
    auto memQuota = std::make_shared<NYql::NDq::TDqMemoryQuota>(
        nullCounter, /*initialMkqlMemoryLimit=*/(ui64)0, memLimits,
        NYql::NDq::TTxId{(ui64)0}, /*taskId=*/(ui64)0, /*profileStats=*/false,
        /*actorSystem=*/nullptr);
    auto computeCtx = std::make_shared<NYql::NDq::TDqComputeContextBase>();
    computeCtx->MemoryQuota = memQuota.get();
    // Capture memQuota to keep the TDqMemoryQuota alive for the lambda's lifetime.
    return [customFactory, computeCtx, memQuota](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        TStringBuf callable_name = callable.GetType()->GetName();
        if (callable_name == "TestList"sv) {
            return new TExternalComputationNode(ctx.Mutables);
        }

        if (callable_name == "DqBlockHashJoin"sv) {
            return WrapDqBlockHashJoin(callable, ctx, *computeCtx);
        }

        else if (callable_name == "DqHashCombine"sv) {
            return WrapDqHashCombine(callable, ctx, *computeCtx);
        }

        else if (callable_name == "DqHashAggregate"sv) {
            return WrapDqHashAggregate(callable, ctx, *computeCtx);
        }

        else if (callable_name == "DqScalarHashJoin") {
            return WrapDqScalarHashJoin(callable, ctx);
        }

        return GetBuiltinFactory()(callable, ctx);
    };
}

} // namespace NMiniKQL
} // namespace NKikimr
