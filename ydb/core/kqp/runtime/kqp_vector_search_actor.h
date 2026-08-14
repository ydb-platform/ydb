#pragma once

#include <ydb/core/protos/tx_datashard.pb.h>

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/runtime/kqp_vector_index_levels_cache.h>

#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

#include <functional>

namespace NKikimrTxDataShard {
    class TKqpReadRangesSourceSettings;
} // namespace NKikimrTxDataShard

namespace NKikimr {
    namespace NKqp {

        // Creates one inner read of the search (level, posting or main table); an empty
        // factory means the standard CreateKqpReadActor. Unit tests substitute a fake read to
        // drive the actor's state machine without datashards and to assert on its settings.
        using TKqpVectorInnerReadFactory = std::function<std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*>(
            const NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings,
            TIntrusivePtr<NActors::TProtoArenaHolder> arena,
            const NActors::TActorId& parentId,
            TVector<TSerializedCellVec>&& keyPoints)>;

        std::pair<NYql::NDq::IDqComputeActorAsyncInput*, IActor*> CreateKqpVectorSearchActor(
            NKikimrTxDataShard::TKqpVectorSearchSettings&& settings,
            ui64 inputIndex,
            const NUdf::TUnboxedValue& input,
            NYql::NDq::TCollectStatsLevel statsLevel,
            NYql::NDq::TTxId txId,
            ui64 taskId,
            const NActors::TActorId& computeActorId,
            const NMiniKQL::TTypeEnvironment& typeEnv,
            const NMiniKQL::THolderFactory& holderFactory,
            std::shared_ptr<NMiniKQL::TScopedAlloc>& alloc,
            const NWilson::TTraceId& traceId,
            TIntrusivePtr<TKqpCounters> counters,
            TIntrusivePtr<TVectorIndexLevelsCache> levelsCache,
            TKqpVectorInnerReadFactory innerReadFactory = {});

        void RegisterKqpVectorSearchActor(NYql::NDq::TDqAsyncIoFactory&, TIntrusivePtr<TKqpCounters>,
                                          TIntrusivePtr<TVectorIndexLevelsCache> levelsCache);

    } // namespace NKqp
} // namespace NKikimr
