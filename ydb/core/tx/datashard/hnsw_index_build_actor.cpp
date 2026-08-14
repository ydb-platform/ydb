#include "hnsw_index_build_actor.h"
#include "datashard_impl.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NDataShard {

using namespace NActors;

class THnswIndexBuildWorker : public TActorBootstrapped<THnswIndexBuildWorker> {
public:
    THnswIndexBuildWorker(const Ydb::Table::VectorIndexSettings& settings,
            std::vector<std::pair<TString, TString>> keysAndVectors,
            std::shared_ptr<void> memoryReservation,
            THnswIndexBuildCallback callback)
        : TActorBootstrapped(NKikimrServices::TActivity::DATASHARD_HNSW_BUILDER)
        , Settings(settings)
        , KeysAndVectors(std::move(keysAndVectors))
        , MemoryReservation(std::move(memoryReservation))
        , Callback(std::move(callback))
    {}

    void Bootstrap(const TActorContext& ctx) {
        THnswIndexBuildResult result;
        auto index = THnswIndex::Build(
            Settings, KeysAndVectors, /* maxMemoryBytes */ 0, result.Error);
        if (index) {
            result.Index = std::shared_ptr<THnswIndex>(std::move(index));
            result.MemoryReservation = std::move(MemoryReservation);
        }
        Callback(std::move(result), ctx);
        Die(ctx);
    }

private:
    const Ydb::Table::VectorIndexSettings Settings;
    const std::vector<std::pair<TString, TString>> KeysAndVectors;
    std::shared_ptr<void> MemoryReservation;
    THnswIndexBuildCallback Callback;
};

IActor* CreateHnswIndexBuildWorker(
        const Ydb::Table::VectorIndexSettings& settings,
        std::vector<std::pair<TString, TString>> keysAndVectors,
        std::shared_ptr<void> memoryReservation,
        THnswIndexBuildCallback callback) {
    return new THnswIndexBuildWorker(settings, std::move(keysAndVectors),
        std::move(memoryReservation), std::move(callback));
}

// Retains the existing friendship with TDataShard while adapting the common
// worker result to DataShard's private lazy-build event.
class THnswIndexBuildActor {
public:
    static IActor* Create(const TActorId& replyTo, ui32 localTid, ui32 vectorColumnTag,
            ui64 rowCountAtBuild,
            const Ydb::Table::VectorIndexSettings& settings,
            std::vector<std::pair<TString, TString>> keysAndVectors,
            std::shared_ptr<void> memoryReservation) {
        return CreateHnswIndexBuildWorker(settings, std::move(keysAndVectors),
            std::move(memoryReservation),
            [replyTo, localTid, vectorColumnTag, rowCountAtBuild]
            (THnswIndexBuildResult&& buildResult, const TActorContext& ctx) mutable {
                auto result = MakeHolder<TDataShard::TEvPrivate::TEvHnswIndexBuildResult>();
                result->LocalTid = localTid;
                result->VectorColumnTag = vectorColumnTag;
                result->RowCountAtBuild = rowCountAtBuild;
                result->Index = std::move(buildResult.Index);
                result->MemoryReservation = std::move(buildResult.MemoryReservation);
                result->Error = std::move(buildResult.Error);
                ctx.Send(replyTo, result.Release());
            });
    }
};

IActor* CreateHnswIndexBuildActor(const TActorId& replyTo, ui32 localTid, ui32 vectorColumnTag,
        ui64 rowCountAtBuild,
        const Ydb::Table::VectorIndexSettings& settings,
        std::vector<std::pair<TString, TString>> keysAndVectors,
        std::shared_ptr<void> memoryReservation) {
    return THnswIndexBuildActor::Create(replyTo, localTid, vectorColumnTag,
        rowCountAtBuild, settings, std::move(keysAndVectors),
        std::move(memoryReservation));
}

void TDataShard::Handle(TEvPrivate::TEvHnswIndexBuildResult::TPtr& ev, const TActorContext& ctx) {
    Actors.erase(ev->Sender);
    auto* result = ev->Get();
    if (result->Index) {
        SetHnswIndex(result->LocalTid, result->Index, std::move(result->MemoryReservation),
            result->RowCountAtBuild, result->VectorColumnTag);
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            TabletID() << " HNSW: lazy build completed for localTid=" << result->LocalTid
            << " size=" << result->Index->Size());
    } else {
        SetHnswIndexBuilding(result->LocalTid, false);
        LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD,
            TabletID() << " HNSW: lazy build failed for localTid=" << result->LocalTid
            << ": " << result->Error);
    }
}

} // namespace NKikimr::NDataShard
