#include "hnsw_index_build_actor.h"
#include "../datashard_impl.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NDataShard {

    using namespace NActors;

    class THnswIndexBuildWorker: public TActorBootstrapped<THnswIndexBuildWorker> {
    public:
        THnswIndexBuildWorker(const Ydb::Table::VectorIndexSettings& settings,
                              std::vector<std::pair<TString, TString>> keysAndVectors,
                              std::shared_ptr<void> memoryReservation,
                              ui64 maxMemoryBytes,
                              THnswIndexBuildCallback callback, bool allowEmpty)
            : TActorBootstrapped(NKikimrServices::TActivity::DATASHARD_READ_SCAN)
            , Settings(settings)
            , KeysAndVectors(std::move(keysAndVectors))
            , MemoryReservation(std::move(memoryReservation))
            , MaxMemoryBytes(maxMemoryBytes)
            , Callback(std::move(callback))
            , AllowEmpty(allowEmpty)
        {
        }

        void Bootstrap(const TActorContext& ctx) {
            THnswIndexBuildResult result;
            try {
                auto index = THnswIndex::Build(
                    Settings, KeysAndVectors, MaxMemoryBytes, result.Error, AllowEmpty);
                if (index) {
                    result.Index = std::shared_ptr<THnswIndex>(std::move(index));
                    result.MemoryReservation = std::move(MemoryReservation);
                }
            } catch (const std::exception& error) {
                result.Error = error.what();
            }
            Callback(std::move(result), ctx);
            Die(ctx);
        }

    private:
        const Ydb::Table::VectorIndexSettings Settings;
        const std::vector<std::pair<TString, TString>> KeysAndVectors;
        std::shared_ptr<void> MemoryReservation;
        const ui64 MaxMemoryBytes;
        THnswIndexBuildCallback Callback;
        bool AllowEmpty;
    };

    IActor* CreateHnswIndexBuildWorker(
        const Ydb::Table::VectorIndexSettings& settings,
        std::vector<std::pair<TString, TString>> keysAndVectors,
        std::shared_ptr<void> memoryReservation,
        ui64 maxMemoryBytes,
        THnswIndexBuildCallback callback, bool allowEmpty) {
        return new THnswIndexBuildWorker(settings, std::move(keysAndVectors),
                                         std::move(memoryReservation), maxMemoryBytes, std::move(callback), allowEmpty);
    }

    // Retains the existing friendship with TDataShard while adapting the common
    // worker result to DataShard's private lazy-build event.
    class THnswIndexBuildActor {
    public:
        static IActor* Create(const TActorId& replyTo, ui32 localTid, ui32 vectorColumnTag,
                              ui64 rowCountAtBuild,
                              const Ydb::Table::VectorIndexSettings& settings,
                              std::vector<std::pair<TString, TString>> keysAndVectors,
                              std::shared_ptr<void> memoryReservation,
                              ui64 maxMemoryBytes, TRowVersion baseVersion, ui64 buildToken, bool allowEmpty, TPathId indexId, ui64 memoryOwner) {
            return CreateHnswIndexBuildWorker(settings, std::move(keysAndVectors),
                                              std::move(memoryReservation), maxMemoryBytes,
                                              [replyTo, localTid, indexId, memoryOwner, vectorColumnTag, rowCountAtBuild, settings, baseVersion, buildToken](THnswIndexBuildResult&& buildResult, const TActorContext& ctx) mutable {
                                                  auto result = MakeHolder<TDataShard::TEvPrivate::TEvHnswIndexBuildResult>();
                                                  result->LocalTid = localTid;
                                                  result->IndexId = indexId;
                                                  result->VectorColumnTag = vectorColumnTag;
                                                  result->RowCountAtBuild = rowCountAtBuild;
                                                  result->Settings = settings;
                                                  result->BaseVersion = baseVersion;
                                                  result->BuildToken = buildToken;
                                                  result->MemoryOwner = memoryOwner;
                                                  result->Index = std::move(buildResult.Index);
                                                  result->MemoryReservation = std::move(buildResult.MemoryReservation);
                                                  result->Error = std::move(buildResult.Error);
                                                  ctx.Send(replyTo, result.Release());
                                              }, allowEmpty);
        }
    };

    IActor* CreateHnswIndexBuildActor(const TActorId& replyTo, ui32 localTid, ui32 vectorColumnTag,
                                      ui64 rowCountAtBuild,
                                      const Ydb::Table::VectorIndexSettings& settings,
                                      std::vector<std::pair<TString, TString>> keysAndVectors,
                                      std::shared_ptr<void> memoryReservation,
                                      ui64 maxMemoryBytes, TRowVersion baseVersion, ui64 buildToken, bool allowEmpty, TPathId indexId, ui64 memoryOwner) {
        return THnswIndexBuildActor::Create(replyTo, localTid, vectorColumnTag,
                                            rowCountAtBuild, settings, std::move(keysAndVectors),
                                            std::move(memoryReservation), maxMemoryBytes, baseVersion, buildToken, allowEmpty, indexId, memoryOwner);
    }

    void TDataShard::Handle(TEvPrivate::TEvHnswIndexBuildResult::TPtr& ev, const TActorContext& ctx) {
        Actors.erase(ev->Sender);
        auto* result = ev->Get();
        HnswBuildsInFlight.erase(result->BuildToken);
        if (!IsHnswBuildCurrent(result->LocalTid, result->BuildToken, result->IndexId)) {
            auto* it = FindHnswIndexEntry(result->LocalTid, result->IndexId);
            if (HnswCacheMemoryTracker && (!it || it->MemoryOwner != result->MemoryOwner || it->BuildToken == result->BuildToken)) {
                HnswCacheMemoryTracker->ResetDemand(result->MemoryOwner);
            }
            if (it && it->BuildToken == result->BuildToken) {
                DeferHnswIndexBuild(result->LocalTid, TDuration::Seconds(1), result->IndexId);
                ScheduleHnswRebuild(result->LocalTid, result->IndexId);
            }
            return;
        }
        if (result->BelowMinRows) {
            // A successful scan of a small partition is not a failed build. Writes
            // and settings changes re-enable it; reads alone must not scan again.
            const auto& entry = *FindHnswIndexEntry(result->LocalTid, result->IndexId);
            if (entry.Changes->CountAfter(entry.BuildVersion)) {
                // The table may have grown while the snapshot scan was running.
                DeferHnswIndexBuild(result->LocalTid, TDuration::Zero(), result->IndexId);
                ScheduleHnswRebuild(result->LocalTid, result->IndexId);
            } else {
                DisableHnswIndexBuild(result->LocalTid, result->IndexId);
            }
            return;
        }
        if (result->Index) {
            LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
                       TabletID() << " HNSW: lazy build completed for localTid=" << result->LocalTid << " indexId=" << result->IndexId
                                  << " size=" << result->Index->Size()
                                  << " baseVersion=" << result->BaseVersion << " buildToken=" << result->BuildToken);
            SetHnswIndex(result->LocalTid, std::move(result->Index), std::move(result->MemoryReservation),
                         result->RowCountAtBuild, result->VectorColumnTag, result->Settings,
                         result->BaseVersion, result->BuildToken, result->IndexId);
        } else {
            DeferHnswIndexBuild(result->LocalTid, TDuration::Seconds(5), result->IndexId);
            ScheduleHnswRebuild(result->LocalTid, result->IndexId);
            LOG_NOTICE_S(ctx, NKikimrServices::TX_DATASHARD,
                         TabletID() << " HNSW: lazy build failed for localTid=" << result->LocalTid << " indexId=" << result->IndexId
                                    << ": " << result->Error);
        }
    }

} // namespace NKikimr::NDataShard
