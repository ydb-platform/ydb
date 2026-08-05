#include "hnsw_index_build_actor.h"
#include "datashard_impl.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NDataShard {

using namespace NActors;

class THnswIndexBuildActor : public TActorBootstrapped<THnswIndexBuildActor> {
public:
    THnswIndexBuildActor(const TActorId& replyTo, ui32 localTid, ui64 rowCountAtBuild,
            const Ydb::Table::VectorIndexSettings& settings,
            std::vector<std::pair<TString, TString>> keysAndVectors, ui64 maxMemoryBytes)
        : TActorBootstrapped(NKikimrServices::TActivity::DATASHARD_HNSW_BUILDER)
        , ReplyTo(replyTo)
        , LocalTid(localTid)
        , RowCountAtBuild(rowCountAtBuild)
        , Settings(settings)
        , KeysAndVectors(std::move(keysAndVectors))
        , MaxMemoryBytes(maxMemoryBytes)
    {}

    void Bootstrap(const TActorContext& ctx) {
        TString error;
        auto index = THnswIndex::Build(Settings, KeysAndVectors, MaxMemoryBytes, error);
        auto result = MakeHolder<TDataShard::TEvPrivate::TEvHnswIndexBuildResult>();
        result->LocalTid = LocalTid;
        result->RowCountAtBuild = RowCountAtBuild;
        result->Error = std::move(error);
        if (index) {
            result->Index = std::shared_ptr<THnswIndex>(std::move(index));
        }
        ctx.Send(ReplyTo, result.Release());
        Die(ctx);
    }

private:
    const TActorId ReplyTo;
    const ui32 LocalTid;
    const ui64 RowCountAtBuild;
    const Ydb::Table::VectorIndexSettings Settings;
    const std::vector<std::pair<TString, TString>> KeysAndVectors;
    const ui64 MaxMemoryBytes;
};

IActor* CreateHnswIndexBuildActor(const TActorId& replyTo, ui32 localTid, ui64 rowCountAtBuild,
        const Ydb::Table::VectorIndexSettings& settings,
        std::vector<std::pair<TString, TString>> keysAndVectors, ui64 maxMemoryBytes) {
    return new THnswIndexBuildActor(replyTo, localTid, rowCountAtBuild, settings,
        std::move(keysAndVectors), maxMemoryBytes);
}

void TDataShard::Handle(TEvPrivate::TEvHnswIndexBuildResult::TPtr& ev, const TActorContext& ctx) {
    Actors.erase(ev->Sender);
    auto* result = ev->Get();
    if (result->Index) {
        SetHnswIndex(result->LocalTid, result->Index, result->RowCountAtBuild);
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
