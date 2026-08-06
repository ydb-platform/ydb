#include "hnsw_index_build_actor.h"
#include "datashard_impl.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NDataShard {

using namespace NActors;

class THnswIndexBuildActor : public TActorBootstrapped<THnswIndexBuildActor> {
public:
    THnswIndexBuildActor(
            const TActorId& replyTo,
            ui32 localTid,
            ui64 rowCountAtBuild,
            const Ydb::Table::VectorIndexSettings& settings,
            std::vector<std::pair<TString, TString>> keysAndVectors,
            ui64 maxMemoryBytes)
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

        auto ev = MakeHolder<TDataShard::TEvPrivate::TEvHnswIndexBuildResult>();
        ev->LocalTid = LocalTid;
        ev->RowCountAtBuild = RowCountAtBuild;
        ev->Error = error;
        if (index) {
            ev->Index = std::shared_ptr<THnswIndex>(std::move(index));
        }

        ctx.Send(ReplyTo, ev.Release());
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

IActor* CreateHnswIndexBuildActor(
        const TActorId& replyTo,
        ui32 localTid,
        ui64 rowCountAtBuild,
        const Ydb::Table::VectorIndexSettings& settings,
        std::vector<std::pair<TString, TString>> keysAndVectors,
        ui64 maxMemoryBytes)
{
    return new THnswIndexBuildActor(replyTo, localTid, rowCountAtBuild, settings, std::move(keysAndVectors), maxMemoryBytes);
}

void TDataShard::Handle(TEvPrivate::TEvHnswIndexBuildResult::TPtr& ev, const TActorContext& ctx) {
    Actors.erase(ev->Sender);

    auto* msg = ev->Get();
    if (msg->Index) {
        SetHnswIndex(msg->LocalTid, msg->Index, msg->RowCountAtBuild);
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            TabletID() << " HNSW: async build completed for localTid=" << msg->LocalTid
            << " size=" << msg->Index->Size());
    } else {
        SetHnswIndexBuilding(msg->LocalTid, false);
        LOG_INFO_S(ctx, NKikimrServices::TX_DATASHARD,
            TabletID() << " HNSW: async build failed for localTid=" << msg->LocalTid
            << ": " << msg->Error);
    }
}

} // namespace NKikimr::NDataShard
