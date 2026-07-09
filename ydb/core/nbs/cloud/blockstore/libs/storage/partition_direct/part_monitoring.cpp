#include "fast_path_service.h"
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_render.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

#include <ydb/library/actors/core/mon.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/algorithm.h>
#include <util/string/cast.h>

#include <numeric>
#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

EMonPage ParsePage(const TCgiParameters& cgi)
{
    const TString& page = cgi.Get("page");
    if (page == "dbg") {
        return EMonPage::Dbg;
    }
    if (page == "localdb") {
        return EMonPage::LocalDb;
    }
    return EMonPage::Overview;
}

std::optional<size_t> ParseSelectedDbg(const TCgiParameters& cgi)
{
    ui32 dbgIndex = 0;
    if (cgi.Has("dbg") && TryFromString(cgi.Get("dbg"), dbgIndex)) {
        return dbgIndex;
    }
    return std::nullopt;
}

template <typename TProto>
std::optional<TString> DumpProto(const TMaybe<TProto>& proto)
{
    if (!proto.Defined()) {
        return std::nullopt;
    }
    return proto->DebugString();
}

TLocalDbContents MakeLocalDbContents(const TTxPartition::TMonitoring& args)
{
    return {
        .VolumeConfig = DumpProto(args.VolumeConfig),
        .DirectBlockGroupsConnections =
            DumpProto(args.DirectBlockGroupsConnections),
        .AddHostInProgress = DumpProto(args.AddHostInProgress),
        .VChunkConfigs = args.VChunkConfigs,
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTabletInfo TPartitionActor::MakeMonTabletInfo()
{
    return {
        .TabletId = TabletID(),
        .Generation = Executor()->Generation(),
        .DiskId = VolumeConfig.GetDiskId(),
        .State = FastPathService ? "WORK" : "INIT",
    };
}

void TPartitionActor::HandleHttpInfo(
    NMon::TEvRemoteHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& cgi = ev->Get()->Cgi();
    const EMonPage page = ParsePage(cgi);

    // Overview (and the not-yet-ready tablet) render synchronously.
    if (!FastPathService || page == EMonPage::Overview) {
        TMonPageData data{
            .Page = page,
            .TabletInfo = MakeMonTabletInfo(),
        };
        if (!FastPathService) {
            data.RuntimeError = "tablet is still initializing";
        } else {
            data.FastPathServiceInfo = FastPathService->GetMonInfo();
        }
        ctx.Send(
            ev->Sender,
            new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
        return;
    }

    // Local DB page: read the persisted state in a transaction;
    // CompleteMonitoring renders and replies.
    if (page == EMonPage::LocalDb) {
        ExecuteTx(ctx, CreateTx<TMonitoring>(ev->Sender));
        return;
    }

    // DBG page: gather snapshots, then render + reply in the callback. Safe
    // off-thread - captures are taken here and RenderMonPage is pure.
    auto* actorSystem = TActivationContext::ActorSystem();
    const TActorId requester = ev->Sender;
    const std::optional<size_t> selectedDbg = ParseSelectedDbg(cgi);

    FastPathService->GatherMonSnapshots(selectedDbg)
        .Subscribe(
            [tabletInfo = MakeMonTabletInfo(),
             page,
             selectedDbg,
             requester,
             actorSystem](const auto& future)
            {
                TMonPageData data{
                    .Page = page,
                    .TabletInfo = tabletInfo,
                    .Dbgs = future.GetValue(),
                };
                if (selectedDbg) {
                    data.SelectedDbg = static_cast<ui32>(*selectedDbg);
                }
                Sort(
                    data.Dbgs,
                    [](const TDbgSnapshot& lhs, const TDbgSnapshot& rhs)
                    { return lhs.Index < rhs.Index; });
                actorSystem->Send(
                    requester,
                    new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
            });
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareMonitoring(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TMonitoring& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);

    std::initializer_list<bool> results = {
        db.ReadVolumeConfig(args.VolumeConfig),
        db.ReadDirectBlockGroupsConnections(args.DirectBlockGroupsConnections),
        db.ReadAllVChunkConfigs(args.VChunkConfigs),
        db.ReadAddHostInProgress(args.AddHostInProgress),
    };

    return std::accumulate(
        results.begin(),
        results.end(),
        true,
        std::logical_and<>());
}

void TPartitionActor::ExecuteMonitoring(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TMonitoring& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteMonitoring(
    const TActorContext& ctx,
    TTxPartition::TMonitoring& args)
{
    TMonPageData data{
        .Page = EMonPage::LocalDb,
        .TabletInfo = MakeMonTabletInfo(),
        .LocalDb = MakeLocalDbContents(args),
    };
    ctx.Send(
        args.Requester,
        new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
