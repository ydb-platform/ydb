#include "part_database.h"
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_render.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <numeric>
#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

using EChaosMode = TChaosConfig::TChaosNodeConfig::EChaosMode;

struct TChaosAction
{
    ui32 NodeId = 0;
    // An absent index means all DBGs.
    std::optional<ui32> DbgIndex;
    EChaosMode Mode = EChaosMode::Enabled;
};

std::optional<EChaosMode> ParseChaosMode(TStringBuf action)
{
    if (action == "disable") {
        return EChaosMode::Disabled;
    }
    if (action == "enable") {
        return EChaosMode::Enabled;
    }
    return std::nullopt;
}

TString MakeRedirectResponse(
    ui64 tabletId,
    TStringBuf page,
    TStringBuf message,
    TStringBuf querySuffix = {})
{
    TStringBuilder reply;
    reply << "<p>" << message << "</p>"
          << "<meta http-equiv='refresh' content='0; ?TabletID=" << tabletId
          << "&page=" << page << querySuffix << "'/>";
    return reply;
}

EMonPage ParsePage(const TCgiParameters& cgi)
{
    const TString& page = cgi.Get("page");
    if (page == "dbg") {
        return EMonPage::Dbg;
    }
    if (page == "chaos") {
        return EMonPage::Chaos;
    }
    if (page == "localdb") {
        return EMonPage::LocalDb;
    }
    if (page == "vchunk") {
        return EMonPage::VChunk;
    }
    if (page == "vchunkcounters") {
        return EMonPage::VChunkCounters;
    }
    if (page == "latency") {
        return EMonPage::Latency;
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

std::optional<TChaosAction> ParseChaosAction(const TCgiParameters& cgi)
{
    const auto mode = ParseChaosMode(cgi.Get("action"));
    if (!mode) {
        return std::nullopt;
    }

    ui32 nodeId = 0;
    if (!cgi.Has("node") || !TryFromString(cgi.Get("node"), nodeId)) {
        return std::nullopt;
    }

    if (cgi.Get("dbg") == "all") {
        return TChaosAction{
            .NodeId = nodeId,
            .Mode = *mode,
        };
    }

    ui32 dbgIndex = 0;
    if (!cgi.Has("dbg") || !TryFromString(cgi.Get("dbg"), dbgIndex)) {
        return std::nullopt;
    }

    return TChaosAction{
        .NodeId = nodeId,
        .DbgIndex = dbgIndex,
        .Mode = *mode,
    };
}

std::optional<ui32> ParseSelectedVChunk(const TCgiParameters& cgi)
{
    ui32 vchunkIndex = 0;
    if (cgi.Has("vchunk") && TryFromString(cgi.Get("vchunk"), vchunkIndex)) {
        return vchunkIndex;
    }
    return std::nullopt;
}

ELatencyPercentile ParseSelectedPercentile(const TCgiParameters& cgi)
{
    const TString& p = cgi.Get("p");
    if (p == "50") {
        return ELatencyPercentile::P50;
    }
    if (p == "90") {
        return ELatencyPercentile::P90;
    }
    if (p == "max") {
        return ELatencyPercentile::Max;
    }
    return ELatencyPercentile::P99;
}

// Per-vchunk row cap. all=1 dumps everything; limit=N overrides the default.
size_t ParseVChunkStatsLimit(const TCgiParameters& cgi)
{
    if (cgi.Get("all") == "1") {
        return 0;
    }
    size_t limit = 0;
    if (cgi.Has("limit") && TryFromString(cgi.Get("limit"), limit)) {
        return limit;
    }
    return DefaultVChunkStatsLimit;
}

std::optional<EOperation> ParseSelectedLatencyOperation(
    const TCgiParameters& cgi)
{
    ui32 opIndex = 0;
    if (cgi.Has("op") && TryFromString(cgi.Get("op"), opIndex) &&
        opIndex < OperationCount)
    {
        return static_cast<EOperation>(opIndex);
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

TTabletInfo TPartitionActor::MakeMonTabletInfo() const
{
    return {
        .TabletId = TabletID(),
        .Generation = Executor()->Generation(),
        .BlockSize = VolumeConfig.GetBlockSize(),
        .DiskId = VolumeConfig.GetDiskId(),
        .State = FastPathService ? "WORK" : "INIT",
    };
}

bool TPartitionActor::OnRenderAppHtmlPage(
    NMon::TEvRemoteHttpInfo::TPtr ev,
    const TActorContext& ctx)
{
    if (!ev) {
        // Probe from the standard tablet page: report that the App page exists
        // so its link is shown.
        return true;
    }

    const auto& cgi = ev->Get()->Cgi();
    const EMonPage page = ParsePage(cgi);

    TMonPageData data{
        .Page = page,
        .TabletInfo = MakeMonTabletInfo(),
        .SelectedDbg = ParseSelectedDbg(cgi),
        .SelectedVChunk = ParseSelectedVChunk(cgi),
        .VChunkStatsLimit = ParseVChunkStatsLimit(cgi),
        .ShowVChunks = cgi.Get("showvchunks") == "1",
        .SelectedPercentile = ParseSelectedPercentile(cgi),
        .SelectedLatencyOperation = ParseSelectedLatencyOperation(cgi)};

    // The not-yet-ready tablet renders synchronously.
    if (!FastPathService) {
        data.RuntimeError = "tablet is still initializing";
        ctx.Send(
            ev->Sender,
            new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
        return true;
    }

    if (page == EMonPage::Overview) {
        data.FastPathServiceInfo = FastPathService->GetMonInfo();
        FastPathService->GatherMonSnapshots(std::nullopt)
            .Subscribe(
                [data = std::move(data),
                 requester = ev->Sender,
                 actorSystem = TActivationContext::ActorSystem()]   //
                (const TFuture<TVector<TDbgSnapshot>>& future) mutable
                {
                    data.Dbgs = future.GetValue();
                    actorSystem->Send(
                        requester,
                        new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
                });
        return true;
    }

    // Local DB page: read the persisted state in a transaction;
    // CompleteMonitoring renders and replies.
    if (page == EMonPage::LocalDb) {
        ExecuteTx(ctx, CreateTx<TMonitoring>(ev->Sender));
        return true;
    }

    // VChunk page: no index - just the input form (synchronous); with an
    // index - gather the snapshot from the owning DBG's executor.
    if (page == EMonPage::VChunk) {
        if (!data.SelectedVChunk) {
            ctx.Send(
                ev->Sender,
                new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
            return true;
        }

        FastPathService->GatherVChunkMonSnapshot(*data.SelectedVChunk)
            .Subscribe(
                [data = std::move(data),
                 requester = ev->Sender,
                 actorSystem = TActivationContext::ActorSystem()]   //
                (const TFuture<std::optional<TVChunkSnapshot>>& future) mutable
                {
                    data.VChunk = future.GetValue();
                    actorSystem->Send(
                        requester,
                        new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
                });
        return true;
    }

    if (page == EMonPage::VChunkCounters) {
        const auto detail = (data.ShowVChunks && data.SelectedDbg)
                                ? EVChunkStatsDetail::PerVChunk
                                : EVChunkStatsDetail::TotalOnly;
        FastPathService->GatherVChunkStats(detail, data.SelectedDbg)
            .Subscribe(
                [data = std::move(data),
                 requester = ev->Sender,
                 actorSystem = TActivationContext::ActorSystem()]   //
                (const TFuture<TVChunkStatsGatherResult>& future) mutable
                {
                    data.VChunkStats = future.GetValue();
                    actorSystem->Send(
                        requester,
                        new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
                });
        return true;
    }

    if (page == EMonPage::Chaos && ev->Get()->GetMethod() == HTTP_METHOD_POST) {
        const auto action = ParseChaosAction(cgi);
        if (action) {
            LOG_INFO(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s Mon page requested chaos action=%s nodeId=%u dbg=%s",
                LogTitle.GetWithTime().c_str(),
                cgi.Get("action").c_str(),
                action->NodeId,
                cgi.Get("dbg").c_str());

            FastPathService->SetNodeChaosMode(
                action->NodeId,
                action->DbgIndex,
                action->Mode);
        }

        const TStringBuf message = action
                                       ? "Chaos configuration updated."
                                       : "Invalid chaos configuration request.";
        ctx.Send(
            ev->Sender,
            new NMon::TEvRemoteHttpInfoRes(
                MakeRedirectResponse(TabletID(), "chaos", message)));
        return true;
    }

    // The "Add host" button. POST only: link prefetching must not add hosts.
    // The index is user input from the URL, but HandleAddHostToDBG treats an
    // out-of-range index as a bug and aborts - so bounds-check it here. All
    // other checks live there. The reply bounces back to the same DBG page.
    if (page == EMonPage::Dbg && data.SelectedDbg &&
        cgi.Get("action") == "addhost" &&
        ev->Get()->GetMethod() == HTTP_METHOD_POST)
    {
        TStringBuilder reply;
        if (FastPathService->GetDirectBlockGroup(*data.SelectedDbg)) {
            LOG_INFO(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s Mon page requested AddHost dbgId=%lu",
                LogTitle.GetWithTime().c_str(),
                *data.SelectedDbg);

            FastPathService->QueryAddHost(
                *data.SelectedDbg,
                DirectBlockGroupsConnections
                    .GetDirectBlockGroupConnections(*data.SelectedDbg)
                    .GetConnectionConfigGeneration());
            reply << "<p>Add host requested for "
                  << PrintDbgId(*data.SelectedDbg) << ".</p>";
        } else {
            reply << "<p>" << PrintDbgId(*data.SelectedDbg)
                  << " not found.</p>";
        }

        // Bounce straight back to the same DBG page.
        reply << "<meta http-equiv='refresh' content='0; ?TabletID="
              << TabletID() << "&page=dbg&dbg=" << *data.SelectedDbg << "'/>";
        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(reply));
        return true;
    }

    if (page == EMonPage::Dbg) {
        FastPathService->GatherMonSnapshots(data.SelectedDbg)
            .Subscribe(
                [data = std::move(data),
                 requester = ev->Sender,
                 actorSystem = TActivationContext::ActorSystem()]   //
                (const TFuture<TVector<TDbgSnapshot>>& future) mutable
                {
                    data.Dbgs = future.GetValue();
                    actorSystem->Send(
                        requester,
                        new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
                });
        return true;
    }

    if (page == EMonPage::Latency) {
        FastPathService->GatherMonSnapshots(std::nullopt)
            .Subscribe(
                [data = std::move(data),
                 requester = ev->Sender,
                 actorSystem = TActivationContext::ActorSystem()]   //
                (const TFuture<TVector<TDbgSnapshot>>& future) mutable
                {
                    data.Dbgs = future.GetValue();
                    actorSystem->Send(
                        requester,
                        new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
                });
        return true;
    }

    if (page == EMonPage::Chaos) {
        data.Chaos = FastPathService->GetChaosConfig();
        FastPathService->GatherMonSnapshots(std::nullopt)
            .Subscribe(
                [data = std::move(data),
                 requester = ev->Sender,
                 actorSystem = TActivationContext::ActorSystem()]   //
                (const TFuture<TVector<TDbgSnapshot>>& future) mutable
                {
                    data.Dbgs = future.GetValue();
                    actorSystem->Send(
                        requester,
                        new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
                });
        return true;
    }

    return true;
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
