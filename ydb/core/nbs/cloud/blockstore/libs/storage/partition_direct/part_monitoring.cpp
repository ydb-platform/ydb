#include "fast_path_service.h"
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_render.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/part_database.h>

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
        const std::optional<ui32> selectedVChunk = ParseSelectedVChunk(cgi);
        if (!selectedVChunk) {
            TMonPageData data{
                .Page = page,
                .TabletInfo = MakeMonTabletInfo(),
            };
            ctx.Send(
                ev->Sender,
                new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
            return true;
        }

        auto* actorSystem = TActivationContext::ActorSystem();
        const TActorId requester = ev->Sender;
        FastPathService->GatherVChunkMonSnapshot(*selectedVChunk)
            .Subscribe(
                [tabletInfo = MakeMonTabletInfo(),
                 page,
                 selectedVChunk,
                 requester,
                 actorSystem](const auto& future)
                {
                    TMonPageData data{
                        .Page = page,
                        .TabletInfo = tabletInfo,
                        .SelectedVChunk = selectedVChunk,
                        .VChunk = future.GetValue(),
                    };
                    actorSystem->Send(
                        requester,
                        new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
                });
        return true;
    }

    if (page == EMonPage::VChunkCounters) {
        auto* actorSystem = TActivationContext::ActorSystem();
        const TActorId requester = ev->Sender;
        const size_t limit = ParseVChunkStatsLimit(cgi);
        const std::optional<size_t> selectedDbg = ParseSelectedDbg(cgi);
        const bool showVChunks = cgi.Get("showvchunks") == "1";
        const auto detail = (showVChunks && selectedDbg)
                                ? EVChunkStatsDetail::PerVChunk
                                : EVChunkStatsDetail::TotalOnly;
        FastPathService->GatherVChunkStats(detail, selectedDbg)
            .Subscribe(
                [tabletInfo = MakeMonTabletInfo(),
                 page,
                 limit,
                 selectedDbg,
                 showVChunks,
                 requester,
                 actorSystem](const auto& future)
                {
                    TMonPageData data{
                        .Page = page,
                        .TabletInfo = tabletInfo,
                        .VChunkStats = future.GetValue(),
                        .VChunkStatsLimit = limit,
                        .ShowVChunks = showVChunks,
                    };
                    if (selectedDbg) {
                        data.SelectedVChunkDbg =
                            static_cast<ui32>(*selectedDbg);
                    }
                    actorSystem->Send(
                        requester,
                        new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
                });
        return true;
    }

    // Latency page always gathers every DBG (needs the full node map).
    const std::optional<size_t> selectedDbg =
        (page == EMonPage::Latency) ? std::nullopt : ParseSelectedDbg(cgi);
    const ELatencyPercentile selectedPercentile = ParseSelectedPercentile(cgi);
    const std::optional<EOperation> selectedLatencyOperation =
        ParseSelectedLatencyOperation(cgi);

    // The "Add host" button. POST only: link prefetching must not add hosts.
    //
    // The index is user input from the URL, but HandleAddHostToDBG treats an
    // out-of-range index as a bug and aborts - so bounds-check it here. All
    // other checks live there. The reply bounces back to the same DBG page.
    if (page == EMonPage::Dbg && selectedDbg &&
        cgi.Get("action") == "addhost" &&
        ev->Get()->GetMethod() == HTTP_METHOD_POST)
    {
        const bool dbgExists =
            *selectedDbg < FastPathService->GetDirectBlockGroups().size();
        if (dbgExists) {
            LOG_INFO(
                ctx,
                NKikimrServices::NBS_PARTITION,
                "%s Mon page requested AddHost dbgId=%lu",
                LogTitle.GetWithTime().c_str(),
                *selectedDbg);

            FastPathService->QueryAddHost(*selectedDbg, 0);
        }

        TStringBuilder reply;
        if (dbgExists) {
            reply << "<p>Add host requested for DBG #" << *selectedDbg
                  << ".</p>";
        } else {
            reply << "<p>DBG #" << *selectedDbg << " not found.</p>";
        }
        // Bounce straight back to the same DBG page.
        reply << "<meta http-equiv='refresh' content='0; ?TabletID="
              << TabletID() << "&page=dbg&dbg=" << *selectedDbg << "'/>";
        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(reply));
        return true;
    }

    // DBG / Latency page: gather snapshots, then render + reply in the
    // callback. Safe off-thread - captures are taken here and RenderMonPage
    // is pure.
    auto* actorSystem = TActivationContext::ActorSystem();
    const TActorId requester = ev->Sender;

    FastPathService->GatherMonSnapshots(selectedDbg)
        .Subscribe(
            [tabletInfo = MakeMonTabletInfo(),
             page,
             selectedDbg,
             selectedPercentile,
             selectedLatencyOperation,
             requester,
             actorSystem](const auto& future)
            {
                TMonPageData data{
                    .Page = page,
                    .TabletInfo = tabletInfo,
                    .Dbgs = future.GetValue(),
                    .SelectedPercentile = selectedPercentile,
                    .SelectedLatencyOperation = selectedLatencyOperation,
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
