#include "fast_path_service.h"
#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/mon_page/mon_render.h>

#include <ydb/library/actors/core/mon.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/algorithm.h>
#include <util/string/cast.h>

#include <optional>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

// How long the actor waits for every DBG to report its snapshot before
// rendering the DBG page with whatever has arrived. A safety net only; the
// gather normally completes in well under this.
constexpr TDuration MonRenderDeadline = TDuration::Seconds(5);

EMonPage ParsePage(const TCgiParameters& cgi)
{
    if (cgi.Get("page") == "dbg") {
        return EMonPage::Dbg;
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTabletInfo TPartitionActor::MakeMonTabletInfo()
{
    TTabletInfo info;
    info.TabletId = TabletID();
    info.Generation = Executor()->Generation();
    info.DiskId = VolumeConfig.GetDiskId();
    info.State = FastPathService ? "WORK" : "INIT";
    return info;
}

void TPartitionActor::HandleHttpInfo(
    NMon::TEvRemoteHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& cgi = ev->Get()->Cgi();
    const EMonPage page = ParsePage(cgi);

    // Overview - and any request while the tablet is still initializing -
    // renders synchronously: no per-DBG gather is needed.
    if (!FastPathService || page == EMonPage::Overview) {
        TMonPageData data;
        data.Page = page;
        data.TabletInfo = MakeMonTabletInfo();
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

    // DBG page: kick the asynchronous per-DBG host snapshot gather and remember
    // the request until every snapshot arrives (or the deadline fires).
    const std::optional<size_t> selectedDbg = ParseSelectedDbg(cgi);
    const ui64 cookie = ++MonCookieCounter;
    const size_t expected =
        FastPathService->RequestMonSnapshots(SelfId(), cookie, selectedDbg);

    TMonRequest request;
    request.Page = page;
    request.Requester = ev->Sender;
    if (selectedDbg) {
        request.SelectedDbg = static_cast<ui32>(*selectedDbg);
    }
    request.PendingDbgs = expected;
    MonRequests[cookie] = std::move(request);

    if (expected == 0) {
        // Nothing to wait for (e.g. detail of a nonexistent DBG): render now.
        // The empty Dbgs list makes RenderMonPage show the "not found" notice.
        MaybeReplyMonRequest(cookie, ctx);
        return;
    }

    ctx.Schedule(
        MonRenderDeadline,
        new TEvPartitionDirectPrivate::TEvMonRenderTimeout(cookie));
}

void TPartitionActor::HandleMonDbgSnapshotReady(
    const TEvPartitionDirectPrivate::TEvMonDbgSnapshotReady::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 cookie = ev->Get()->Cookie;
    auto it = MonRequests.find(cookie);
    if (it == MonRequests.end()) {
        // Already rendered (deadline fired) or unknown cookie.
        return;
    }

    auto& request = it->second;
    request.Dbgs.push_back(std::move(ev->Get()->Snapshot));
    if (request.PendingDbgs != 0) {
        --request.PendingDbgs;
    }
    MaybeReplyMonRequest(cookie, ctx);
}

void TPartitionActor::HandleMonRenderTimeout(
    const TEvPartitionDirectPrivate::TEvMonRenderTimeout::TPtr& ev,
    const TActorContext& ctx)
{
    const ui64 cookie = ev->Get()->Cookie;
    auto it = MonRequests.find(cookie);
    if (it == MonRequests.end()) {
        return;
    }
    // Render with whatever snapshots have arrived so far.
    it->second.PendingDbgs = 0;
    MaybeReplyMonRequest(cookie, ctx);
}

void TPartitionActor::MaybeReplyMonRequest(
    ui64 cookie,
    const TActorContext& ctx)
{
    auto it = MonRequests.find(cookie);
    if (it == MonRequests.end()) {
        return;
    }
    auto& request = it->second;
    if (request.PendingDbgs != 0) {
        return;
    }

    // DBGs may report out of order; present them by index.
    Sort(
        request.Dbgs,
        [](const TDbgSnapshot& l, const TDbgSnapshot& r)
        { return l.Index < r.Index; });

    TMonPageData data;
    data.Page = request.Page;
    data.TabletInfo = MakeMonTabletInfo();
    data.SelectedDbg = request.SelectedDbg;
    data.Dbgs = std::move(request.Dbgs);

    ctx.Send(
        request.Requester,
        new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
    MonRequests.erase(it);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
