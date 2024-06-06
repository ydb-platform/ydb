#include "flat_executor_bootlogic.h"
#include "flat_boot_env.h"
#include "flat_boot_blobs.h"
#include "flat_boot_back.h"
#include "flat_boot_stages.h"
#include "flat_exec_commit_mgr.h"
#include "flat_bio_actor.h"
#include "flat_bio_events.h"
#include "flat_sausage_chop.h"
#include "logic_snap_waste.h"
#include "logic_snap_main.h"
#include "util_fmt_logger.h"
#include "util_fmt_basic.h"
#include "shared_sausagecache.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/util/pb.h>
#include <ydb/library/actors/core/monotonic_provider.h>


namespace NKikimr {
namespace NTabletFlatExecutor {

NBoot::TLoadBlobs::TLoadBlobs(IStep *owner, NPageCollection::TLargeGlobId largeGlobId, ui64 cookie)
    : IStep(owner, NBoot::EStep::Blobs)
    , Cookie(cookie)
    , LargeGlobId(largeGlobId)
    , State(LargeGlobId)
{
    Cerr << "LoadBlobs " << TypeName(*owner) << " ";
    Logic->LoadEntry(this);
}

TExecutorBootLogic::TExecutorBootLogic(IOps *ops, const TActorId &self, TTabletStorageInfo *info, ui64 maxBytesInFly)
    : Ops(ops)
    , SelfId(self)
    , Info(info)
    , GroupResolveCachedChannel(Max<ui32>())
    , GroupResolveCachedGeneration(Max<ui32>())
    , GroupResolveCachedGroup(Max<ui32>())
{
    LoadBlobQueue.Config.MaxBytesInFly = maxBytesInFly;
}

TExecutorBootLogic::~TExecutorBootLogic()
{
    LoadBlobQueue.Clear();

    Loads.clear();
    EntriesToLoad.clear();

    Steps->Execute(); /* should flush all jobs in internal queue */

    Y_ABORT_UNLESS(Steps->Alone(), "Bootlogic is still has pending IStep()s");
}

void TExecutorBootLogic::Describe(IOutputStream &out) const noexcept
{
    return Steps->Describe(out);
}

TExecutorBootLogic::EOpResult TExecutorBootLogic::ReceiveFollowerBoot(
        TEvTablet::TEvFBoot::TPtr &ev,
        TExecutorCaches &&caches)
{
    TEvTablet::TEvFBoot *msg = ev->Get();
    PrepareEnv(true, msg->Generation, std::move(caches));

    if (msg->DependencyGraph) {
        Steps->Spawn<NBoot::TStages>(std::move(msg->DependencyGraph), nullptr);
    } else {
        auto *update = msg->Update.Get();
        Y_ABORT_UNLESS(update->IsSnapshot);
        Y_ABORT_UNLESS(!update->NeedFollowerGcAck);

        if (auto logl = Steps->Logger()->Log(ELnLev::Debug))
            logl
            << NFmt::Do(State()) << " start follower from log"
            << " snapshot " << State().Generation << ":" << update->Step;

        TString body;
        TVector<TLogoBlobID> logo;
        logo.reserve(update->References.size());

        for (const auto &blob : update->References) {
            logo.emplace_back(blob.first);
            body.append(blob.second);
        }

        const auto span = NPageCollection::TGroupBlobsByCookie(logo).Do();
        const auto largeGlobId = NPageCollection::TGroupBlobsByCookie::ToLargeGlobId(span, GetBSGroupFor(logo[0]));

        Y_ABORT_UNLESS(span.size() == update->References.size());
        Y_ABORT_UNLESS(TCookie(logo[0].Cookie()).Type() == TCookie::EType::Log);
        Y_ABORT_UNLESS(largeGlobId, "Cannot make TLargeGlobId for snapshot");

        Steps->Spawn<NBoot::TStages>(nullptr, new NBoot::TBody{ largeGlobId, std::move(body) });
    }

    Steps->Execute();

    return CheckCompletion();
}

TExecutorBootLogic::EOpResult TExecutorBootLogic::ReceiveBoot(
        TEvTablet::TEvBoot::TPtr &ev,
        TExecutorCaches &&caches)
{
    TEvTablet::TEvBoot *msg = ev->Get();
    PrepareEnv(false, msg->Generation, std::move(caches));

    if (msg->DependencyGraph) {
        StartLeaseWaiter(BootTimestamp, *msg->DependencyGraph);

        for (const auto &entry : msg->DependencyGraph->Entries) {
            for (const auto &blobId : entry.References) {
                Cerr << "ReceiveBoot ";
                SeenBlob(blobId);
            }
        }
    }

    Steps->Spawn<NBoot::TStages>(std::move(msg->DependencyGraph), nullptr);
    Steps->Execute();

    return CheckCompletion();
}

void TExecutorBootLogic::PrepareEnv(bool follower, ui32 gen, TExecutorCaches caches) noexcept
{
    BootTimestamp = AppData()->MonotonicTimeProvider->Now();

    auto *sys = TlsActivationContext->ExecutorThread.ActorSystem;
    auto *logger = new NUtil::TLogger(sys, NKikimrServices::TABLET_FLATBOOT);

    LoadBlobQueue.Config.TabletID = Info->TabletID;
    LoadBlobQueue.Config.Generation = gen;
    LoadBlobQueue.Config.Follower = follower;
    LoadBlobQueue.Config.NoDataCounter = GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_boot_nodata", true);

    State_ = new NBoot::TBack(follower, Info->TabletID, gen);
    State().Scheme = new NTable::TScheme;
    State().PageCaches = std::move(caches.PageCaches);
    State().TxStatusCaches = std::move(caches.TxStatusCaches);

    Steps = new NBoot::TRoot(this, State_.Get(), logger);

    Result_ = new NBoot::TResult;

    if (follower) {
        /* Required for TLargeGlobId-less TPart data (Evolution < 12) */

        Result().Loans = new TExecutorBorrowLogic(nullptr);
    } else {
        auto &steppedCookieAllocatorFactory = *(State().SteppedCookieAllocatorFactory = new NBoot::TSteppedCookieAllocatorFactory(*Info, gen));

        State().Waste = new NSnap::TWaste(gen);
        Result().GcLogic = new TExecutorGCLogic(Info, steppedCookieAllocatorFactory.Sys(TCookie::EIdx::GCExt));
        Result().Alter = new TLogicAlter(steppedCookieAllocatorFactory.Sys(TCookie::EIdx::Alter));
        Result().Loans = new TExecutorBorrowLogic(steppedCookieAllocatorFactory.Sys(TCookie::EIdx::Loan));
        Result().Comp = new TCompactionLogicState();

        /* The rest of ... are produced on TStages::FinalizeLogicObjects() */
    }
}

void TExecutorBootLogic::LoadEntry(TIntrusivePtr<NBoot::TLoadBlobs> entry) {
    if (auto logl = Steps->Logger()->Log(ELnLev::Debug)) {
        logl
            << NFmt::Do(State()) << " Loading " << NFmt::Do(entry->LargeGlobId);
    }

    Y_ABORT_UNLESS(entry->LargeGlobId, "Support loads only of valid TLargeGlobId units");
    Y_ABORT_UNLESS(entry->Blobs(), "Valid TLargeGlobId unit hasn't been expanded to blobs");

    const ui32 group = entry->LargeGlobId.Group;

    Y_ABORT_UNLESS(group != NPageCollection::TLargeGlobId::InvalidGroup, "Got TLargeGlobId without BS group");

    for (const auto &blobId : entry->Blobs()) {
        EntriesToLoad[blobId] = entry;
        LoadBlobQueue.Enqueue(blobId, group, this);
        Cerr << "LoadEntry ";
        SeenBlob(blobId);
    }
}

NBoot::TSpawned TExecutorBootLogic::LoadPages(NBoot::IStep *step, TAutoPtr<NPageCollection::TFetch> req) {
    auto success = Loads.insert(std::make_pair(req->PageCollection.Get(), step)).second;

    Y_ABORT_UNLESS(success, "IPageCollection queued twice for loading");

    Cerr << "LoadPages ";
    SeenBlob(req->PageCollection->Label());

    Ops->Send(
        MakeSharedPageCacheId(),
        new NSharedCache::TEvRequest(
            NBlockIO::EPriority::Fast,
            req,
            SelfId),
        0, (ui64)EPageCollectionRequest::BootLogic);

    return NBoot::TSpawned(true);
}

ui32 TExecutorBootLogic::GetBSGroupFor(const TLogoBlobID &logo) const {
    auto *info = (logo.TabletID() == Info->TabletID) ? Info.Get() : Result().Loans->StorageInfoFor(logo);
    return info->GroupFor(logo.Channel(), logo.Generation());
}

ui32 TExecutorBootLogic::GetBSGroupID(ui32 channel, ui32 generation) {
    if (generation != GroupResolveCachedGeneration || channel != GroupResolveCachedChannel) {
        GroupResolveCachedChannel = channel;
        GroupResolveCachedGeneration = generation;
        GroupResolveCachedGroup = Info->GroupFor(channel, generation);
    }

    return GroupResolveCachedGroup;
}

TExecutorBootLogic::EOpResult TExecutorBootLogic::CheckCompletion()
{
    if (LoadBlobQueue.SendRequests(SelfId))
        return OpResultContinue;

    Y_ABORT_UNLESS(EntriesToLoad.empty());

    if (Steps && !Steps->Alone())
        return OpResultContinue;

    if (Loads)
        return OpResultContinue;

    if (LeaseWaiter)
        return OpResultContinue;

    if (State().Follower || Restored) {
        if (auto logl = Steps->Logger()->Log(ELnLev::Info)) {
            auto spent = AppData()->MonotonicTimeProvider->Now() - BootTimestamp;

            logl
                << NFmt::Do(State()) << " booting completed"
                << ", took " << NFmt::TDelay(spent);
        }

        return OpResultComplete;
    }

    return OpResultContinue;
}

TExecutorBootLogic::EOpResult TExecutorBootLogic::ReceiveRestored(TEvTablet::TEvRestored::TPtr &ev) {
    Y_UNUSED(ev);
    Restored = true;
    return CheckCompletion();
}

void TExecutorBootLogic::OnBlobLoaded(const TLogoBlobID& id, TString body, uintptr_t cookie) {
    Y_UNUSED(cookie);

    auto it = EntriesToLoad.find(id);

    Y_ABORT_UNLESS(it != EntriesToLoad.end(),
        "OnBlobLoaded with unexpected blob id %s", id.ToString().c_str());

    auto entry = std::move(it->second);

    EntriesToLoad.erase(it);

    entry->Feed(id, std::move(body));
    entry.Reset();

    Steps->Execute();
}

void TExecutorBootLogic::SeenBlob(const TLogoBlobID& id) {
    if (Result().GcLogic) {
        Result().GcLogic->HistoryCutter.SeenBlob(id);
    }
}

TExecutorBootLogic::EOpResult TExecutorBootLogic::Receive(::NActors::IEventHandle &ev)
{
    if (auto *msg = ev.CastAsLocal<TEvBlobStorage::TEvGetResult>()) {
        if (!LoadBlobQueue.ProcessResult(msg))
            return OpResultBroken;

    } else if (auto *msg = ev.CastAsLocal<NSharedCache::TEvResult>()) {
        if (EPageCollectionRequest(ev.Cookie) != EPageCollectionRequest::BootLogic)
            return OpResultUnhandled;

        auto it = Loads.find(msg->Origin.Get());
        if (it == Loads.end()) // could receive outdated results
            return OpResultUnhandled;

        // Remove step from loads first (so HandleBio may request more pages)
        auto step = std::move(it->second);
        Loads.erase(it);

        if (msg->Status == NKikimrProto::NODATA) {
            GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_boot_nodata", true)->Inc();
        }

        if (!step->HandleBio(*msg))
            return OpResultBroken;

        step.Drop();
        Steps->Execute();
    } else if (auto *msg = ev.CastAsLocal<TEvTablet::TEvLeaseDropped>()) {
        if (LeaseWaiter != ev.Sender) {
            return OpResultUnhandled;
        }

        LeaseWaiter = { };
    } else {
        return OpResultUnhandled;
    }

    return CheckCompletion();
}

TAutoPtr<NBoot::TResult> TExecutorBootLogic::ExtractState() noexcept {
    Y_ABORT_UNLESS(Result_->Database, "Looks like booting hasn't been done");
    for (const auto& [tableId, table] : Result_->Database->GetScheme().Tables) {
        for (const auto& part : Result_->Database->GetTableParts(tableId)) {
            if (!part || !part->Blobs) {
                continue;
            }
            for (const auto& glob : **(part->Blobs)) {
                Cerr << "TableParts ";
                SeenBlob(glob.Logo);
            }
        }
        if (table.ColdBorrow) {
            for (const auto& [_, room] : table.Rooms) {
                Result().GcLogic->HistoryCutter.BecomeUncertain(room.Main);
                Result().GcLogic->HistoryCutter.BecomeUncertain(room.Blobs);
                Result().GcLogic->HistoryCutter.BecomeUncertain(room.Outer);
            }
        }
    }
    return Result_;
}

void TExecutorBootLogic::Cancel() {
    if (LeaseWaiter) {
        Ops->Send(LeaseWaiter, new TEvents::TEvPoison);
    }
}

void TExecutorBootLogic::FollowersSyncComplete() {
    Y_ABORT_UNLESS(Result_);
    Y_ABORT_UNLESS(Result().GcLogic);
    Result().GcLogic->FollowersSyncComplete(true);
}

TExecutorCaches TExecutorBootLogic::DetachCaches() {
    if (Result_) {
        for (auto &x : Result().PageCaches)
            State().PageCaches[x->Id] = x;
    }
    return TExecutorCaches{
        .PageCaches = std::move(State().PageCaches),
        .TxStatusCaches = std::move(State().TxStatusCaches),
    };
}

}}

