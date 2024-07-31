#pragma once

#include "flat_scan_feed.h"
#include "flat_scan_events.h"
#include "flat_scan_eggs.h"
#include "flat_scan_spent.h"
#include "flat_bio_events.h"
#include "flat_fwd_env.h"
#include "util_fmt_logger.h"
#include "util_fmt_desc.h"
#include "shared_sausagecache.h"
#include "flat_part_store.h"
#include "flat_load_blob_queue.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/cast.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NOps {

    class TDriver final
            : public ::NActors::TActor<TDriver>
            , private NTable::TFeed
            , private NTable::IDriver
            , private ILoadBlob
    {
    public:
        using TSubset = NTable::TSubset;
        using TPartView = NTable::TPartView;
        using TPartStore = NTable::TPartStore;
        using TColdPart = NTable::TColdPart;
        using TColdPartStore = NTable::TColdPartStore;
        using TEnv = NTable::NFwd::TEnv;
        using TSpent = NTable::TSpent;
        using IScan = NTable::IScan;
        using EScan = NTable::EScan;
        using EAbort = NTable::EAbort;
        using ELnLev = NUtil::ELnLev;

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TABLET_OPS_HOST_A;
        }

        TDriver(ui64 serial, TAutoPtr<IScan> scan, TConf args, THolder<TScanSnapshot> snapshot)
            : TActor(&TDriver::StateBoot)
            , NTable::TFeed(scan.Release(), *snapshot->Subset, snapshot->Snapshot)
            , Serial(serial)
            , Args(args)
            , Snapshot(std::move(snapshot))
            , MaxCyclesPerIteration(/* 10ms */ (NHPTimer::GetCyclesPerSecond() + 99) / 100)
        {
        }

        ~TDriver()
        {
            /* Correct actors shutdown hasn't been implemented in
                kikimr, thus actors may be destructed in incompleted state
                and dtor cannot be used for completeness checkups.

                Moreover, special workaround is required in case of sudden
                actor system shutdown happens before Bootstrap(...).
            */

            if (Scan && Spent == nullptr)
                delete DetachScan();
        }

        void Describe(IOutputStream &out) const noexcept override
        {
            out
                << "Scan{" << Serial << " on " << Snapshot->Table
                << ", " << NFmt::Do(*Scan) << "}";
        }

    private:
        struct TEvPrivate {
            enum EEv {
                EvLoadBlob = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
                EvBlobLoaded,
                EvLoadPages,
                EvPartLoaded,
                EvPartFailed,
            };

            struct TEvLoadBlob : public TEventLocal<TEvLoadBlob, EvLoadBlob> {
                TLogoBlobID BlobId;
                ui32 Group;

                TEvLoadBlob(const TLogoBlobID& blobId, ui32 group)
                    : BlobId(blobId)
                    , Group(group)
                { }
            };

            struct TEvBlobLoaded : public TEventLocal<TEvBlobLoaded, EvBlobLoaded> {
                TLogoBlobID BlobId;
                TString Body;

                TEvBlobLoaded(const TLogoBlobID& blobId, TString body)
                    : BlobId(blobId)
                    , Body(std::move(body))
                { }
            };

            struct TEvLoadPages : public TEventLocal<TEvLoadPages, EvLoadPages> {
                TAutoPtr<NPageCollection::TFetch> Request;

                TEvLoadPages(TAutoPtr<NPageCollection::TFetch> request)
                    : Request(std::move(request))
                { }
            };

            struct TEvPartLoaded : public TEventLocal<TEvPartLoaded, EvPartLoaded> {
                TPartView Part;

                TEvPartLoaded(TPartView part)
                    : Part(std::move(part))
                { }
            };

            struct TEvPartFailed : public TEventLocal<TEvPartFailed, EvPartFailed> {
                const TLogoBlobID Label;

                TEvPartFailed(const TLogoBlobID& label)
                    : Label(label)
                { }
            };
        };

    private:
        class TColdPartLoader : public ::NActors::TActorBootstrapped<TColdPartLoader> {
        public:
            TColdPartLoader(TActorId owner, TIntrusiveConstPtr<TColdPartStore> part)
                : Owner(owner)
                , Part(std::move(part))
            { }

            void Bootstrap() {
                PageCollectionLoaders.reserve(Part->LargeGlobIds.size());
                for (ui64 slot = 0; slot < Part->LargeGlobIds.size(); ++slot) {
                    const ui32 group = Part->LargeGlobIds[slot].Group;
                    auto& loader = PageCollectionLoaders.emplace_back(Part->LargeGlobIds[slot]);
                    for (const auto& blobId : loader.GetBlobs()) {
                        Send(Owner, new TEvPrivate::TEvLoadBlob(blobId, group), 0, slot);
                    }
                }
                PageCollections.resize(PageCollectionLoaders.size());
                PageCollectionsLeft = PageCollectionLoaders.size();
                Become(&TThis::StateLoadPageCollections);
            }

        private:
            STRICT_STFUNC(StateLoadPageCollections, {
                sFunc(TEvents::TEvPoison, PassAway);
                hFunc(TEvPrivate::TEvBlobLoaded, Handle);
            });

            void Handle(TEvPrivate::TEvBlobLoaded::TPtr& ev) {
                auto* msg = ev->Get();
                ui64 slot = ev->Cookie;
                Y_ABORT_UNLESS(slot < PageCollections.size());
                Y_ABORT_UNLESS(slot < PageCollectionLoaders.size());
                Y_ABORT_UNLESS(!PageCollections[slot]);
                auto& loader = PageCollectionLoaders[slot];
                if (loader.Apply(msg->BlobId, std::move(msg->Body))) {
                    TIntrusiveConstPtr<NPageCollection::IPageCollection> pack =
                        new NPageCollection::TPageCollection(Part->LargeGlobIds[slot], loader.ExtractSharedData());
                    PageCollections[slot] = new TPrivatePageCache::TInfo(std::move(pack));
                    Y_ABORT_UNLESS(PageCollectionsLeft > 0);
                    if (0 == --PageCollectionsLeft) {
                        PageCollectionLoaders.clear();
                        StartLoader();
                    }
                }
            }

        private:
            void StartLoader() {
                Y_ABORT_UNLESS(!Loader);
                Loader.emplace(
                    std::move(PageCollections),
                    Part->Legacy,
                    Part->Opaque,
                    TVector<TString>{ },
                    Part->Epoch);
                Become(&TThis::StateLoadPart);

                RunLoader();
            }

            void RunLoader() {
                for (auto req : Loader->Run(false)) {
                    Send(Owner, new TEvPrivate::TEvLoadPages(std::move(req)));
                    ++ReadsLeft;
                }

                if (!ReadsLeft) {
                    TPartView partView = Loader->Result();
                    Send(Owner, new TEvPrivate::TEvPartLoaded(std::move(partView)));
                    return PassAway();
                }
            }

            STRICT_STFUNC(StateLoadPart, {
                sFunc(TEvents::TEvPoison, PassAway);
                hFunc(NSharedCache::TEvResult, Handle);
            });

            void Handle(NSharedCache::TEvResult::TPtr& ev) {
                auto* msg = ev->Get();
                if (msg->Status != NKikimrProto::OK) {
                    Send(Owner, new TEvPrivate::TEvPartFailed(Part->Label));
                    return PassAway();
                }

                Y_ABORT_UNLESS(ReadsLeft > 0);
                --ReadsLeft;

                Y_ABORT_UNLESS(Loader);
                Loader->Save(msg->Cookie, msg->Loaded);

                if (ReadsLeft == 0) {
                    RunLoader();
                }
            }

        private:
            TActorId Owner;
            TIntrusiveConstPtr<TColdPartStore> Part;
            TVector<TIntrusivePtr<TPrivatePageCache::TInfo>> PageCollections;
            TVector<NPageCollection::TLargeGlobIdRestoreState> PageCollectionLoaders;
            size_t PageCollectionsLeft = 0;
            std::optional<NTable::TLoader> Loader;
            size_t ReadsLeft = 0;
        };

    private:
        void MakeCache() noexcept
        {
            NTable::NFwd::TConf conf;

            conf.AheadLo = Args.AheadLo;
            conf.AheadHi = Args.AheadHi;

            if (Conf.ReadAheadLo != Max<ui64>() && Conf.ReadAheadLo <= conf.AheadLo) {
                 conf.AheadLo = Conf.ReadAheadLo;
            }

            if (Conf.ReadAheadHi != Max<ui64>() && Conf.ReadAheadHi <= conf.AheadHi) {
                 conf.AheadHi = Conf.ReadAheadHi;
            }

            conf.AheadLo = Min(conf.AheadLo, conf.AheadHi);

            conf.Trace = Args.Trace;
            conf.Edge = Conf.LargeEdge;
            conf.Tablet = Args.Tablet;

            Cache = new TEnv(conf, Subset);

            BlobQueue.Config.TabletID = Args.Tablet;

            switch (Args.ReadPrio) {
                case NBlockIO::EPriority::None:
                case NBlockIO::EPriority::Fast:
                    BlobQueue.Config.ReadPrio = NKikimrBlobStorage::FastRead;
                    break;
                case NBlockIO::EPriority::Bulk:
                case NBlockIO::EPriority::Bkgr: /* switch to LowRead in the future */
                    BlobQueue.Config.ReadPrio = NKikimrBlobStorage::AsyncRead;
                    break;
                case NBlockIO::EPriority::Low:
                    BlobQueue.Config.ReadPrio = NKikimrBlobStorage::LowRead;
                    break;
            }
        }

        NTable::IPages* MakeEnv() noexcept override
        {
            if (Resets++ != 0) {
                Cache->Reset();
                for (const auto& pr : ColdPartLoaded) {
                    Cache->AddCold(pr.second);
                }
            }

            return Cache.Get();
        }

        TPartView LoadPart(const TIntrusiveConstPtr<TColdPart>& part) noexcept override
        {
            const auto label = part->Label;
            auto itLoaded = ColdPartLoaded.find(label);
            if (itLoaded != ColdPartLoaded.end()) {
                // Return part that is already loaded
                return itLoaded->second;
            }

            auto itLoader = ColdPartLoaders.find(label);
            if (itLoader == ColdPartLoaders.end()) {
                // Create a loader for this new part
                TIntrusiveConstPtr<TColdPartStore> partStore = dynamic_cast<TColdPartStore*>(const_cast<TColdPart*>(part.Get()));
                Y_VERIFY_S(partStore, "Cannot load unsupported part " << NFmt::Do(*part));
                ColdPartLoaders[label] = RegisterWithSameMailbox(new TColdPartLoader(SelfId(), std::move(partStore)));
            }

            // Return empty TPartView to signal loader is still in progress
            return { };
        }

        bool MayProgress() noexcept {
            return Cache->MayProgress() && ColdPartLoaders.empty();
        }

        void Touch(EScan scan) noexcept override
        {
            Y_ABORT_UNLESS(Depth == 0, "Touch(..) is used from invalid context");

            switch (scan) {
                case EScan::Feed:
                case EScan::Reset:
                    Resume(scan);

                    if (MayProgress()) {
                        return React();
                    }

                    return Spent->Alter(/* resources not available */ false);

                case EScan::Final:
                    return Terminate(EAbort::None);

                case EScan::Sleep:
                    Y_ABORT("Scan actor got an unexpected EScan::Sleep");
            }

            Y_ABORT("Scan actor got an unexpected EScan value");
        }

        void Registered(TActorSystem *sys, const TActorId &owner) override
        {
            Owner = owner;
            Logger = new NUtil::TLogger(sys, NKikimrServices::TABLET_OPS_HOST);
            sys->Send(SelfId(), new TEvents::TEvBootstrap);
        }

        STRICT_STFUNC(StateBoot, {
            cFunc(TEvents::TEvBootstrap::EventType, Bootstrap);
        });

        STRICT_STFUNC(StateWork, {
            hFunc(TEvContinue, Handle);
            hFunc(TEvPrivate::TEvLoadBlob, Handle);
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
            hFunc(TEvPrivate::TEvLoadPages, Handle);
            hFunc(NBlockIO::TEvStat, Handle);
            hFunc(TEvPrivate::TEvPartLoaded, Handle);
            hFunc(TEvPrivate::TEvPartFailed, Handle);
            hFunc(NSharedCache::TEvResult, Handle);
            IgnoreFunc(NSharedCache::TEvUpdated);
            cFunc(TEvents::TEvUndelivered::EventType, HandleUndelivered);
            cFunc(TEvents::TEvPoison::EventType, HandlePoison);
        });

        void Bootstrap() noexcept
        {
            Y_ABORT_UNLESS(!Spent, "Talble scan actor bootstrapped twice");

            Spent = new TSpent(TAppData::TimeProvider.Get());

            if (auto logl = Logger->Log(ELnLev::Info)) {
                logl
                    << NFmt::Do(*this) << " begin on " << NFmt::Do(Subset);
            }

            Become(&TDriver::StateWork);

            {
                TGuard<ui64, NUtil::TIncDecOps<ui64>> guard(Depth);

                auto hello = Scan->Prepare(this, Subset.Scheme);

                Conf = hello.Conf;

                guard.Release();

                MakeCache();

                if (hello.Scan != EScan::Sleep)
                    Touch(hello.Scan);
            }
        }

        /**
         * Helper for calculating TEvScanStat
         */
        struct TStatState {
            ui64 LastSeen;
            ui64 LastSkipped;
            NHPTimer::STime StartTime;
            NHPTimer::STime EndTime;
            ui64 Seen = 0;
            ui64 Skipped = 0;

            TStatState(ui64 seen, ui64 skipped)
                : LastSeen(seen)
                , LastSkipped(skipped)
            {
                GetTimeFast(&StartTime);
                EndTime = StartTime;
            }

            ui64 UpdateRows(ui64 seen, ui64 skipped) {
                Seen += (seen - LastSeen);
                Skipped += (skipped - LastSkipped);
                ui64 total = (seen - LastSeen) + (skipped - LastSkipped);
                LastSeen = seen;
                LastSkipped = skipped;
                return total;
            }

            void UpdateCycles() {
                GetTimeFast(&EndTime);
            }

            NHPTimer::STime ElapsedCycles() const {
                return EndTime - StartTime;
            }
        };

        void SendStat(const TStatState& stat)
        {
            ui64 elapsedUs = 1000000. * NHPTimer::GetSeconds(stat.ElapsedCycles());

            SendToOwner(new TEvScanStat(elapsedUs, stat.Seen, stat.Skipped));
        }

        void React() noexcept
        {
            TGuard<ui64, NUtil::TIncDecOps<ui64>> guard(Depth);

            Y_DEBUG_ABORT_UNLESS(MayProgress(), "React called with non-ready cache");
            Y_ABORT_UNLESS(Scan, "Table scan op has been finalized");

            TStatState stat(Seen, Skipped);
            ui64 processed = 0;
            bool yield = false;

            for (;;) {
                // Check elapsed time every N rows
                if (processed >= MinRowsPerCheck) {
                    stat.UpdateCycles();
                    if (stat.ElapsedCycles() >= MaxCyclesPerIteration) {
                        // Yield to allow other actors to use this thread
                        if (!ContinueInFly) {
                            SendToSelf(MakeHolder<TEvContinue>());
                            ContinueInFly = true;
                        }
                        yield = true;
                        break;
                    }
                    processed = 0;
                }

                const auto ready = Process();

                processed += stat.UpdateRows(Seen, Skipped);

                if (ready == NTable::EReady::Gone) {
                    Terminate(EAbort::None);
                    stat.UpdateCycles();
                    SendStat(stat);
                    return;
                }

                while (auto req = Cache->GrabFetches()) {
                    if (auto logl = Logger->Log(ELnLev::Debug))
                        logl << NFmt::Do(*this) << " " << NFmt::Do(*req);

                    const auto label = req->PageCollection->Label();
                    if (PrivateCollections.contains(label)) {
                        Send(MakeSharedPageCacheId(), new NSharedCache::TEvRequest(Args.ReadPrio, req, SelfId()));
                        ForwardedSharedRequests = true;
                    } else {
                        SendToOwner(new NSharedCache::TEvRequest(Args.ReadPrio, req, Owner), true);
                    }
                }

                if (ready == NTable::EReady::Page)
                    break; /* pages required or just suspended */

                if (!MayProgress()) {
                    // We must honor EReady::Gone from an implicit callback
                    if (ImplicitPageFault() == NTable::EReady::Gone) {
                        Terminate(EAbort::None);
                        stat.UpdateCycles();
                        SendStat(stat);
                        return;
                    }

                    break;
                }
            }

            Spent->Alter(MayProgress());

            if (!yield) {
                stat.UpdateCycles();
            }
            SendStat(stat);
        }

        void Handle(TEvContinue::TPtr&) noexcept
        {
            Y_ABORT_UNLESS(ContinueInFly);

            ContinueInFly = false;

            if (!IsPaused() && MayProgress()) {
                React();
            }
        }

        void Handle(TEvPrivate::TEvLoadBlob::TPtr& ev) noexcept
        {
            Y_ABORT_UNLESS(ev->Sender);
            auto* msg = ev->Get();

            auto& req = BlobQueueRequests.emplace_back();
            req.Sender = ev->Sender;
            req.Cookie = ev->Cookie;
            ui64 reqId = BlobQueueRequestsOffset + BlobQueueRequests.size() - 1;

            BlobQueue.Enqueue(msg->BlobId, msg->Group, this, reqId);
            BlobQueue.SendRequests(SelfId());
        }

        void Handle(TEvBlobStorage::TEvGetResult::TPtr& ev) noexcept
        {
            if (!BlobQueue.ProcessResult(ev->Get())) {
                return Terminate(EAbort::Host);
            }

            BlobQueue.SendRequests(SelfId());
        }

        void OnBlobLoaded(const TLogoBlobID& id, TString body, uintptr_t cookie) noexcept override
        {
            Y_ABORT_UNLESS(cookie >= BlobQueueRequestsOffset);
            size_t idx = cookie - BlobQueueRequestsOffset;
            Y_ABORT_UNLESS(idx < BlobQueueRequests.size());
            auto& req = BlobQueueRequests[idx];
            Y_ABORT_UNLESS(req.Sender);
            Send(req.Sender, new TEvPrivate::TEvBlobLoaded(id, std::move(body)), 0, req.Cookie);
            req.Sender = {};
            while (!BlobQueueRequests.empty() && !BlobQueueRequests.front().Sender) {
                BlobQueueRequests.pop_front();
                ++BlobQueueRequestsOffset;
            }
        }

        void Handle(TEvPrivate::TEvLoadPages::TPtr& ev) noexcept
        {
            auto* msg = ev->Get();

            TActorIdentity(ev->Sender).Send(
                MakeSharedPageCacheId(),
                new NSharedCache::TEvRequest(Args.ReadPrio, std::move(msg->Request), SelfId()),
                ev->Flags, ev->Cookie);
            ForwardedSharedRequests = true;
        }

        void Handle(NBlockIO::TEvStat::TPtr& ev) noexcept
        {
            ev->Rewrite(ev->GetTypeRewrite(), Owner);
            TActivationContext::Send(ev.Release());
        }

        void Handle(TEvPrivate::TEvPartLoaded::TPtr& ev) noexcept
        {
            auto* msg = ev->Get();

            const auto label = msg->Part->Label;
            ColdPartLoaders.erase(label);

            auto& partView = ColdPartLoaded[label];
            partView = std::move(msg->Part);

            auto* partStore = partView.As<TPartStore>();
            Y_ABORT_UNLESS(partStore);

            for (auto& cache : partStore->PageCollections) {
                PrivateCollections.insert(cache->Id);
            }
            if (auto& cache = partStore->Pseudo) {
                PrivateCollections.insert(cache->Id);
            }

            Cache->AddCold(partView);

            if (MayProgress()) {
                Spent->Alter(true /* resource available again */);
                React();
            }
        }

        void Handle(TEvPrivate::TEvPartFailed::TPtr& ev) noexcept
        {
            auto* msg = ev->Get();

            const auto label = msg->Label;
            ColdPartLoaders.erase(label);

            Terminate(EAbort::Host);
        }

        void Handle(NSharedCache::TEvResult::TPtr& ev) noexcept
        {
            auto& msg = *ev->Get();

            auto lvl = msg.Status ? ELnLev::Error : ELnLev::Debug;

            if (auto logl = Logger->Log(lvl))
                logl << NFmt::Do(*this) << " " << NFmt::Do(msg);

            if (msg.Status != NKikimrProto::OK) {
                if (msg.Status == NKikimrProto::NODATA) {
                    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_scan_nodata", true)->Inc();
                }

                return Terminate(EAbort::Host);
            }

            // TODO: would want to postpone pinning until usage
            TVector<NPageCollection::TLoadedPage> pinned(Reserve(msg.Loaded.size()));
            for (auto& loaded : msg.Loaded) {
                pinned.emplace_back(loaded.PageId, TPinnedPageRef(loaded.Page).GetData());
            }

            Cache->DoSave(std::move(msg.Origin), msg.Cookie, pinned);

            if (MayProgress()) {
                Spent->Alter(true /* resource available again */);
                React();
            }
        }

        void HandleUndelivered() noexcept
        {
            Terminate(EAbort::Lost);
        }

        void HandlePoison() noexcept
        {
            Terminate(EAbort::Term);
        }

        void Terminate(EAbort abort) noexcept
        {
            auto trace = Args.Trace ? Cache->GrabTraces() : nullptr;

            if (auto logl = Logger->Log(ELnLev::Info)) {
                logl
                    << NFmt::Do(*this) << " end=" << ui32(abort)
                    << ", " << Seen << "r seen, " << NFmt::Do(Cache->Stats())
                    << ", bio " << NFmt::If(Spent.Get());

                if (trace)
                    logl
                        << ", trace " << trace->Seen << " of " << trace->Total
                        << " ~" << trace->Sieve.size() << "p";
            }

            /* Each Flatten should have its trace on the same position */

            Y_ABORT_UNLESS(!trace || trace->Sieve.size() == Subset.Flatten.size() + 1);

            /* After invocation of Finish(...) scan object is left on its
                own and it has to handle self deletion if required. */

            auto prod = DetachScan()->Finish(abort);

            if (abort != EAbort::Lost) {
                auto ev = new TEvResult(Serial, abort, std::move(Snapshot), prod);

                ev->Trace = std::move(trace);

                SendToOwner(ev);
            }

            for (const auto& pr : ColdPartLoaders) {
                Send(pr.second, new TEvents::TEvPoison);
            }

            if (ForwardedSharedRequests) {
                Send(MakeSharedPageCacheId(), new NSharedCache::TEvUnregister);
            }

            PassAway();
        }

        void SendToSelf(THolder<IEventBase> event) noexcept
        {
            Send(SelfId(), event.Release());
        }

        void SendToOwner(TAutoPtr<IEventBase> event, bool nack = false) noexcept
        {
            ui32 flags = nack ? NActors::IEventHandle::FlagTrackDelivery : 0;

            Send(Owner, event.Release(), flags);
        }

    private:
        struct TBlobQueueRequest {
            TActorId Sender;
            ui64 Cookie;
        };

    private:
        const ui64 Serial = 0;
        const NOps::TConf Args;
        TAutoPtr<NUtil::ILogger> Logger;
        TActorId Owner;

        THolder<TScanSnapshot> Snapshot;
        TAutoPtr<TEnv> Cache;       /* NFwd scan read ahead cache   */
        TAutoPtr<TSpent> Spent;     /* NBlockIO read blockage stats */
        ui64 Depth = 0;
        ui64 Resets = 0;

        THashMap<TLogoBlobID, TActorId> ColdPartLoaders;
        THashMap<TLogoBlobID, TPartView> ColdPartLoaded;
        THashSet<TLogoBlobID> PrivateCollections;

        TLoadBlobQueue BlobQueue;
        TDeque<TBlobQueueRequest> BlobQueueRequests;
        ui64 BlobQueueRequestsOffset = 0;

        bool ForwardedSharedRequests = false;
        bool ContinueInFly = false;

        const NHPTimer::STime MaxCyclesPerIteration;
        static constexpr ui64 MinRowsPerCheck = 1000;
    };

}
}
}
