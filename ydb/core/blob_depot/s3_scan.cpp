#include "s3.h"
#include "coro_tx.h"
#include "s3_collection.h"
#include "s3_request_timer.h"

#include <ydb/core/wrappers/abstract.h>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;

    struct TS3Manager::TEvScanFound : TEventLocal<TEvScanFound, TEvPrivate::EvScanFound> {
        std::vector<std::tuple<TString, ui64>> KeysWithoutPrefix;
        TString NextMarker;
        std::optional<TString> Error;

        TEvScanFound(std::vector<std::tuple<TString, ui64>>&& keysWithoutPrefix, TString nextMarker, std::optional<TString>&& error)
            : KeysWithoutPrefix(std::move(keysWithoutPrefix))
            , NextMarker(std::move(nextMarker))
            , Error(std::move(error))
        {}
    };

    class TS3Manager::TScannerActor : public TActorBootstrapped<TScannerActor> {
        const TActorId ParentId;
        const TActorId WrapperId;
        TS3Listing Listing;
        const TString Bucket;
        const TString LogId;
        TS3RequestTimer Timer;

    public:
        TScannerActor(TActorId parentId, TActorId wrapperId, TString prefix, TString marker, TString bucket, TString logId)
            : ParentId(parentId)
            , WrapperId(wrapperId)
            , Listing(prefix, std::move(marker))
            , Bucket(std::move(bucket))
            , LogId(std::move(logId))
        {}

        void Bootstrap() {
            IssueNextRequest();
            Become(&TThis::StateFunc);
        }

        void IssueNextRequest() {
            YDB_LOG_DEBUG("TScannerActor::IssueNextRequest",
                {"marker", "BDTS18"},
                {"id", LogId},
                {"prefix", Listing.GetPrefix()},
                {"requestMarker", Listing.GetMarker()});

            Timer.SendWithTimeout(WrapperId, new TEvExternalStorage::TEvListObjectsRequest(Listing.MakeRequest(Bucket)));
        }

        void Handle(TEvExternalStorage::TEvListObjectsResponse::TPtr ev) {
            Timer.Clear();
            auto& msg = *ev->Get();
            if (!msg.IsSuccess()) {
                FinishWithError(msg.GetError().GetMessage().c_str());
            } else {
                const auto& result = msg.Result.GetResult();
                std::vector<std::tuple<TString, ui64>> keysWithoutPrefix;
                if (auto error = Listing.ReadPage(result, [&](const TString&, TStringBuf name, ui64 len) {
                    keysWithoutPrefix.emplace_back(TString(name), len);
                })) {
                    return FinishWithError(std::move(*error));
                }

                Send(ParentId, new TEvScanFound(std::move(keysWithoutPrefix),
                    result.GetIsTruncated() ? Listing.GetMarker() : TString(), std::nullopt));
                PassAway();
            }
        }

        void HandleUndelivered() {
            FinishWithError("event undelivered");
        }

        void HandleTimeout() {
            if (Timer.OnWakeup()) {
                FinishWithError("S3 listing timed out");
            }
        }

        void FinishWithError(TString error) {
            Send(ParentId, new TEvScanFound({}, {}, std::move(error)));
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvExternalStorage::TEvListObjectsResponse, Handle)
            cFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    };

    struct TS3Manager::TEvProbeFinished : TEventLocal<TEvProbeFinished, TEvPrivate::EvS3ProbeFinished> {
        const EProbeResult Result;
        const TInstant HeadIssuedAt;

        TEvProbeFinished(EProbeResult result, TInstant headIssuedAt)
            : Result(result), HeadIssuedAt(headIssuedAt)
        {}
    };

    class TS3Manager::TUnfinishedPutProbe : public TActorBootstrapped<TUnfinishedPutProbe> {
        const TActorId Parent;
        const TActorId Router;
        const TString Bucket;
        const TString Key;
        TS3RequestTimer Timer;
        TInstant HeadIssuedAt;

    public:
        TUnfinishedPutProbe(TActorId parent, TActorId router, TString bucket, TString key)
            : Parent(parent), Router(router), Bucket(std::move(bucket)), Key(std::move(key))
        {}

        void Bootstrap() {
            Become(&TThis::StateWork);
            HeadIssuedAt = TActivationContext::Now();
            Timer.SendWithTimeout(Router, new TEvExternalStorage::TEvHeadObjectRequest(
                Aws::S3::Model::HeadObjectRequest().WithBucket(Bucket).WithKey(Key)));
        }

        void Handle(TEvExternalStorage::TEvHeadObjectResponse::TPtr ev) {
            if (ev->Get()->IsSuccess()) {
                Timer.SendWithTimeout(Router, new TEvExternalStorage::TEvDeleteObjectRequest(
                    Aws::S3::Model::DeleteObjectRequest().WithBucket(Bucket).WithKey(Key)));
            } else if (IsS3MissingObject(ev->Get()->GetError())) {
                Finish(EProbeResult::Missing);
            } else {
                YDB_LOG_WARN("Failed to probe unfinished S3 PUT", {"key", Key}, {"error", ev->Get()->GetError().GetMessage().c_str()});
                Finish(EProbeResult::Error);
            }
        }

        void Handle(TEvExternalStorage::TEvDeleteObjectResponse::TPtr ev) {
            if (!ev->Get()->IsSuccess()) {
                YDB_LOG_WARN("Failed to delete unfinished S3 PUT", {"key", Key}, {"error", ev->Get()->GetError().GetMessage().c_str()});
                return Finish(EProbeResult::Error);
            }
            Finish(EProbeResult::Deleted);
        }

        void Finish(EProbeResult result) {
            Send(Parent, new TEvProbeFinished(result, HeadIssuedAt));
            PassAway();
        }

        STRICT_STFUNC(StateWork,
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, Handle)
            hFunc(TEvExternalStorage::TEvDeleteObjectResponse, Handle)
            cFunc(TEvents::TSystem::Undelivered, [&] { Finish(EProbeResult::Error); })
            cFunc(TEvents::TSystem::Poison, PassAway)
            cFunc(TEvents::TSystem::Wakeup, [&] { if (Timer.OnWakeup()) { Finish(EProbeResult::Error); } })
        )
    };

    void TS3Manager::UpdatePutWatchCount(NTabletFlatExecutor::TTransactionContext& txc, bool added) {
        Y_ABORT_UNLESS(added || NumPutWatches);
        SetNumPutWatches(added ? NumPutWatches + 1 : NumPutWatches - 1);
        NIceDb::TNiceDb(txc.DB).Table<Schema::Config>().Key(Schema::Config::Key::Value)
            .Update<Schema::Config::NumPutWatches>(NumPutWatches);
    }

    void TS3Manager::EnqueuePutWatch(TS3Locator locator, TInstant retireAt, NTabletFlatExecutor::TTransactionContext& txc) {
        using T = Schema::S3PutWatches;
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::CoolingS3Puts>().Key(locator.Generation, locator.KeyId).Delete();
        db.Table<T>().Key(TActivationContext::Now().MicroSeconds(), locator.Generation, locator.KeyId).Update(
            NIceDb::TUpdate<T::Len>(locator.Len), NIceDb::TUpdate<T::RetireAt>(retireAt),
            NIceDb::TUpdate<T::Interval>(TDuration::Seconds(1)));
        UpdatePutWatchCount(txc, true);
    }

    void TS3Manager::SchedulePutProbe(TInstant when) {
        const auto at = Max(NextProbeAt, TActivationContext::Monotonic() + (when - TActivationContext::Now()));
        ProbeTimer.Schedule(at, Self->SelfId(), TEvPrivate::EvS3ProbeWakeup);
    }

    void TS3Manager::ReadDuePutWatches() {
        ReadingPutWatches = true;
        PutWatchPageChanged = false;
        Self->Execute(std::make_unique<TCoroTx>(Self, TTokens{{Self->Token}}, [this](TCoroTx::TContextBase& tx) {
            using T = Schema::S3PutWatches;
            static constexpr size_t PageSize = 32;
            std::deque<TPutWatch> page;
            TInstant next = TInstant::Max();
            for (;;) {
                auto rows = NIceDb::TNiceDb(tx.GetTxc()->DB).Table<T>().Range().Select();
                if (rows.IsReady()) {
                    const auto now = TActivationContext::Now();
                    while (rows.IsValid()) {
                        const auto due = TInstant::MicroSeconds(rows.GetValue<T::Due>());
                        if (due > now) {
                            next = due;
                            break;
                        }
                        page.push_back({TS3Locator{.Len = rows.GetValue<T::Len>(), .Generation = rows.GetValue<T::Generation>(),
                            .KeyId = rows.GetValue<T::KeyId>()}, due, rows.GetValue<T::RetireAt>(), rows.GetValue<T::Interval>()});
                        if (page.size() == PageSize || !rows.Next()) {
                            break; // consume any available due rows before asking for another page
                        }
                    }
                    break;
                }
                tx.RestartTx();
            }
            tx.FinishTx();
            ReadingPutWatches = false;
            DuePutWatches.swap(page);
            if (!DuePutWatches.empty() || PutWatchPageChanged) {
                RunPutProbe();
            } else if (next != TInstant::Max()) {
                SchedulePutProbe(next);
            }
        }));
    }

    void TS3Manager::RunPutProbe() {
        if (!Self->Data->IsLoaded() || !NumPutWatches) {
            return;
        }
        if (ReadingPutWatches) {
            PutWatchPageChanged = true;
        } else if (PutProbe) {
            return;
        } else if (TActivationContext::Monotonic() < NextProbeAt) {
            SchedulePutProbe(TActivationContext::Now());
        } else if (DuePutWatches.empty()) {
            ReadDuePutWatches();
        } else {
            NextProbeAt = TActivationContext::Monotonic() + TDuration::MilliSeconds(100);
            PutProbe = Self->Register(new TUnfinishedPutProbe(Self->SelfId(), WrapperId, Bucket,
                DuePutWatches.front().Locator.MakeObjectName(BasePath)));
        }
    }

    void TS3Manager::HandleProbeWakeup(TAutoPtr<IEventHandle> ev) {
        if (ProbeTimer.Consume(ev->Cookie)) {
            RunPutProbe();
        }
    }

    void TS3Manager::HandleProbeFinished(TAutoPtr<IEventHandle> ev) {
        Y_ABORT_UNLESS(ev->Sender == PutProbe && !DuePutWatches.empty());
        auto& msg = *ev->Get<TEvProbeFinished>();
        const auto watch = DuePutWatches.front();
        const bool retire = msg.Result == EProbeResult::Missing && msg.HeadIssuedAt >= watch.RetireAt;
        const auto now = TActivationContext::Now();
        auto next = now + watch.Interval;
        if (now < watch.RetireAt) {
            next = Min(next, watch.RetireAt);
        } else if (msg.Result != EProbeResult::Error) {
            next = now + TDuration::Seconds(1); // confirm a successful DELETE or pre-horizon HEAD
        }
        Self->Execute(std::make_unique<TCoroTx>(Self, TTokens{{Self->Token}}, [this, watch, retire, next](TCoroTx::TContextBase& tx) {
            using T = Schema::S3PutWatches;
            NIceDb::TNiceDb db(tx.GetTxc()->DB);
            auto& loc = watch.Locator;
            db.Table<T>().Key(watch.Due.MicroSeconds(), loc.Generation, loc.KeyId).Delete();
            const bool startReconciliation = retire && !NextOrphanScan;
            if (retire) {
                UpdatePutWatchCount(*tx, false);
                if (startReconciliation) {
                    // Even a successful post-horizon HEAD cannot rule out a very slow remote PUT.
                    NextOrphanScan = TActivationContext::Now() + TDuration::Minutes(1);
                    db.Table<Schema::Config>().Key(Schema::Config::Key::Value)
                        .Update<Schema::Config::NextOrphanScan>(NextOrphanScan);
                }
            } else {
                db.Table<T>().Key(next.MicroSeconds(), loc.Generation, loc.KeyId).Update(NIceDb::TUpdate<T::Len>(loc.Len),
                    NIceDb::TUpdate<T::RetireAt>(watch.RetireAt),
                    NIceDb::TUpdate<T::Interval>(Min(TDuration::Hours(1), watch.Interval * 2)));
            }
            tx.FinishTx();
            if (startReconciliation && !ScannerActorId && OrphanScanEnabled) {
                ScheduleScan(NextOrphanScan - TActivationContext::Now());
            }
            DuePutWatches.pop_front();
            PutProbe = {};
            RunPutProbe();
        }));
    }

    class TS3Manager::TTxProcessScannedKeys : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        THashSet<TS3Locator> UnprocessedLocators;
        THashSet<TS3Locator> LocatorsToDelete;
        const TString NextMarker;

    public:
        TTxProcessScannedKeys(TBlobDepot *self, THashSet<TS3Locator>&& locators, TString nextMarker)
            : TTransactionBase(self)
            , UnprocessedLocators(std::move(locators))
            , NextMarker(std::move(nextMarker))
        {}

        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_PROCESS_SCANNED_KEYS; }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);

            for (auto it = UnprocessedLocators.begin(); it != UnprocessedLocators.end(); ) {
                if (!Self->S3Manager->CanCollectScannedObject(*it)) {
                    UnprocessedLocators.erase(it++);
                    continue;
                }
                auto row = db.Table<Schema::TrashS3>().Key(it->Generation, it->KeyId).Select();
                if (!row.IsReady()) {
                    return false;
                } else if (!row.IsValid()) {
                    const auto stage = db.Table<Schema::CoolingS3Puts>().Key(it->Generation, it->KeyId).Select();
                    if (!stage.IsReady()) {
                        return false;
                    } else if (!stage.IsValid()) {
                        LocatorsToDelete.insert(*it);
                    }
                }
                UnprocessedLocators.erase(it++);
            }

            Y_ABORT_UNLESS(UnprocessedLocators.empty());

            for (const TS3Locator& locator : LocatorsToDelete) {
                db.Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Update<Schema::TrashS3::Len>(locator.Len);
            }

            auto& manager = *Self->S3Manager;
            manager.ScanMarker = NextMarker;
            if (manager.NextOrphanScan) {
                // Commit the cursor with any trash found on this page. A crash can replay
                // the page but cannot advance past an orphan that still needs deletion.
                manager.NextOrphanScan = TActivationContext::Now() +
                    (NextMarker.empty() ? TDuration::Days(1) : TDuration::Minutes(1));
                db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                    NIceDb::TUpdate<Schema::Config::OrphanScanMarker>(NextMarker),
                    NIceDb::TUpdate<Schema::Config::NextOrphanScan>(manager.NextOrphanScan));
            }
            return true;
        }

        void Complete(const TActorContext&) override {
            for (const auto& locator : LocatorsToDelete) {
                Self->S3Manager->AddTrashToCollect(locator);
            }
            auto& manager = *Self->S3Manager;
            manager.ScannerActorId = {};
            if (manager.NextOrphanScan) {
                manager.ScheduleScan(manager.NextOrphanScan - TActivationContext::Now());
            } else if (!NextMarker.empty()) {
                manager.RunScannerActor();
            }
        }
    };

    void TS3Manager::ScheduleScan(TDuration delay) {
        if (!std::exchange(ScanScheduled, true)) {
            TActivationContext::Schedule(delay, new IEventHandle(TEvPrivate::EvResumeS3Scan, 0, Self->SelfId(), {}, nullptr, 0));
        }
    }

    void TS3Manager::RunScannerActor() {
        Y_ABORT_UNLESS(OrphanScanEnabled && !ScannerActorId);
        ScannerActorId = Self->Register(new TScannerActor(Self->SelfId(), WrapperId, BasePath, ScanMarker, Bucket, Self->GetLogId()));
    }

    void TS3Manager::HandleResumeScanner() {
        ScanScheduled = false;
        RunScannerActor();
    }

    void TS3Manager::HandleScanner(TAutoPtr<IEventHandle> ev) {
        STRICT_STFUNC_BODY(
            hFunc(TEvScanFound, [&](TEvScanFound::TPtr ev) {
                auto& msg = *ev->Get();

                YDB_LOG_DEBUG("TEvScanFound received",
                    {"marker", "BDTS17"},
                    {"id", Self->GetLogId()},
                    {"nextMarker", msg.NextMarker},
                    {"error", msg.Error},
                    {"KeysWithoutPrefix.size", msg.KeysWithoutPrefix.size()});

                Y_ABORT_UNLESS(ScannerActorId);
                Y_ABORT_UNLESS(ev->Sender == ScannerActorId);

                if (msg.Error) {
                    YDB_LOG_WARN("Scanner error",
                        {"marker", "BDTS14"},
                        {"id", Self->GetLogId()},
                        {"error", msg.Error});
                    ScannerActorId = {};
                    return ScheduleScan(TDuration::Minutes(1));
                }

                THashSet<TS3Locator> trash;

                for (const auto& [key, len] : msg.KeysWithoutPrefix) {
                    TString error;
                    if (const auto& locator = TS3Locator::FromObjectName(key, len, &error)) {
                        const bool allow = CanCollectScannedObject(*locator);
                        YDB_LOG_DEBUG("TEvScanFound: found key",
                            {"marker", "BDTS15"},
                            {"id", Self->GetLogId()},
                            {"locator", *locator},
                            {"allow", allow},
                            {"error", error});
                        YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "Scan_S3_found_key",
                            {"marker", "BDEV35"},
                            {"BDT", Self->TabletID()},
                            {"locator", *locator},
                            {"allow", allow});
                        if (allow) {
                            trash.insert(*locator);
                        }
                    } else {
                        YDB_LOG_WARN("TEvScanFound: incorrect key name",
                            {"marker", "BDTS16"},
                            {"id", Self->GetLogId()},
                            {"key", key},
                            {"len", len});
                    }
                }

                Self->Execute(std::make_unique<TTxProcessScannedKeys>(Self, std::move(trash), std::move(msg.NextMarker)));
            })
        )
    }

} // NKikimr::NBlobDepot
