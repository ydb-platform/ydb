#include "s3.h"

#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;
    using TEvExternalStorage = NWrappers::TEvExternalStorage;

    struct TS3Manager::TEvDeleteResult : TEventLocal<TEvDeleteResult, TEvPrivate::EvDeleteResult> {
        std::vector<TS3Locator> LocatorsOk;
        std::vector<TS3Locator> LocatorsThrottled;
        std::vector<TS3Locator> LocatorsError;

        TEvDeleteResult(std::vector<TS3Locator>&& locatorsOk, std::vector<TS3Locator>&& locatorsThrottled,
                std::vector<TS3Locator>&& locatorsError)
            : LocatorsOk(std::move(locatorsOk))
            , LocatorsThrottled(std::move(locatorsThrottled))
            , LocatorsError(std::move(locatorsError))
        {}
    };

    static bool IsSlowDown(const Aws::S3::S3Error& error) {
        return error.GetErrorType() == Aws::S3::S3Errors::SLOW_DOWN
            || error.GetExceptionName() == "SlowDown";
    }

    class TS3Manager::TDeleterActor : public TActor<TDeleterActor> {
        TActorId ParentId;
        THashMap<TString, TS3Locator> Locators;
        TString LogId;
        const ui64 TabletId;

    public:
        TDeleterActor(TActorId parentId, THashMap<TString, TS3Locator> locators, TString logId, ui64 tabletId)
            : TActor(&TThis::StateFunc)
            , ParentId(parentId)
            , Locators(locators)
            , LogId(std::move(logId))
            , TabletId(tabletId)
        {}

        void Handle(TEvExternalStorage::TEvDeleteObjectResponse::TPtr ev) {
            auto& msg = *ev->Get();
            if (msg.IsSuccess()) {
                Finish(std::nullopt);
            } else if (const auto& error = msg.GetError(); error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) {
                Finish(std::nullopt);
            } else if (IsSlowDown(error)) {
                Finish(error.GetMessage().c_str(), /*throttled=*/true);
            } else {
                Finish(error.GetMessage().c_str());
            }
        }

        void Handle(TEvExternalStorage::TEvDeleteObjectsResponse::TPtr ev) {
            auto& msg = *ev->Get();

            std::vector<TS3Locator> locatorsOk;
            std::vector<TS3Locator> locatorsThrottled;
            std::vector<TS3Locator> locatorsError;
            bool requestThrottled = false;

            if (msg.IsSuccess()) {
                auto& result = msg.Result.GetResult();
                for (const Aws::S3::Model::DeletedObject& deleted : result.GetDeleted()) {
                    if (deleted.KeyHasBeenSet()) {
                        if (const auto it = Locators.find(deleted.GetKey().c_str()); it != Locators.end()) {
                            BDEV(BDEV29, "deleted_from_S3", (BDT, TabletId), (Locator, it->second));
                            locatorsOk.push_back(it->second);
                            Locators.erase(it);
                        } else {
                            STLOG(PRI_WARN, BLOB_DEPOT, BDTS09, "key not found", (Id, LogId),
                                (Key, deleted.KeyHasBeenSet() ? std::make_optional<TString>(deleted.GetKey().c_str()) : std::nullopt));
                        }
                    } else {
                        STLOG(PRI_WARN, BLOB_DEPOT, BDTS10, "key not set", (Id, LogId));
                    }
                }
                for (const Aws::S3::Model::Error& error : result.GetErrors()) {
                    if (error.KeyHasBeenSet() && error.GetCode() == "NoSuchKey") { // this key has already been deleted
                        if (const auto it = Locators.find(error.GetKey().c_str()); it != Locators.end()) {
                            BDEV(BDEV30, "deleted_from_S3:NoSuchKey", (BDT, TabletId), (Locator, it->second));
                            locatorsOk.push_back(it->second);
                            Locators.erase(it);
                        }
                    } else if (error.KeyHasBeenSet() && error.GetCode() == "SlowDown") {
                        if (const auto it = Locators.find(error.GetKey().c_str()); it != Locators.end()) {
                            STLOG(PRI_WARN, BLOB_DEPOT, BDTS19, "S3 SlowDown for object", (Id, LogId),
                                (Locator, it->second), (Error, error.GetMessage().c_str()));
                            BDEV(BDEV39, "deleted_from_S3:SlowDown", (BDT, TabletId), (Locator, it->second));
                            locatorsThrottled.push_back(it->second);
                            Locators.erase(it);
                        }
                    } else {
                        STLOG(PRI_WARN, BLOB_DEPOT, BDTS11, "failed to delete object from S3", (Id, LogId),
                            (Key, error.KeyHasBeenSet() ? std::make_optional<TString>(error.GetKey().c_str()) : std::nullopt),
                            (Error, error.GetMessage().c_str()));
                    }
                }
            } else if (IsSlowDown(msg.GetError())) {
                requestThrottled = true;
                STLOG(PRI_WARN, BLOB_DEPOT, BDTS20, "S3 SlowDown for batch delete", (Id, LogId),
                    (Error, msg.GetError().GetMessage().c_str()));
            } else {
                STLOG(PRI_WARN, BLOB_DEPOT, BDTS12, "failed to delete object(s) from S3", (Id, LogId),
                    (Error, msg.GetError().GetMessage().c_str()));
            }

            auto *remainingTarget = requestThrottled ? &locatorsThrottled : &locatorsError;
            for (const auto& [key, locator] : Locators) {
                remainingTarget->push_back(locator);
                if (requestThrottled) {
                    BDEV(BDEV40, "deleted_from_S3:SlowDown", (BDT, TabletId), (Locator, locator));
                } else {
                    STLOG(PRI_WARN, BLOB_DEPOT, BDTS08, "failed to delete object from S3", (Id, LogId), (Locator, locator));
                    BDEV(BDEV31, "deleted_from_S3:error", (BDT, TabletId), (Locator, locator));
                }
            }

            Send(ParentId, new TEvDeleteResult(std::move(locatorsOk), std::move(locatorsThrottled),
                std::move(locatorsError)));
            PassAway();
        }

        void HandleUndelivered() {
            Finish("event undelivered");
        }

        void Finish(std::optional<TString> error, bool throttled = false) {
            if (error) {
                STLOG(PRI_WARN, BLOB_DEPOT, BDTS03, "failed to delete object(s) from S3", (Id, LogId), (Locators, Locators),
                    (Error, error), (Throttled, throttled));
            }
            std::vector<TS3Locator> locatorsOk;
            std::vector<TS3Locator> locatorsThrottled;
            std::vector<TS3Locator> locatorsError;
            std::vector<TS3Locator> *target;
            if (!error) {
                target = &locatorsOk;
            } else if (throttled) {
                target = &locatorsThrottled;
            } else {
                target = &locatorsError;
            }
            for (const auto& [key, locator] : Locators) {
                target->push_back(locator);
            }
            Send(ParentId, new TEvDeleteResult(std::move(locatorsOk), std::move(locatorsThrottled),
                std::move(locatorsError)));
            PassAway();
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvExternalStorage::TEvDeleteObjectResponse, Handle)
            hFunc(TEvExternalStorage::TEvDeleteObjectsResponse, Handle)
            cFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
            cFunc(TEvents::TSystem::Poison, PassAway)
        )
    };

    class TS3Manager::TTxDeleteTrashS3 : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::vector<TS3Locator> Locators;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_DELETE_TRASH_S3; }

        TTxDeleteTrashS3(TBlobDepot *self, std::vector<TS3Locator>&& locators)
            : TTransactionBase(self)
            , Locators(std::move(locators))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            for (const TS3Locator& locator : Locators) {
                NIceDb::TNiceDb(txc.DB).Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Delete();
            }
            return true;
        }

        void Complete(const TActorContext&) override {
            STLOG(PRI_INFO, BLOB_DEPOT, BDTS04, "TTxDeleteTrashS3 complete", (Id, Self->GetLogId()), (Locators, Locators));

            size_t len = 0;
            for (const TS3Locator& locator : Locators) {
                len += locator.Len;
            }

            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_S3_TRASH_OBJECTS] =
                Self->S3Manager->TotalS3TrashObjects -= Locators.size();
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_S3_TRASH_SIZE] =
                Self->S3Manager->TotalS3TrashSize -= len;

            --Self->S3Manager->NumDeleteTxInFlight;

            Self->S3Manager->RunDeletersIfNeeded();
        }
    };

    void TS3Manager::AddTrashToCollect(TS3Locator locator) {
        STLOG(PRI_INFO, BLOB_DEPOT, BDTS06, "AddTrashToCollect", (Id, Self->GetLogId()), (Locator, locator));
        BDEV(BDEV32, "add_S3_trash_to_collect", (BDT, Self->TabletID()), (Locator, locator));
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_S3_TRASH_OBJECTS] = ++TotalS3TrashObjects;
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_S3_TRASH_SIZE] = TotalS3TrashSize += locator.Len;
        DeleteQueue.push_back(locator);
        RunDeletersIfNeeded();
    }

    void TS3Manager::RunDeletersIfNeeded() {
        // Gate on SlowDown-induced throttling cooldown.
        const TMonotonic now = TActivationContext::Monotonic();
        if (now < DeleteThrottleUntil) {
            if (!DeleteWakeupScheduled) {
                TActivationContext::Schedule(DeleteThrottleUntil, new IEventHandle(TEvPrivate::EvDeleteThrottleWakeup,
                    0, Self->SelfId(), {}, nullptr, 0));
                DeleteWakeupScheduled = true;
            }
            return;
        }

        while (NumDeleteTxInFlight + ActiveDeleters.size() < CurrentMaxDeletesInFlight) {
            if (DeleteQueue.empty()) {
                break;
            }

            // create list of locators we are going to delete during this operation
            THashMap<TString, TS3Locator> locators;
            while (!DeleteQueue.empty() && locators.size() < MaxObjectsToDeleteAtOnce) {
                const TS3Locator& locator = DeleteQueue.front();
                locators.emplace(locator.MakeObjectName(BasePath), locator);
                DeleteQueue.pop_front();
            }
            if (!locators) {
                break;
            }

            const TActorId actorId = Self->Register(new TDeleterActor(Self->SelfId(), locators, Self->GetLogId(),
                Self->TabletID()));
            ActiveDeleters.insert(actorId);

            if (locators.size() == 1) {
                BDEV(BDEV33, "issue_S3_delete", (BDT, Self->TabletID()), (Locator, locators.begin()->second));
                TActivationContext::Send(new IEventHandle(WrapperId, actorId,
                    new TEvExternalStorage::TEvDeleteObjectRequest(
                        Aws::S3::Model::DeleteObjectRequest()
                            .WithBucket(Bucket)
                            .WithKey(locators.begin()->first)
                    ),
                    IEventHandle::FlagTrackDelivery
                ));
            } else {
                auto del = Aws::S3::Model::Delete();
                for (const auto& [key, locator] : locators) {
                    del.AddObjects(Aws::S3::Model::ObjectIdentifier().WithKey(key));
                    BDEV(BDEV34, "issue_S3_delete:multi", (BDT, Self->TabletID()), (Locator, locator));
                }

                TActivationContext::Send(new IEventHandle(WrapperId, actorId,
                    new TEvExternalStorage::TEvDeleteObjectsRequest(Aws::S3::Model::DeleteObjectsRequest()
                        .WithBucket(Bucket).WithDelete(std::move(del))), IEventHandle::FlagTrackDelivery));
            }
        }
    }

    void TS3Manager::HandleDeleter(TAutoPtr<IEventHandle> ev) {
        STRICT_STFUNC_BODY(
            hFunc(TEvDeleteResult, [&](TEvDeleteResult::TPtr ev) {
                const size_t numErased = ActiveDeleters.erase(ev->Sender);
                Y_ABORT_UNLESS(numErased == 1);

                auto& msg = *ev->Get();

                Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_DELETES_OK] += msg.LocatorsOk.size();
                size_t len = 0;
                for (const TS3Locator& locator : msg.LocatorsOk) {
                    len += locator.Len;
                }
                Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_DELETES_BYTES] += len;

                Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_DELETES_ERROR] += msg.LocatorsError.size();
                Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_S3_DELETES_SLOW_DOWN] +=
                    msg.LocatorsThrottled.size();

                if (!msg.LocatorsThrottled.empty()) {
                    // S3 asked us to slow down: requeue, shrink concurrency, and arm exponential backoff.
                    DeleteQueue.insert(DeleteQueue.end(), msg.LocatorsThrottled.begin(), msg.LocatorsThrottled.end());
                    CurrentMaxDeletesInFlight = 1;
                    ConsecutiveSuccessfulDeleteBatches = 0;
                    const TDuration delay = DeleteBackoff.Next();
                    DeleteThrottleUntil = TActivationContext::Monotonic() + delay;
                    STLOG(PRI_WARN, BLOB_DEPOT, BDTS21, "S3 delete throttled", (Id, Self->GetLogId()),
                        (Delay, delay), (Throttled, msg.LocatorsThrottled.size()),
                        (CurrentMaxDeletesInFlight, CurrentMaxDeletesInFlight));
                    BDEV(BDEV36, "S3_delete_throttled", (BDT, Self->TabletID()), (DelayMs, delay.MilliSeconds()),
                        (Throttled, msg.LocatorsThrottled.size()));
                } else if (!msg.LocatorsOk.empty()) {
                    // Pure success: gradually restore concurrency.
                    if (CurrentMaxDeletesInFlight < MaxDeletesInFlight) {
                        if (++ConsecutiveSuccessfulDeleteBatches >= SuccessesPerConcurrencyStepUp) {
                            ConsecutiveSuccessfulDeleteBatches = 0;
                            ++CurrentMaxDeletesInFlight;
                            if (CurrentMaxDeletesInFlight >= MaxDeletesInFlight) {
                                CurrentMaxDeletesInFlight = MaxDeletesInFlight;
                                DeleteBackoff.Reset();
                            }
                        }
                    }
                }

                if (!msg.LocatorsOk.empty()) {
                    Self->Execute(std::make_unique<TTxDeleteTrashS3>(Self, std::move(msg.LocatorsOk)));
                    ++NumDeleteTxInFlight;
                }

                if (!msg.LocatorsError.empty()) {
                    DeleteQueue.insert(DeleteQueue.end(), msg.LocatorsError.begin(), msg.LocatorsError.end());
                }

                RunDeletersIfNeeded();
            })
        )
    }

    void TS3Manager::HandleDeleteThrottleWakeup() {
        DeleteWakeupScheduled = false;
        RunDeletersIfNeeded();
    }

} // NKikimr::NBlobDepot
