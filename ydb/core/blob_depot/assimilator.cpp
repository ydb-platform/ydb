#include "blob_depot_tablet.h"
#include "assimilator.h"
#include "blocks.h"
#include "garbage_collection.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    using TAssimilator = TBlobDepot::TGroupAssimilator;

    struct TStateBits {
        enum {
            Blocks = 1,
            Barriers = 2,
            Blobs = 4,
        };
    };

    void TAssimilator::Bootstrap() {
        if (Token.expired()) {
            return PassAway();
        }

        const std::optional<TString>& state = Self->AssimilatorState;
        if (state) {
            TStringInput stream(*state);
            ui8 stateBits;
            Load(&stream, stateBits);
            if (stateBits & TStateBits::Blocks) {
                Load(&stream, SkipBlocksUpTo.emplace());
            }
            if (stateBits & TStateBits::Barriers) {
                Load(&stream, SkipBarriersUpTo.emplace());
            }
            if (stateBits & TStateBits::Blobs) {
                Load(&stream, SkipBlobsUpTo.emplace());
            }
        }

        Become(&TThis::StateFunc);
        Action();
    }

    void TAssimilator::PassAway() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT52, "TAssimilator::PassAway", (Id, Self->GetLogId()));
        TActorBootstrapped::PassAway();
    }

    STATEFN(TAssimilator::StateFunc) {
        if (Token.expired()) {
            return PassAway();
        }

        switch (const ui32 type = ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvAssimilateResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
            hFunc(TEvBlobStorage::TEvPutResult, Handle);
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvBlobStorage::TEvControllerGroupDecommittedResponse, Handle);
            cFunc(TEvPrivate::EvResume, Action);
            cFunc(TEvPrivate::EvTxComplete, HandleTxComplete);
            cFunc(TEvents::TSystem::Poison, PassAway);

            default:
                Y_VERIFY_DEBUG(false, "unexpected event Type# %08" PRIx32, type);
                STLOG(PRI_CRIT, BLOB_DEPOT, BDT00, "unexpected event", (Id, Self->GetLogId()), (Type, type));
                break;
        }
    }

    void TAssimilator::Action() {
        if (Self->DecommitState < EDecommitState::BlobsFinished) {
            SendAssimilateRequest();
        } else if (Self->DecommitState < EDecommitState::BlobsCopied) {
            ScanDataForCopying();
        } else if (Self->DecommitState == EDecommitState::BlobsCopied) {
            CreatePipe();
        } else if (Self->DecommitState != EDecommitState::Done) {
            Y_UNREACHABLE();
        }
    }

    void TAssimilator::SendAssimilateRequest() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT53, "TAssimilator::SendAssimilateRequest", (Id, Self->GetLogId()),
            (SelfId, SelfId()));
        Y_VERIFY(Self->Config.GetIsDecommittingGroup());
        SendToBSProxy(SelfId(), Self->Config.GetVirtualGroupId(), new TEvBlobStorage::TEvAssimilate(SkipBlocksUpTo,
            SkipBarriersUpTo, SkipBlobsUpTo));
    }

    void TAssimilator::Handle(TEvents::TEvUndelivered::TPtr ev) {
        STLOG(PRI_ERROR, BLOB_DEPOT, BDT55, "received TEvUndelivered", (Id, Self->GetLogId()), (Sender, ev->Sender),
            (Cookie, ev->Cookie), (Type, ev->Get()->SourceType), (Reason, ev->Get()->Reason));
        TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvResume, 0, SelfId(), {},
            nullptr, 0));
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvAssimilateResult::TPtr ev) {
        class TTxPutAssimilatedData : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            TAssimilator* const Self;
            std::unique_ptr<TEvBlobStorage::TEvAssimilateResult> Ev;
            bool BlocksFinished = false;
            bool BarriersFinished = false;

            bool UnblockRegisterActorQ = false;
            bool MoreData = false;

        public:
            TTxPutAssimilatedData(TAssimilator *self, TEvBlobStorage::TEvAssimilateResult::TPtr ev)
                : TTransactionBase(self->Self)
                , Self(self)
                , Ev(ev->Release().Release())
            {}

            TTxPutAssimilatedData(TTxPutAssimilatedData& predecessor)
                : TTransactionBase(predecessor.Self->Self)
                , Self(predecessor.Self)
                , Ev(std::move(predecessor.Ev))
                , BlocksFinished(predecessor.BlocksFinished)
                , BarriersFinished(predecessor.BarriersFinished)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);

                const bool wasEmpty = Ev->Blocks.empty() && Ev->Barriers.empty() && Ev->Blobs.empty();
                if (wasEmpty) {
                    BlocksFinished = BarriersFinished = true;
                }

                ui32 maxItems = 10'000;
                for (auto& blocks = Ev->Blocks; maxItems && !blocks.empty(); blocks.pop_front(), --maxItems) {
                    auto& block = blocks.front();
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT31, "assimilated block", (Id, Self->Self->GetLogId()), (Block, block));
                    Self->Self->BlocksManager->AddBlockOnDecommit(block, txc);
                    Self->SkipBlocksUpTo.emplace(block.TabletId);
                }
                for (auto& barriers = Ev->Barriers; maxItems && !barriers.empty(); barriers.pop_front(), --maxItems) {
                    auto& barrier = barriers.front();
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT32, "assimilated barrier", (Id, Self->Self->GetLogId()), (Barrier, barrier));
                    if (!Self->Self->BarrierServer->AddBarrierOnDecommit(barrier, maxItems, txc, this)) {
                        Y_VERIFY(!maxItems);
                        break;
                    }
                    Self->SkipBarriersUpTo.emplace(barrier.TabletId, barrier.Channel);
                    BlocksFinished = true; // there will be no blocks for sure
                }
                for (auto& blobs = Ev->Blobs; maxItems && !blobs.empty(); blobs.pop_front(), --maxItems) {
                    auto& blob = blobs.front();
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT33, "assimilated blob", (Id, Self->Self->GetLogId()), (Blob, blob));
                    Self->Self->Data->AddDataOnDecommit(blob, txc, this);
                    Self->SkipBlobsUpTo.emplace(blob.Id);
                    Self->Self->Data->LastAssimilatedBlobId = blob.Id;
                    BlocksFinished = BarriersFinished = true; // no blocks and no more barriers
                }

                if (Ev->Blocks.empty() && Ev->Barriers.empty() && Ev->Blobs.empty()) {
                    auto& decommitState = Self->Self->DecommitState;
                    const auto decommitStateOnEntry = decommitState;
                    if (BlocksFinished && decommitState < EDecommitState::BlocksFinished) {
                        decommitState = EDecommitState::BlocksFinished;
                        UnblockRegisterActorQ = true;
                    }
                    if (BarriersFinished && decommitState < EDecommitState::BarriersFinished) {
                        decommitState = EDecommitState::BarriersFinished;
                    }
                    if (wasEmpty && decommitState < EDecommitState::BlobsFinished) {
                        decommitState = EDecommitState::BlobsFinished;
                    }

                    db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                        NIceDb::TUpdate<Schema::Config::DecommitState>(decommitState),
                        NIceDb::TUpdate<Schema::Config::AssimilatorState>(Self->SerializeAssimilatorState())
                    );

                    auto toString = [](EDecommitState state) {
                        switch (state) {
                            case EDecommitState::Default: return "Default";
                            case EDecommitState::BlocksFinished: return "BlocksFinished";
                            case EDecommitState::BarriersFinished: return "BarriersFinished";
                            case EDecommitState::BlobsFinished: return "BlobsFinished";
                            case EDecommitState::BlobsCopied: return "BlobsCopied";
                            case EDecommitState::Done: return "Done";
                        }
                    };

                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT47, "decommit state change", (Id, Self->Self->GetLogId()),
                        (From, toString(decommitStateOnEntry)), (To, toString(decommitState)),
                        (UnblockRegisterActorQ, UnblockRegisterActorQ));
                } else {
                    MoreData = true;
                }

                return true;
            }

            void Complete(const TActorContext&) override {
                Self->Self->Data->CommitTrash(this);

                if (MoreData) {
                    Self->Self->Execute(std::make_unique<TTxPutAssimilatedData>(*this));
                } else {
                    if (UnblockRegisterActorQ) {
                        STLOG(PRI_INFO, BLOB_DEPOT, BDT35, "blocks assimilation complete", (Id, Self->Self->GetLogId()));
                        Self->Self->ProcessRegisterAgentQ();
                    }

                    Self->Action();
                }
            }
        };

        Self->Execute(std::make_unique<TTxPutAssimilatedData>(this, ev));
    }

    void TAssimilator::ScanDataForCopying() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT54, "TAssimilator::ScanDataForCopying", (Id, Self->GetLogId()),
            (LastScannedKey, LastScannedKey));

        TData::TKey lastScannedKey;
        if (LastScannedKey) {
            lastScannedKey = TData::TKey(*LastScannedKey);
        }

        struct TScanQueueItem {
            TLogoBlobID Key;
            TLogoBlobID OriginalBlobId;
        };
        std::deque<TScanQueueItem> scanQ;
        ui32 totalSize = 0;
        THPTimer timer;
        ui32 numItems = 0;

        auto callback = [&](const TData::TKey& key, const TData::TValue& value) {
            if (++numItems == 1000) {
                numItems = 0;
                if (TDuration::Seconds(timer.Passed()) >= TDuration::MilliSeconds(1)) {
                    return false;
                }
            }
            if (!value.OriginalBlobId) {
                LastScannedKey.emplace(key.GetBlobId());
                return true; // keep scanning
            } else if (const TLogoBlobID& id = *value.OriginalBlobId; scanQ.empty() ||
                    scanQ.front().OriginalBlobId.TabletID() == id.TabletID()) {
                LastScannedKey.emplace(key.GetBlobId());
                scanQ.push_back({.Key = *LastScannedKey, .OriginalBlobId = id});
                totalSize += id.BlobSize();
                EntriesToProcess = true;
                return totalSize < MaxSizeToQuery;
            } else {
                return false; // a blob belonging to different tablet
            }
        };

        // FIXME: reentrable as it shares mailbox with the BlobDepot tablet itself
        const bool finished = Self->Data->ScanRange(LastScannedKey ? &lastScannedKey : nullptr, nullptr, {}, callback);

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT56, "ScanDataForCopying done", (Id, Self->GetLogId()),
            (LastScannedKey, LastScannedKey), (ScanQ.size, scanQ.size()), (EntriesToProcess, EntriesToProcess),
            (Finished, finished));

        if (!scanQ.empty()) {
            using TQuery = TEvBlobStorage::TEvGet::TQuery;
            const ui32 sz = scanQ.size();
            TArrayHolder<TQuery> queries(new TQuery[sz]);
            TQuery *query = queries.Get();
            for (const TScanQueueItem& item : scanQ) {
                query->Set(item.OriginalBlobId);
                ++query;
            }
            auto ev = std::make_unique<TEvBlobStorage::TEvGet>(queries, sz, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead);
            ev->Decommission = true;
            SendToBSProxy(SelfId(), Self->Config.GetVirtualGroupId(), ev.release());
        } else if (!finished) { // timeout hit, reschedule work
            TActivationContext::Send(new IEventHandle(TEvPrivate::EvResume, 0, SelfId(), {}, nullptr, 0));
        } else if (!EntriesToProcess) { // we have finished scanning the whole table without any entries, copying is done
            OnCopyDone();
        } else { // we have finished scanning, but we have replicated some data, restart scanning to ensure that nothing left
            LastScannedKey.reset();
            EntriesToProcess = false;
            TActivationContext::Send(new IEventHandle(TEvPrivate::EvResume, 0, SelfId(), {}, nullptr, 0));
        }
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        class TTxDropBlobIfNoData : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            const TLogoBlobID Id;
            const TActorId AssimilatorId;

        public:
            TTxDropBlobIfNoData(TBlobDepot *self, TLogoBlobID id, TActorId assimilatorId)
                : TTransactionBase(self)
                , Id(id)
                , AssimilatorId(assimilatorId)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                const TData::TKey key(Id);
                if (const TData::TValue *v = Self->Data->FindKey(key); v && v->OriginalBlobId &&
                        v->KeepState != NKikimrBlobDepot::EKeepState::Keep) {
                    Self->Data->DeleteKey(key, txc, this);
                }
                return true;
            }

            void Complete(const TActorContext&) override {
                Self->Data->CommitTrash(this);
                TActivationContext::Send(new IEventHandle(TEvPrivate::EvTxComplete, 0, AssimilatorId, {}, nullptr, 0));
            }
        };

        auto& msg = *ev->Get();
        for (ui32 i = 0; i < msg.ResponseSz; ++i) {
            auto& resp = msg.Responses[i];
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT34, "got TEvGetResult", (Id, Self->GetLogId()), (BlobId, resp.Id),
                (Status, resp.Status), (NumPutsInFlight, NumPutsInFlight));
            if (resp.Status == NKikimrProto::OK) {
                auto ev = std::make_unique<TEvBlobStorage::TEvPut>(resp.Id, resp.Buffer, TInstant::Max());
                ev->Decommission = true;
                SendToBSProxy(SelfId(), Self->Config.GetVirtualGroupId(), ev.release());
                ++NumPutsInFlight;
            } else if (resp.Status == NKikimrProto::NODATA) {
                Self->Execute(std::make_unique<TTxDropBlobIfNoData>(Self, resp.Id, SelfId()));
                ++NumPutsInFlight;
            }
        }
        if (!NumPutsInFlight) {
            Action();
        }
    }

    void TAssimilator::HandleTxComplete() {
        if (!--NumPutsInFlight) {
            Action();
        }
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvPutResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT37, "got TEvPutResult", (Id, Self->GetLogId()), (Msg, msg),
            (NumPutsInFlight, NumPutsInFlight));
        if (!--NumPutsInFlight) {
            IssueCollects();
        }
    }

    void TAssimilator::IssueCollects() {
        // FIXME: do it
        Action();
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
        (void)ev;
    }

    void TAssimilator::OnCopyDone() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT38, "data copying is done", (Id, Self->GetLogId()));

        class TTxFinishCopying : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            TAssimilator* const Self;

        public:
            TTxFinishCopying(TAssimilator *self)
                : TTransactionBase(self->Self)
                , Self(self)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);
                Self->Self->DecommitState = EDecommitState::BlobsCopied;
                db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                    NIceDb::TUpdate<Schema::Config::DecommitState>(Self->Self->DecommitState)
                );
                return true;
            }

            void Complete(const TActorContext&) override {
                Self->Action();
            }
        };

        Self->Execute(std::make_unique<TTxFinishCopying>(this));
    }

    void TAssimilator::CreatePipe() {
        const TGroupID groupId(Self->Config.GetVirtualGroupId());
        const ui64 tabletId = MakeBSControllerID(groupId.AvailabilityDomainID());
        PipeId = Register(NTabletPipe::CreateClient(SelfId(), tabletId, NTabletPipe::TClientRetryPolicy::WithRetries()));
        NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobStorage::TEvControllerGroupDecommittedNotify(groupId.GetRaw()));
    }

    void TAssimilator::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT39, "received TEvClientConnected", (Id, Self->GetLogId()), (Status, msg.Status));
        if (msg.Status != NKikimrProto::OK) {
            CreatePipe();
        }
    }

    void TAssimilator::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr /*ev*/) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT40, "received TEvClientDestroyed", (Id, Self->GetLogId()));
        CreatePipe();
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvControllerGroupDecommittedResponse::TPtr ev) {
        auto& msg = *ev->Get();
        const NKikimrProto::EReplyStatus status = msg.Record.GetStatus();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT41, "received TEvControllerGroupDecommittedResponse", (Id, Self->GetLogId()),
            (Status, status));
        if (status == NKikimrProto::OK) {
            class TTxFinishDecommission : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            public:
                TTxFinishDecommission(TAssimilator *self)
                    : TTransactionBase(self->Self)
                {}

                bool Execute(TTransactionContext& txc, const TActorContext&) override {
                    NIceDb::TNiceDb db(txc.DB);
                    Self->DecommitState = EDecommitState::Done;
                    db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                        NIceDb::TUpdate<Schema::Config::DecommitState>(Self->DecommitState)
                    );
                    return true;
                }

                void Complete(const TActorContext&) override {}
            };

            Self->GroupAssimilatorId = {};
            Self->Execute(std::make_unique<TTxFinishDecommission>(this));
            PassAway();
        } else {
            NTabletPipe::CloseAndForgetClient(SelfId(), PipeId);
            Action();
        }
    }

    TString TAssimilator::SerializeAssimilatorState() const {
        TStringStream stream;

        const ui8 stateBits = (SkipBlocksUpTo ? TStateBits::Blocks : 0)
            | (SkipBarriersUpTo ? TStateBits::Barriers : 0)
            | (SkipBlocksUpTo ? TStateBits::Blocks : 0);

        Save(&stream, stateBits);

        if (SkipBlocksUpTo) {
            Save(&stream, *SkipBlocksUpTo);
        }
        if (SkipBarriersUpTo) {
            Save(&stream, *SkipBarriersUpTo);
        }
        if (SkipBlobsUpTo) {
            Save(&stream, *SkipBlobsUpTo);
        }

        return stream.Str();
    }

    void TBlobDepot::StartGroupAssimilator() {
        if (Config.GetIsDecommittingGroup()) {
           Y_VERIFY(!GroupAssimilatorId);
           GroupAssimilatorId = RegisterWithSameMailbox(new TGroupAssimilator(this));
        }
    }

} // NKikimr::NBlobDepot
