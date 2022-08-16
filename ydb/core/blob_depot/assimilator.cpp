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
    }

    STATEFN(TAssimilator::StateFunc) {
        if (Token.expired()) {
            return PassAway();
        }

        switch (const ui32 type = ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvAssimilateResult, Handle);
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
            hFunc(TEvBlobStorage::TEvPutResult, Handle);
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvBlobStorage::TEvControllerGroupDecommittedResponse, Handle);
            cFunc(TEvPrivate::EvResume, Action);

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
        SendToBSProxy(SelfId(), Self->Config.GetDecommitGroupId(), new TEvBlobStorage::TEvAssimilate(SkipBlocksUpTo,
            SkipBarriersUpTo, SkipBlobsUpTo));
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvAssimilateResult::TPtr ev) {
        class TTxPutAssimilatedData : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            TAssimilator *Self;
            std::unique_ptr<TEvBlobStorage::TEvAssimilateResult> Ev;
            bool UnblockRegisterActorQ = false;

        public:
            TTxPutAssimilatedData(TAssimilator *self, TEvBlobStorage::TEvAssimilateResult::TPtr ev)
                : TTransactionBase(self->Self)
                , Self(self)
                , Ev(ev->Release().Release())
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);

                const bool done = Ev->Blocks.empty() && Ev->Barriers.empty() && Ev->Blobs.empty();
                const bool blocksFinished = Ev->Blocks.empty() || !Ev->Barriers.empty() || !Ev->Blobs.empty() || done;
                const bool barriersFinished = Ev->Barriers.empty() || !Ev->Blobs.empty() || done;

                if (const auto& blocks = Ev->Blocks; !blocks.empty()) {
                    Self->SkipBlocksUpTo = blocks.back().TabletId;
                }
                if (const auto& barriers = Ev->Barriers; !barriers.empty()) {
                    Self->SkipBarriersUpTo = {barriers.back().TabletId, barriers.back().Channel};
                }
                if (const auto& blobs = Ev->Blobs; !blobs.empty()) {
                    Self->SkipBlobsUpTo = blobs.back().Id;
                }

                for (const auto& block : Ev->Blocks) {
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT31, "assimilated block", (Id, Self->Self->GetLogId()), (Block, block));
                    Self->Self->BlocksManager->AddBlockOnDecommit(block, txc);
                }
                for (const auto& barrier : Ev->Barriers) {
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT32, "assimilated barrier", (Id, Self->Self->GetLogId()), (Barrier, barrier));
                    Self->Self->BarrierServer->AddBarrierOnDecommit(barrier, txc);
                }
                for (const auto& blob : Ev->Blobs) {
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT33, "assimilated blob", (Id, Self->Self->GetLogId()), (Blob, blob));
                    Self->Self->Data->AddDataOnDecommit(blob, txc);
                }

                auto& decommitState = Self->Self->DecommitState;
                if (blocksFinished && decommitState < EDecommitState::BlocksFinished) {
                    decommitState = EDecommitState::BlocksFinished;
                    UnblockRegisterActorQ = true;
                }
                if (barriersFinished && decommitState < EDecommitState::BarriersFinished) {
                    decommitState = EDecommitState::BarriersFinished;
                }
                if (done && decommitState < EDecommitState::BlobsFinished) {
                    decommitState = EDecommitState::BlobsFinished;
                }

                db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                    NIceDb::TUpdate<Schema::Config::DecommitState>(decommitState),
                    NIceDb::TUpdate<Schema::Config::AssimilatorState>(Self->SerializeAssimilatorState())
                );

                return true;
            }

            void Complete(const TActorContext&) override {
                if (UnblockRegisterActorQ) {
                    STLOG(PRI_INFO, BLOB_DEPOT, BDT35, "blocks assimilation complete", (Id, Self->Self->GetLogId()),
                        (DecommitGroupId, Self->Self->Config.GetDecommitGroupId()));
                    Self->Self->ProcessRegisterAgentQ();
                }

                Self->Action();
            }
        };

        Self->Execute(std::make_unique<TTxPutAssimilatedData>(this, ev));
    }

    void TAssimilator::ScanDataForCopying() {
        const bool fromTheBeginning = !LastScannedKey;

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

        auto callback = [&](const TData::TKey& key, const TData::TValue& value) {
            if (!value.OriginalBlobId) {
                LastScannedKey.emplace(key.GetBlobId());
                return true; // keep scanning
            } else if (const TLogoBlobID& id = *value.OriginalBlobId; scanQ.empty() ||
                    scanQ.front().OriginalBlobId.TabletID() == id.TabletID()) {
                LastScannedKey.emplace(key.GetBlobId());
                scanQ.push_back({.Key = *LastScannedKey, .OriginalBlobId = id});
                totalSize += id.BlobSize();
                NeedfulBlobs.insert(id);
                return totalSize < MaxSizeToQuery;
            } else {
                return false; // a blob belonging to different tablet
            }
        };

        // FIXME: reentrable as it shares mailbox with the BlobDepot tablet itself
        Self->Data->ScanRange(LastScannedKey ? &lastScannedKey : nullptr, nullptr, {}, callback);

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
            SendToBSProxy(SelfId(), Self->Config.GetDecommitGroupId(), ev.release());
        } else if (fromTheBeginning) {
            OnCopyDone();
        } else {
            // restart the scan from the beginning and find other keys to copy or finish it
            LastScannedKey.reset();
            TActivationContext::Send(new IEventHandle(TEvPrivate::EvResume, 0, SelfId(), {}, nullptr, 0));
        }
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        auto& msg = *ev->Get();
        for (ui32 i = 0; i < msg.ResponseSz; ++i) {
            auto& resp = msg.Responses[i];
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT34, "got TEvGetResult", (Id, Self->GetLogId()), (BlobId, resp.Id),
                (Status, resp.Status));
            if (resp.Status == NKikimrProto::OK) {
                auto ev = std::make_unique<TEvBlobStorage::TEvPut>(resp.Id, resp.Buffer, TInstant::Max());
                ev->Decommission = true;
                SendToBSProxy(SelfId(), Self->Config.GetDecommitGroupId(), ev.release());
                ++NumPutsInFlight;
            }
        }
        if (!NumPutsInFlight) {
            Action();
        }
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvPutResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT37, "got TEvPutResult", (Id, Self->GetLogId()), (Msg, msg));
        if (msg.Status == NKikimrProto::OK) {
            const size_t numErased = NeedfulBlobs.erase(msg.Id);
            Y_VERIFY(numErased == 1);
        }
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
        const TGroupID groupId(Self->Config.GetDecommitGroupId());
        const ui64 tabletId = MakeBSControllerID(groupId.AvailabilityDomainID());
        PipeId = Register(NTabletPipe::CreateClient(SelfId(), tabletId, NTabletPipe::TClientRetryPolicy::WithRetries()));
        NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobStorage::TEvControllerGroupDecommittedNotify(groupId.GetRaw()));
    }

    void TAssimilator::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDTxx, "received TEvClientConnected", (Id, Self->GetLogId()), (Status, msg.Status));
        if (msg.Status != NKikimrProto::OK) {
            CreatePipe();
        }
    }

    void TAssimilator::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr /*ev*/) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDTxx, "received TEvClientDestroyed", (Id, Self->GetLogId()));
        CreatePipe();
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvControllerGroupDecommittedResponse::TPtr ev) {
        auto& msg = *ev->Get();
        const NKikimrProto::EReplyStatus status = msg.Record.GetStatus();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDTxx, "received TEvControllerGroupDecommittedResponse", (Id, Self->GetLogId()),
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
        if (Config.HasDecommitGroupId()) {
           Y_VERIFY(!GroupAssimilatorId);
           GroupAssimilatorId = RegisterWithSameMailbox(new TGroupAssimilator(this));
        }
    }

} // NKikimr::NBlobDepot
