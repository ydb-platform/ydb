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

    class TBlobDepot::TData::TTxCommitAssimilatedBlob : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const NKikimrProto::EReplyStatus Status;
        const TBlobSeqId BlobSeqId;
        const TData::TKey Key;
        const ui32 NotifyEventType;
        const TActorId ParentId;
        const ui64 Cookie;
        const bool Keep;
        const bool DoNotKeep;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_COMMIT_ASSIMILATED_BLOB; }

        TTxCommitAssimilatedBlob(TBlobDepot *self, NKikimrProto::EReplyStatus status, TBlobSeqId blobSeqId,
                TData::TKey key, ui32 notifyEventType, TActorId parentId, ui64 cookie, bool keep, bool doNotKeep)
            : TTransactionBase(self)
            , Status(status)
            , BlobSeqId(blobSeqId)
            , Key(std::move(key))
            , NotifyEventType(notifyEventType)
            , ParentId(parentId)
            , Cookie(cookie)
            , Keep(keep)
            , DoNotKeep(doNotKeep)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            if (Status == NKikimrProto::OK) {
                Y_ABORT_UNLESS(!Self->Data->CanBeCollected(BlobSeqId));
                Self->Data->BindToBlob(Key, BlobSeqId, Keep, DoNotKeep, txc, this);
            } else if (Status == NKikimrProto::NODATA) {
                if (const TData::TValue *value = Self->Data->FindKey(Key); value && value->GoingToAssimilate) {
                    Self->Data->DeleteKey(Key, txc, this);
                }
            }
            return true;
        }

        void Complete(const TActorContext&) override {
            if (BlobSeqId) {
                TChannelInfo& channel = Self->Channels[BlobSeqId.Channel];
                const ui32 generation = Self->Executor()->Generation();
                const TBlobSeqId leastExpectedBlobIdBefore = channel.GetLeastExpectedBlobId(generation);
                const size_t numErased = channel.AssimilatedBlobsInFlight.erase(BlobSeqId.ToSequentialNumber());
                Y_ABORT_UNLESS(numErased == 1);
                if (leastExpectedBlobIdBefore != channel.GetLeastExpectedBlobId(generation)) {
                    Self->Data->OnLeastExpectedBlobIdChange(channel.Index); // allow garbage collection
                }
            }
            Self->Data->CommitTrash(this);
            TActivationContext::Send(new IEventHandle(NotifyEventType, 0, ParentId, {}, nullptr, Cookie));
        }
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
        if (!Token.expired()) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT52, "TAssimilator::PassAway", (Id, Self->GetLogId()));
        }
        TActorBootstrapped::PassAway();
    }

    STATEFN(TAssimilator::StateFunc) {
        if (Token.expired()) {
            return PassAway();
        }

        switch (const ui32 type = ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvAssimilateResult, Handle);
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
            hFunc(TEvBlobStorage::TEvPutResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvBlobStorage::TEvControllerGroupDecommittedResponse, Handle);
            cFunc(TEvPrivate::EvResume, Action);
            cFunc(TEvPrivate::EvResumeScanDataForCopying, HandleResumeScanDataForCopying);
            fFunc(TEvPrivate::EvTxComplete, HandleTxComplete);
            cFunc(TEvents::TSystem::Poison, PassAway);

            default:
                Y_DEBUG_ABORT_UNLESS(false, "unexpected event Type# %08" PRIx32, type);
                STLOG(PRI_CRIT, BLOB_DEPOT, BDT00, "unexpected event", (Id, Self->GetLogId()), (Type, type));
                break;
        }
    }

    void TAssimilator::Action() {
        Y_ABORT_UNLESS(!ActionInProgress);
        ActionInProgress = true;

        if (Self->DecommitState < EDecommitState::BlobsFinished) {
            SendAssimilateRequest();
        } else if (Self->DecommitState < EDecommitState::BlobsCopied) {
            ScanDataForCopying();
        } else if (Self->DecommitState == EDecommitState::BlobsCopied) {
            Y_ABORT_UNLESS(!PipeId);
            CreatePipe();
        } else if (Self->DecommitState != EDecommitState::Done) {
            Y_UNREACHABLE();
        } else {
            Y_UNREACHABLE();
        }
    }

    void TAssimilator::SendAssimilateRequest() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT53, "TAssimilator::SendAssimilateRequest", (Id, Self->GetLogId()),
            (SelfId, SelfId()));
        Y_ABORT_UNLESS(Self->Config.GetIsDecommittingGroup());
        SendToBSProxy(SelfId(), Self->Config.GetVirtualGroupId(), new TEvBlobStorage::TEvAssimilate(SkipBlocksUpTo,
            SkipBarriersUpTo, SkipBlobsUpTo));
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
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_PUT_ASSIMILATED_DATA; }

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
                        Y_ABORT_UNLESS(!maxItems);
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

                    Self->ActionInProgress = false;
                    Self->Action();
                }
            }
        };

        if (ev->Get()->Status == NKikimrProto::OK) {
            Self->Execute(std::make_unique<TTxPutAssimilatedData>(this, ev));
        } else {
            STLOG(PRI_INFO, BLOB_DEPOT, BDT75, "TEvAssimilate failed", (Id, Self->GetLogId()),
                (Status, ev->Get()->Status), (ErrorReason, ev->Get()->ErrorReason));

            ActionInProgress = false;
            Action();
        }
    }

    void TAssimilator::ScanDataForCopying() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT54, "TAssimilator::ScanDataForCopying", (Id, Self->GetLogId()),
            (LastScannedKey, LastScannedKey), (NumGetsUnprocessed, GetIdToUnprocessedPuts.size()));

        THPTimer timer;

        while (GetIdToUnprocessedPuts.size() < MaxGetsUnprocessed) {
            ui32 numItems = 0;
            bool timeout = false;

            auto callback = [&](const TData::TKey& key, const TData::TValue& value) {
                if (++numItems == 1000) {
                    numItems = 0;
                    if (TDuration::Seconds(timer.Passed()) >= TDuration::MilliSeconds(1)) {
                        timeout = true;
                        return false;
                    }
                }
                const TLogoBlobID& id = key.GetBlobId();
                if (!value.GoingToAssimilate) {
                    LastScannedKey.emplace(id);
                    return true; // keep scanning
                } else if (ScanQ.empty() || ScanQ.front().TabletID() == id.TabletID()) {
                    LastScannedKey.emplace(id);
                    ScanQ.push_back(id);
                    TotalSize += id.BlobSize();
                    EntriesToProcess = true;
                    return TotalSize < MaxSizeToQuery;
                } else {
                    return false; // a blob belonging to different tablet
                }
            };

            TData::TScanRange r{LastScannedKey ? TData::TKey(*LastScannedKey) : TData::TKey::Min(), TData::TKey::Max()};
            Self->Data->ScanRange(r, nullptr, nullptr, callback);

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT56, "ScanDataForCopying step", (Id, Self->GetLogId()),
                (LastScannedKey, LastScannedKey), (ScanQ.size, ScanQ.size()), (TotalSize, TotalSize),
                (EntriesToProcess, EntriesToProcess), (Timeout, timeout), (NumGetsUnprocessed, GetIdToUnprocessedPuts.size()));

            if (timeout) { // timeout hit, reschedule work
                if (!ResumeScanDataForCopyingInFlight) {
                    TActivationContext::Send(new IEventHandle(TEvPrivate::EvResumeScanDataForCopying, 0, SelfId(), {}, nullptr, 0));
                    ResumeScanDataForCopyingInFlight = true;
                }
                break;
            } else if (!ScanQ.empty()) {
                using TQuery = TEvBlobStorage::TEvGet::TQuery;
                const ui32 sz = ScanQ.size();
                TArrayHolder<TQuery> queries(new TQuery[sz]);
                TQuery *query = queries.Get();
                for (const TLogoBlobID& id : ScanQ) {
                    query->Set(id);
                    ++query;
                }
                auto ev = std::make_unique<TEvBlobStorage::TEvGet>(queries, sz, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead);
                ev->Decommission = true;
                const ui64 getId = NextGetId++;
                SendToBSProxy(SelfId(), Self->Config.GetVirtualGroupId(), ev.release(), getId);
                GetIdToUnprocessedPuts.try_emplace(getId);
                ScanQ.clear();
                TotalSize = 0;
            } else if (!GetIdToUnprocessedPuts.empty()) { // there are some unprocessed get queries, still have to wait
                break;
            } else if (!EntriesToProcess) { // we have finished scanning the whole table without any entries, copying is done
                OnCopyDone();
                break;
            } else { // we have finished scanning, but we have replicated some data, restart scanning to ensure that nothing left
                LastScannedKey.reset();
                EntriesToProcess = false;
            }
        }
    }

    void TAssimilator::HandleResumeScanDataForCopying() {
        Y_ABORT_UNLESS(ResumeScanDataForCopyingInFlight);
        ResumeScanDataForCopyingInFlight = false;
        ScanDataForCopying();
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        auto& msg = *ev->Get();
        const auto it = GetIdToUnprocessedPuts.find(ev->Cookie);
        Y_ABORT_UNLESS(it != GetIdToUnprocessedPuts.end());
        ui32 getBytes = 0;
        for (ui32 i = 0; i < msg.ResponseSz; ++i) {
            auto& resp = msg.Responses[i];
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT34, "got TEvGetResult", (Id, Self->GetLogId()), (BlobId, resp.Id),
                (Status, resp.Status), (NumGetsUnprocessed, GetIdToUnprocessedPuts.size()));
            if (resp.Status == NKikimrProto::OK) {
                std::vector<ui8> channels(1);
                if (Self->PickChannels(NKikimrBlobDepot::TChannelKind::Data, channels)) {
                    TChannelInfo& channel = Self->Channels[channels.front()];
                    const ui64 value = channel.NextBlobSeqId++;
                    const auto blobSeqId = TBlobSeqId::FromSequentalNumber(channel.Index, Self->Executor()->Generation(), value);
                    const TLogoBlobID id = blobSeqId.MakeBlobId(Self->TabletID(), EBlobType::VG_DATA_BLOB, 0, resp.Id.BlobSize());
                    const ui64 putId = NextPutId++;
                    SendToBSProxy(SelfId(), channel.GroupId, new TEvBlobStorage::TEvPut(id, TRcBuf(resp.Buffer), TInstant::Max()), putId);
                    const bool inserted = channel.AssimilatedBlobsInFlight.insert(value).second; // prevent from barrier advancing
                    Y_ABORT_UNLESS(inserted);
                    const bool inserted1 = PutIdToKey.try_emplace(putId, TData::TKey(resp.Id), it->first).second;
                    Y_ABORT_UNLESS(inserted1);
                    ++it->second;
                }
                getBytes += resp.Id.BlobSize();
            } else if (resp.Status == NKikimrProto::NODATA) {
                Self->Data->ExecuteTxCommitAssimilatedBlob(NKikimrProto::NODATA, TBlobSeqId(), TData::TKey(resp.Id),
                    TEvPrivate::EvTxComplete, SelfId(), it->first);
                ++it->second;
            }
        }
        if (getBytes) {
            Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_DECOMMIT_GET_BYTES] += getBytes;
        }
        if (!it->second) {
            GetIdToUnprocessedPuts.erase(it);
            if (!ResumeScanDataForCopyingInFlight) {
                ScanDataForCopying();
            }
        }
    }

    void TAssimilator::HandleTxComplete(TAutoPtr<IEventHandle> ev) {
        const auto it = GetIdToUnprocessedPuts.find(ev->Cookie);
        Y_ABORT_UNLESS(it != GetIdToUnprocessedPuts.end());
        if (!--it->second) {
            GetIdToUnprocessedPuts.erase(it);
            if (!ResumeScanDataForCopyingInFlight) {
                ScanDataForCopying();
            }
        }
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvPutResult::TPtr ev) {
        auto& msg = *ev->Get();
        if (msg.Status == NKikimrProto::OK) {
            Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_DECOMMIT_PUT_OK_BYTES] += msg.Id.BlobSize();
        }
        const auto it = PutIdToKey.find(ev->Cookie);
        Y_ABORT_UNLESS(it != PutIdToKey.end());
        const auto& [key, getId] = it->second;
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT37, "got TEvPutResult", (Id, Self->GetLogId()), (Msg, msg),
            (NumGetsUnprocessed, GetIdToUnprocessedPuts.size()), (Key, key));
        Self->Data->ExecuteTxCommitAssimilatedBlob(msg.Status, TBlobSeqId::FromLogoBlobId(msg.Id), std::move(key),
            TEvPrivate::EvTxComplete, SelfId(), getId);
        PutIdToKey.erase(it);
    }

    void TAssimilator::OnCopyDone() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT38, "data copying is done", (Id, Self->GetLogId()));
        Y_ABORT_UNLESS(GetIdToUnprocessedPuts.empty());

        class TTxFinishCopying : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            TAssimilator* const Self;

        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_FINISH_COPYING; }

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
                Self->ActionInProgress = false;
                Self->Action();
            }
        };

        Y_ABORT_UNLESS(ActionInProgress);
        Self->Execute(std::make_unique<TTxFinishCopying>(this));
    }

    void TAssimilator::CreatePipe() {
        const TGroupID groupId(Self->Config.GetVirtualGroupId());
        const ui64 tabletId = MakeBSControllerID();
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
        if (status == NKikimrProto::OK || status == NKikimrProto::ALREADY) {
            class TTxFinishDecommission : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            public:
                TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_FINISH_DECOMMISSION; }

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
            TActivationContext::Schedule(TDuration::Seconds(5), new IEventHandle(TEvPrivate::EvResume, 0, SelfId(), {}, nullptr, 0));
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

    void TBlobDepot::TData::ExecuteTxCommitAssimilatedBlob(NKikimrProto::EReplyStatus status, TBlobSeqId blobSeqId,
            TData::TKey key, ui32 notifyEventType, TActorId parentId, ui64 cookie, bool keep, bool doNotKeep) {
        Self->Execute(std::make_unique<TTxCommitAssimilatedBlob>(Self, status, blobSeqId, std::move(key),
            notifyEventType, parentId, cookie, keep, doNotKeep));
    }

    void TBlobDepot::StartGroupAssimilator() {
        if (Config.GetIsDecommittingGroup() && DecommitState != EDecommitState::Done) {
           Y_ABORT_UNLESS(!GroupAssimilatorId);
           Y_ABORT_UNLESS(Data->IsLoaded());
           GroupAssimilatorId = RegisterWithSameMailbox(new TGroupAssimilator(this));
        }
    }

} // NKikimr::NBlobDepot
