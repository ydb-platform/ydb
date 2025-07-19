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
        std::vector<TAssimilatedBlobInfo> Blobs;
        const ui32 NotifyEventType;
        const TActorId ParentId;
        const ui64 Cookie;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_COMMIT_ASSIMILATED_BLOB; }

        TTxCommitAssimilatedBlob(TBlobDepot *self, std::vector<TAssimilatedBlobInfo>&& blobs, ui32 notifyEventType,
                TActorId parentId, ui64 cookie)
            : TTransactionBase(self)
            , Blobs(std::move(blobs))
            , NotifyEventType(notifyEventType)
            , ParentId(parentId)
            , Cookie(cookie)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            for (auto& blob : Blobs) {
                std::visit(TOverloaded{
                    [&](TAssimilatedBlobInfo::TDrop&) {
                        if (const TData::TValue *value = Self->Data->FindKey(blob.Key); value && value->GoingToAssimilate) {
                            Self->Data->DeleteKey(blob.Key, txc, this);
                        }
                    },
                    [&](TAssimilatedBlobInfo::TUpdate& update) {
                        Y_ABORT_UNLESS(!Self->Data->CanBeCollected(update.BlobSeqId));
                        Self->Data->BindToBlob(blob.Key, update.BlobSeqId, update.Keep, update.DoNotKeep, txc, this);
                    },
                }, blob.Action);
            }
            return true;
        }

        void Complete(const TActorContext&) override {
            for (const auto& blob : Blobs) {
                if (auto *update = std::get_if<TAssimilatedBlobInfo::TUpdate>(&blob.Action)) {
                    const auto& blobSeqId = update->BlobSeqId;
                    TChannelInfo& channel = Self->Channels[blobSeqId.Channel];
                    const ui32 generation = Self->Executor()->Generation();
                    const TBlobSeqId leastExpectedBlobIdBefore = channel.GetLeastExpectedBlobId(generation);
                    const size_t numErased = channel.AssimilatedBlobsInFlight.erase(blobSeqId.ToSequentialNumber());
                    Y_ABORT_UNLESS(numErased == 1);
                    if (leastExpectedBlobIdBefore != channel.GetLeastExpectedBlobId(generation)) {
                        Self->Data->OnLeastExpectedBlobIdChange(channel.Index); // allow garbage collection
                    }
                }
            }
            Self->Data->CommitTrash(this);
            TActivationContext::Send(new IEventHandle(NotifyEventType, 0, ParentId, {}, nullptr, Cookie));
        }
    };

    class TBlobDepot::TData::TTxHardCollectAssimilatedBlobs : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui64 TabletId;
        const ui8 Channel;
        const TGenStep Barrier;
        const TActorId ParentId;

        ui32 PerGenerationCounter = 0;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_HARD_COLLECT_ASSIMILATED_BLOBS; }

        TTxHardCollectAssimilatedBlobs(TBlobDepot *self, ui64 tabletId, ui8 channel, TGenStep barrier, TActorId parentId)
            : TTransactionBase(self)
            , TabletId(tabletId)
            , Channel(channel)
            , Barrier(barrier)
            , ParentId(parentId)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            NIceDb::TNiceDb db(txc.DB);
            using T = Schema::Config;
            PerGenerationCounter = Self->PerGenerationCounter++;
            db.Table<Schema::Config>().Key(T::Key::Value).Update<T::PerGenerationCounter>(Self->PerGenerationCounter);
            return true;
        }

        void Complete(const TActorContext&) override {
            const bool del = Barrier.Generation() == Max<ui32>() && Barrier.Step() == Max<ui32>();
            auto ev = std::make_unique<TEvBlobStorage::TEvCollectGarbage>(
                TabletId, Max<ui32>(), del ? Max<ui32>() : PerGenerationCounter, Channel, true, Barrier.Generation(),
                Barrier.Step(), nullptr, nullptr, TInstant::Max(), false, /*hard=*/ true
            );
            ev->Decommission = true;
            TActivationContext::Send(ParentId, std::move(ev), 0, PerGenerationCounter);
        }
    };

    void TAssimilator::Bootstrap() {
        if (Token.expired()) {
            return PassAway();
        }

        ExpectedPerGenerationCounter = Self->PerGenerationCounter;

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
            UpdateAssimilatorPosition();
        }

        Become(&TThis::StateFunc);
        Action();
        UpdateBytesCopiedQ();
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
            hFunc(TEvBlobStorage::TEvCollectGarbage, Handle);
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvBlobStorage::TEvControllerGroupDecommittedResponse, Handle);
            cFunc(TEvPrivate::EvResume, Action);
            cFunc(TEvPrivate::EvResumeScanDataForPlanning, HandleResumeScanDataForPlanning);
            cFunc(TEvPrivate::EvResumeScanDataForCopying, HandleResumeScanDataForCopying);
            fFunc(TEvPrivate::EvTxComplete, HandleTxComplete);
            cFunc(TEvPrivate::EvUpdateBytesCopiedQ, UpdateBytesCopiedQ);
            cFunc(TEvents::TSystem::Poison, PassAway);

            default:
                Y_DEBUG_ABORT("unexpected event Type# %08" PRIx32, type);
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
            if (PlanningComplete) {
                ScanDataForCopying();
            } else {
                ScanDataForPlanning();
            }
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
            SkipBarriersUpTo, SkipBlobsUpTo, false, false));
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
                    Self->Self->JsonHandler.Invalidate();
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
                    Self->Self->JsonHandler.Invalidate();

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
                Self->Self->OnUpdateDecommitState();
                Self->Self->Data->CommitTrash(this);
                Self->UpdateAssimilatorPosition();

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

    void TAssimilator::ScanDataForPlanning() {
        if (ResumeScanDataForPlanningInFlight) {
            return;
        }

        const ui64 endTime = GetCycleCountFast() + DurationToCycles(TDuration::MilliSeconds(10));
        ui32 numItems = 0;
        bool timeout = false;
        bool invalidate = false;

        if (!LastPlanScannedKey) {
            ++Self->AsStats.CopyIteration;
            Self->AsStats.BytesToCopy = 0;
            Self->JsonHandler.Invalidate();
        }

        TData::TScanRange range{
            LastPlanScannedKey ? TData::TKey(*LastPlanScannedKey) : TData::TKey::Min(),
            TData::TKey::Max(),
        };

        Self->Data->ScanRange(range, nullptr, nullptr, [&](const TData::TKey& key, const TData::TValue& value) {
            const TLogoBlobID& id = key.GetBlobId();
            const auto blobQueueKey = std::make_tuple(id.TabletID(), id.Channel());
            auto& queue = LeastBlobQueue[blobQueueKey];
            if (value.GoingToAssimilate) {
                queue.emplace_back(TGenStep(id), id.Cookie(), id.BlobSize());
                Self->AsStats.BytesToCopy += id.BlobSize();
                invalidate = true;
            }
            LastPlanScannedKey.emplace(key.GetBlobId());
            if (++numItems % 1024 == 0 && endTime <= GetCycleCountFast()) {
                timeout = true;
                return false;
            } else {
                return true;
            }
        });

        if (invalidate) {
            Self->JsonHandler.Invalidate();
        }

        if (timeout) {
            ResumeScanDataForPlanningInFlight = true;
            TActivationContext::Send(new IEventHandle(TEvPrivate::EvResumeScanDataForPlanning, 0, SelfId(), {}, nullptr, 0));
            return;
        }

        for (const auto& [key, value] : LeastBlobQueue) {
            if (value.empty()) {
                const auto& [tabletId, channel] = key;
                Self->Data->ExecuteTxHardCollectAssimilatedBlobs(tabletId, channel, TGenStep(Max<ui32>(), Max<ui32>()),
                    SelfId());
            }
        }

        ActionInProgress = false;
        PlanningComplete = true;
        Action();
    }

    void TAssimilator::HandleResumeScanDataForPlanning() {
        Y_ABORT_UNLESS(ResumeScanDataForPlanningInFlight);
        ResumeScanDataForPlanningInFlight = false;
        ScanDataForPlanning();
    }

    void TAssimilator::ScanDataForCopying() {
        if (ResumeScanDataForCopyingInFlight) {
            return;
        }

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT54, "TAssimilator::ScanDataForCopying", (Id, Self->GetLogId()),
            (LastScannedKey, LastScannedKey), (NumGets, Gets.size()));

        THPTimer timer;

        while (Gets.size() < MaxGetsUnprocessed) {
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
                (EntriesToProcess, EntriesToProcess), (Timeout, timeout), (NumGets, Gets.size()));

            if (timeout) { // timeout hit, reschedule work
                TActivationContext::Send(new IEventHandle(TEvPrivate::EvResumeScanDataForCopying, 0, SelfId(), {}, nullptr, 0));
                ResumeScanDataForCopyingInFlight = true;
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
                Gets.try_emplace(getId);
                ScanQ.clear();
                TotalSize = 0;
                continue;
            } else if (!Gets.empty()) {
                // there are some unprocessed get queries, still have to wait
            } else if (!EntriesToProcess) { // we have finished scanning the whole table without any entries, copying is done
                OnCopyDone();
            } else { // we have finished scanning, but we have replicated some data, restart scanning to ensure that nothing left
                LastScannedKey.reset();
                LastPlanScannedKey.reset();
                EntriesToProcess = PlanningComplete = ActionInProgress = false;
                LeastBlobQueue.clear();
                Action();
            }
            break;
        }
    }

    void TAssimilator::HandleResumeScanDataForCopying() {
        Y_ABORT_UNLESS(ResumeScanDataForCopyingInFlight);
        ResumeScanDataForCopyingInFlight = false;
        ScanDataForCopying();
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        auto& msg = *ev->Get();
        (msg.Status == NKikimrProto::OK ? Self->AsStats.LatestOkGet : Self->AsStats.LatestErrorGet) = TInstant::Now();

        const auto it = Gets.find(ev->Cookie);
        Y_ABORT_UNLESS(it != Gets.end());
        TGetBatch& get = it->second;

        for (ui32 i = 0; i < msg.ResponseSz; ++i) {
            auto& resp = msg.Responses[i];

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT34, "got TEvGetResult", (Id, Self->GetLogId()), (BlobId, resp.Id),
                (Status, resp.Status), (NumGets, Gets.size()));

            switch (resp.Status) {
                case NKikimrProto::OK:
                    Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_DECOMMIT_GET_BYTES] += resp.Buffer.GetSize();
                    ++Self->AsStats.BlobsReadOk;

                    if (std::vector<ui8> channels(1); Self->PickChannels(NKikimrBlobDepot::TChannelKind::Data, channels)) {
                        TChannelInfo& channel = Self->Channels[channels.front()];
                        const ui64 value = channel.NextBlobSeqId++;
                        const auto blobSeqId = TBlobSeqId::FromSequentalNumber(channel.Index, Self->Executor()->Generation(), value);
                        const TLogoBlobID id = blobSeqId.MakeBlobId(Self->TabletID(), EBlobType::VG_DATA_BLOB, 0, resp.Id.BlobSize());
                        const ui64 putId = NextPutId++;
                        SendToBSProxy(SelfId(), channel.GroupId, new TEvBlobStorage::TEvPut(id, TRcBuf(resp.Buffer), TInstant::Max()), putId);
                        const bool inserted = channel.AssimilatedBlobsInFlight.insert(value).second; // prevent from barrier advancing
                        Y_ABORT_UNLESS(inserted);
                        const bool inserted1 = Puts.try_emplace(putId, TData::TKey(resp.Id), it->first).second;
                        Y_ABORT_UNLESS(inserted1);
                        ++get.PutsPending;
                    }
                    break;

                case NKikimrProto::NODATA:
                    ++Self->AsStats.BlobsReadNoData;
                    Self->AsStats.BytesToCopy -= resp.Id.BlobSize();

                    get.BlobIds.push_back(resp.Id);
                    get.AssimilatedBlobs.push_back({TData::TKey(resp.Id), TData::TAssimilatedBlobInfo::TDrop{}});
                    break;

                default:
                    ++Self->AsStats.BlobsReadError;
                    continue;
            }

            Self->AsStats.LastReadBlobId = resp.Id;
        }

        if (!get.PutsPending) {
            Self->Data->ExecuteTxCommitAssimilatedBlob(std::move(get.AssimilatedBlobs), TEvPrivate::EvTxComplete,
                SelfId(), it->first);
        }

        Self->JsonHandler.Invalidate();
    }

    void TAssimilator::HandleTxComplete(TAutoPtr<IEventHandle> ev) {
        const auto it = Gets.find(ev->Cookie);
        Y_ABORT_UNLESS(it != Gets.end());
        TGetBatch& get = it->second;

        for (const TLogoBlobID& id : get.BlobIds) {
            const auto qIt = LeastBlobQueue.find(std::make_tuple(id.TabletID(), id.Channel()));
            Y_ABORT_UNLESS(qIt != LeastBlobQueue.end());
            auto& items = qIt->second;
            Y_ABORT_UNLESS(!items.empty());

            const auto value = std::make_tuple(TGenStep(id), id.Cookie(), id.BlobSize());
            auto it = items.front() == value ? items.begin() : std::ranges::lower_bound(items, value);
            Y_ABORT_UNLESS(it != items.end() && *it == value);

            if (it == items.begin()) {
                const TGenStep beginGenStep = std::get<0>(items.front());

                // remove front item and trim any pending items
                items.pop_front();
                while (!items.empty() && !std::get<2>(items.front())) {
                    items.pop_front();
                }

                std::optional<TGenStep> barrier;
                if (items.empty()) {
                    // we have processed all the data from this tablet/channel pair
                    barrier.emplace(Max<ui32>(), Max<ui32>());
                } else if (std::get<0>(items.front()) != beginGenStep) {
                    // gen/step has been changed
                    barrier.emplace(std::get<0>(items.front()).Previous());
                }
                if (barrier) {
                    // issue hard barrier cmd
                    Self->Data->ExecuteTxHardCollectAssimilatedBlobs(id.TabletID(), id.Channel(), *barrier, SelfId());
                }
            } else {
                auto& [genStep, cookie, blobSize] = *it;
                blobSize = 0; // mark this item as already processed
            }
        }

        Gets.erase(it);
        ScanDataForCopying();
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvPutResult::TPtr ev) {
        auto& msg = *ev->Get();

        // adjust counters
        (msg.Status == NKikimrProto::OK ? Self->AsStats.LatestOkPut : Self->AsStats.LatestErrorPut) = TInstant::Now();
        if (msg.Status == NKikimrProto::OK) {
            Self->TabletCounters->Cumulative()[NKikimrBlobDepot::COUNTER_DECOMMIT_PUT_OK_BYTES] += msg.Id.BlobSize();
            ++Self->AsStats.BlobsPutOk;
            Self->AsStats.BytesToCopy -= msg.Id.BlobSize();
            Self->AsStats.BytesCopied += msg.Id.BlobSize();
        } else {
            ++Self->AsStats.BlobsPutError;
        }
        Self->JsonHandler.Invalidate();

        // find matching put record
        const auto it = Puts.find(ev->Cookie);
        Y_ABORT_UNLESS(it != Puts.end());
        auto [key, getId] = std::move(it->second);
        Puts.erase(it);

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT37, "got TEvPutResult", (Id, Self->GetLogId()), (Msg, msg),
            (NumGets, Gets.size()), (Key, key));

        // process get
        const auto jt = Gets.find(getId);
        Y_ABORT_UNLESS(jt != Gets.end());
        TGetBatch& get = jt->second;
        if (msg.Status == NKikimrProto::OK) { // mark blob assimilated only in case of success
            get.BlobIds.push_back(key.GetBlobId());
            get.AssimilatedBlobs.push_back({std::move(key), TData::TAssimilatedBlobInfo::TUpdate{
                TBlobSeqId::FromLogoBlobId(msg.Id)}});
        }
        if (!--get.PutsPending) {
            Self->Data->ExecuteTxCommitAssimilatedBlob(std::move(get.AssimilatedBlobs), TEvPrivate::EvTxComplete,
                SelfId(), getId);
        }
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvCollectGarbage::TPtr ev) {
        CollectGarbageQ.emplace(ev->Cookie, ev->Release().Release());
        ProcessCollectGarbageQ();
    }

    void TAssimilator::Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT93, "got TEvCollectGarbageResult", (Id, Self->GetLogId()), (Msg, msg));
        ++(msg.Status == NKikimrProto::OK ? Self->AsStats.CollectGarbageOK : Self->AsStats.CollectGarbageError);

        Y_ABORT_UNLESS(CollectGarbageInFlight);
        --CollectGarbageInFlight;
        ProcessCollectGarbageQ();
    }

    void TAssimilator::ProcessCollectGarbageQ() {
        const ui32 maxInFlight = CollectGarbageQ.size() >= 1000 ? 4 :
            CollectGarbageQ.size() >= 100 ? 3 :
            CollectGarbageQ.size() >= 10 ? 2 : 1;
        while (!CollectGarbageQ.empty() && CollectGarbageInFlight < maxInFlight) {
            auto& [counter, ev] = *CollectGarbageQ.begin();
            if (counter == ExpectedPerGenerationCounter) {
                SendToBSProxy(SelfId(), Self->Config.GetVirtualGroupId(), ev.release());
                CollectGarbageQ.erase(CollectGarbageQ.begin());
                ++CollectGarbageInFlight;
                ++ExpectedPerGenerationCounter;
            } else {
                break;
            }
        }

        Self->AsStats.CollectGarbageInFlight = CollectGarbageInFlight;
        Self->AsStats.CollectGarbageQueue = CollectGarbageQ.size();
        Self->JsonHandler.Invalidate();
    }

    void TAssimilator::OnCopyDone() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT38, "data copying is done", (Id, Self->GetLogId()));
        Y_ABORT_UNLESS(Gets.empty());

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
                Self->Self->JsonHandler.Invalidate();
                return true;
            }

            void Complete(const TActorContext&) override {
                Self->Self->OnUpdateDecommitState();
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
                    Self->JsonHandler.Invalidate();
                    db.Table<Schema::Config>().Key(Schema::Config::Key::Value).Update(
                        NIceDb::TUpdate<Schema::Config::DecommitState>(Self->DecommitState)
                    );
                    return true;
                }

                void Complete(const TActorContext&) override {
                    Self->OnUpdateDecommitState();
                }
            };

            Self->GroupAssimilatorId = {};
            Self->JsonHandler.Invalidate();
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

    void TAssimilator::UpdateAssimilatorPosition() const {
        Self->AsStats.SkipBlocksUpTo = SkipBlocksUpTo;
        Self->AsStats.SkipBarriersUpTo = SkipBarriersUpTo;
        Self->AsStats.SkipBlobsUpTo = SkipBlobsUpTo;
        Self->JsonHandler.Invalidate();
    }

    void TAssimilator::UpdateBytesCopiedQ() {
        while (BytesCopiedQ.size() >= 3) {
            BytesCopiedQ.pop_front();
        }
        BytesCopiedQ.emplace_back(TActivationContext::Monotonic(), Self->AsStats.BytesCopied);

        Self->AsStats.CopySpeed = 0;
        Self->AsStats.CopyTimeRemaining = TDuration::Max();

        if (BytesCopiedQ.size() > 1) {
            const auto& [frontTs, frontBytes] = BytesCopiedQ.front();
            const auto& [backTs, backBytes] = BytesCopiedQ.back();
            const TDuration deltaTs = backTs - frontTs;
            const ui64 deltaBytes = backBytes - frontBytes;
            if (deltaTs != TDuration::Zero()) {
                Self->AsStats.CopySpeed = deltaBytes * 1'000'000 / deltaTs.MicroSeconds();
            }
            if (deltaBytes) {
                Self->AsStats.CopyTimeRemaining = TDuration::MicroSeconds(Self->AsStats.BytesToCopy *
                    deltaTs.MicroSeconds() / deltaBytes);
            }
        }

        Self->JsonHandler.Invalidate();
        TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvUpdateBytesCopiedQ, 0,
            SelfId(), {}, nullptr, 0));
    }

    void TBlobDepot::TAsStats::ToJson(NJson::TJsonValue& json, bool pretty) const {
        auto formatSize = [&](ui64 size) { return pretty ? FormatByteSize(size) : ToString(size); };

        json["d.skip_blocks_up_to"] = SkipBlocksUpTo
            ? ToString(*SkipBlocksUpTo)
            : "<null>";
        json["d.skip_barriers_up_to"] = SkipBarriersUpTo
            ? TStringBuilder() << std::get<0>(*SkipBarriersUpTo) << ':' << (int)std::get<1>(*SkipBarriersUpTo)
            : "<null>"_sb;
        json["d.skip_blobs_up_to"] = SkipBlobsUpTo
            ? SkipBlobsUpTo->ToString()
            : "<null>";
        json["d.latest_error_get"] = LatestErrorGet.ToString();
        json["d.latest_ok_get"] = LatestOkGet.ToString();
        json["d.latest_error_put"] = LatestErrorPut.ToString();
        json["d.latest_ok_put"] = LatestOkPut.ToString();
        json["d.last_read_blob_id"] = LastReadBlobId.ToString();
        json["d.bytes_to_copy"] = formatSize(BytesToCopy);
        json["d.bytes_copied"] = formatSize(BytesCopied);
        json["d.copy_speed"] = TStringBuilder() << formatSize(CopySpeed) << "/s";
        json["d.copy_time_remaining"] = TStringBuilder() << CopyTimeRemaining;
        json["d.blobs_read_ok"] = ToString(BlobsReadOk);
        json["d.blobs_read_nodata"] = ToString(BlobsReadNoData);
        json["d.blobs_read_error"] = ToString(BlobsReadError);
        json["d.blobs_put_ok"] = ToString(BlobsPutOk);
        json["d.blobs_put_error"] = ToString(BlobsPutError);
        json["d.copy_iteration"] = ToString(CopyIteration);
        json["d.collect_garbage_in_flight"] = ToString(CollectGarbageInFlight);
        json["d.collect_garbage_queue"] = ToString(CollectGarbageQueue);
        json["d.collect_garbage_ok"] = ToString(CollectGarbageOK);
        json["d.collect_garbage_error"] = ToString(CollectGarbageError);
    }

    void TBlobDepot::TData::ExecuteTxCommitAssimilatedBlob(std::vector<TAssimilatedBlobInfo>&& blobs, ui32 notifyEventType,
            TActorId parentId, ui64 cookie) {
        Self->Execute(std::make_unique<TTxCommitAssimilatedBlob>(Self, std::move(blobs), notifyEventType, parentId, cookie));
    }

    void TBlobDepot::TData::ExecuteTxHardCollectAssimilatedBlobs(ui64 tabletId, ui8 channel, TGenStep barrier, TActorId parentId) {
        Self->Execute(std::make_unique<TTxHardCollectAssimilatedBlobs>(Self, tabletId, channel, barrier, parentId));
    }

    void TBlobDepot::StartGroupAssimilator() {
        if (Config.GetIsDecommittingGroup() && DecommitState != EDecommitState::Done) {
           Y_ABORT_UNLESS(!GroupAssimilatorId);
           Y_ABORT_UNLESS(Data->IsLoaded());
           GroupAssimilatorId = RegisterWithSameMailbox(new TGroupAssimilator(this));
           JsonHandler.Invalidate();
        }
    }

    void TBlobDepot::OnUpdateDecommitState() {
        auto&& c = TabletCounters->Simple();
        const bool d = Configured && Config.GetIsDecommittingGroup(); // is decommission enabled for this tablet?
        using E = EDecommitState;
        c[NKikimrBlobDepot::COUNTER_DECOMMIT_MODE_PREPARING] = d && DecommitState < E::BlocksFinished;
        c[NKikimrBlobDepot::COUNTER_DECOMMIT_MODE_IN_PROGRESS] = d && E::BlocksFinished <= DecommitState && DecommitState < E::Done;
        c[NKikimrBlobDepot::COUNTER_DECOMMIT_MODE_DONE] = d && E::Done <= DecommitState;
    }

} // NKikimr::NBlobDepot
