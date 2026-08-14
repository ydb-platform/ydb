#include "blob_depot_tablet.h"
#include "data.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    namespace {

        static constexpr ui32 MaxMoveDataKeysPerTx = 10'000;

        TLogoBlobID MakeFirstBlobId(ui64 tabletId, const NKikimrBlobDepot::TBlobLocator& locator) {
            const TBlobSeqId blobSeqId = TBlobSeqId::FromProto(locator.GetBlobSeqId());
            if (!locator.GetFooterLen()) {
                return blobSeqId.MakeBlobId(tabletId, EBlobType::VG_DATA_BLOB, 0, locator.GetTotalDataLen());
            } else if (locator.GetTotalDataLen() + locator.GetFooterLen() > MaxBlobSize) {
                return blobSeqId.MakeBlobId(tabletId, EBlobType::VG_DATA_BLOB, 0, locator.GetTotalDataLen());
            } else {
                return blobSeqId.MakeBlobId(tabletId, EBlobType::VG_COMPOSITE_BLOB, 0,
                    locator.GetTotalDataLen() + locator.GetFooterLen());
            }
        }

        std::vector<TLogoBlobID> MakeBlobIds(ui64 tabletId, const NKikimrBlobDepot::TBlobLocator& locator) {
            const TBlobSeqId blobSeqId = TBlobSeqId::FromProto(locator.GetBlobSeqId());
            if (!locator.GetFooterLen()) {
                return {blobSeqId.MakeBlobId(tabletId, EBlobType::VG_DATA_BLOB, 0, locator.GetTotalDataLen())};
            } else if (locator.GetTotalDataLen() + locator.GetFooterLen() > MaxBlobSize) {
                return {
                    blobSeqId.MakeBlobId(tabletId, EBlobType::VG_DATA_BLOB, 0, locator.GetTotalDataLen()),
                    blobSeqId.MakeBlobId(tabletId, EBlobType::VG_FOOTER_BLOB, 0, locator.GetFooterLen()),
                };
            } else {
                return {blobSeqId.MakeBlobId(tabletId, EBlobType::VG_COMPOSITE_BLOB, 0,
                    locator.GetTotalDataLen() + locator.GetFooterLen())};
            }
        }

    } // anonymous namespace

    class TBlobDepot::TTxMoveDataScan
        : public NTabletFlatExecutor::TTransactionBase<TBlobDepot>
    {
        bool SendContinue = false;

    public:
        TTxType GetTxType() const override {
            return NKikimrBlobDepot::TXTYPE_MOVE_DATA_SCAN;
        }

        explicit TTxMoveDataScan(TBlobDepot *self)
            : TTransactionBase(self)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            auto& state = Self->MoveData;
            Y_ABORT_UNLESS(state.Phase == TMoveDataState::EPhase::ScanningIndex);

            bool progress = false;
            bool found = false;
            bool stoppedByLimit = false;
            ui32 keysProcessed = 0;

            TData::TScanRange range{
                state.Key
                    ? TData::TKey::FromBinaryKey(*state.Key, Self->Config)
                    : TData::TKey::Min(),
                TData::TKey::Max(),
                TData::EScanFlags::INCLUDE_BEGIN,
            };

            const bool scanFinished = Self->Data->ScanRange(range, &txc, &progress,
                [&](const TData::TKey& key, const TData::TValue& value) {
                    const TString binaryKey = key.MakeBinaryKey();
                    const int beginIndex = state.Key && *state.Key == binaryKey
                        ? static_cast<int>(state.ValueChainIndex)
                        : 0;

                    for (int index = beginIndex; index < value.ValueChain.size(); ++index) {
                        const auto& item = value.ValueChain[index];
                        if (item.HasBlobLocator() && Self->NeedMoveBlob(item.GetBlobLocator())) {
                            state.Key = binaryKey;
                            state.ValueChainIndex = static_cast<ui32>(index);
                            state.ValueVersion = value.ValueVersion;
                            state.BlobLocator.CopyFrom(item.GetBlobLocator());
                            state.BlobId = MakeFirstBlobId(Self->TabletID(), state.BlobLocator);
                            auto it = state.BlobIdToNewLocator.find(state.BlobId);
                            if (it != state.BlobIdToNewLocator.end()) {
                                const TBlobSeqId newBlobSeqId = TBlobSeqId::FromProto(it->second.GetBlobSeqId());
                                const TLogoBlobID newBlobId = MakeFirstBlobId(Self->TabletID(), it->second);
                                if (state.ProtectedBlobSeqIds.contains(newBlobSeqId) ||
                                        Self->Data->IsBlobReferenced(newBlobId)) {
                                    state.NewBlobLocator.CopyFrom(it->second);
                                    state.NewBlobSeqId = newBlobSeqId;
                                    state.Phase = TMoveDataState::EPhase::UpdatingIndex;
                                } else {
                                    state.BlobIdToNewLocator.erase(it);
                                    state.Phase = TMoveDataState::EPhase::CopyingBlob;
                                }
                            } else {
                                state.Phase = TMoveDataState::EPhase::CopyingBlob;
                            }
                            found = true;
                            return false;
                        }
                    }

                    state.Key = binaryKey;
                    state.ValueChainIndex = static_cast<ui32>(value.ValueChain.size());
                    if (++keysProcessed >= MaxMoveDataKeysPerTx) {
                        stoppedByLimit = true;
                        return false;
                    }
                    return true;
                });

            if (found) {
                SendContinue = true;
                return true;
            }
            if (!scanFinished && !progress) {
                return false;
            }
            if (stoppedByLimit || !scanFinished) {
                SendContinue = true;
                return true;
            }

            if (state.NeedsAnotherPass) {
                Self->RestartMoveDataScan();
            } else {
                state.Phase = TMoveDataState::EPhase::CheckingTrash;
            }
            SendContinue = true;
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            if (SendContinue) {
                ctx.Send(Self->SelfId(), new TEvMoveDataContinue);
            }
        }
    };

    class TBlobDepot::TTxMoveDataUpdateIndex
        : public NTabletFlatExecutor::TTransactionBase<TBlobDepot>
    {
        TData::EMoveDataReplaceResult Result = TData::EMoveDataReplaceResult::KeyChanged;

    public:
        TTxType GetTxType() const override {
            return NKikimrBlobDepot::TXTYPE_MOVE_DATA_UPDATE_INDEX;
        }

        explicit TTxMoveDataUpdateIndex(TBlobDepot *self)
            : TTransactionBase(self)
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            auto& state = Self->MoveData;
            Y_ABORT_UNLESS(state.Phase == TMoveDataState::EPhase::UpdatingIndex);
            Y_ABORT_UNLESS(state.Key);

            const TData::TKey key = TData::TKey::FromBinaryKey(*state.Key, Self->Config);
            Result = Self->Data->ReplaceLocatorForMoveData(key, state.ValueChainIndex, state.ValueVersion,
                state.BlobLocator, state.NewBlobLocator, txc, this);
            return true;
        }

        void Complete(const TActorContext& ctx) override {
            auto& state = Self->MoveData;
            Self->Data->CommitTrash(this);

            if (Result == TData::EMoveDataReplaceResult::Replaced) {
                Self->ReleaseMoveDataBlobSeqId(TBlobSeqId::FromProto(state.NewBlobLocator.GetBlobSeqId()));
                ++state.ValueChainIndex;
            } else {
                state.ValueChainIndex = 0;
            }

            state.RecordTouched = false;
            state.BlobId = {};
            state.BlobLocator.Clear();
            state.NewBlobLocator.Clear();
            state.NewBlobSeqId = {};
            state.Phase = TMoveDataState::EPhase::ScanningIndex;
            ctx.Send(Self->SelfId(), new TEvMoveDataContinue);
        }
    };

    class TBlobDepot::TMoveDataCopyActor
        : public TActorBootstrapped<TMoveDataCopyActor>
    {
        const TActorId OwnerId;
        const ui32 SourceGroupId;
        const ui32 TargetGroupId;
        NKikimrBlobDepot::TBlobLocator NewLocator;
        const std::vector<TLogoBlobID> SourceBlobIds;
        const std::vector<TLogoBlobID> TargetBlobIds;
        size_t Index = 0;

    public:
        TMoveDataCopyActor(TActorId ownerId, ui64 tabletId,
                const NKikimrBlobDepot::TBlobLocator& sourceLocator,
                NKikimrBlobDepot::TBlobLocator newLocator)
            : OwnerId(ownerId)
            , SourceGroupId(sourceLocator.GetGroupId())
            , TargetGroupId(newLocator.GetGroupId())
            , NewLocator(std::move(newLocator))
            , SourceBlobIds(MakeBlobIds(tabletId, sourceLocator))
            , TargetBlobIds(MakeBlobIds(tabletId, NewLocator))
        {
            Y_ABORT_UNLESS(SourceBlobIds.size() == TargetBlobIds.size());
        }

        void Bootstrap() {
            IssueGet();
            Become(&TThis::StateGet);
        }

        void IssueGet() {
            const TLogoBlobID& id = SourceBlobIds[Index];
            SendToBSProxy(SelfId(), SourceGroupId, new TEvBlobStorage::TEvGet(
                id, 0, id.BlobSize(), TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::LowRead));
        }

        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
            auto& msg = *ev->Get();
            if (msg.Status != NKikimrProto::OK || msg.ResponseSz != 1 ||
                    msg.Responses[0].Status != NKikimrProto::OK) {
                const NKikimrProto::EReplyStatus status = msg.Status != NKikimrProto::OK
                    ? msg.Status
                    : msg.ResponseSz == 1 ? msg.Responses[0].Status : NKikimrProto::ERROR;
                return ReplyAndDie(status, msg.ErrorReason);
            }

            auto& response = msg.Responses[0];
            if (response.Buffer.size() != SourceBlobIds[Index].BlobSize()) {
                return ReplyAndDie(NKikimrProto::ERROR, "move data blob size mismatch");
            }

            SendToBSProxy(SelfId(), TargetGroupId, new TEvBlobStorage::TEvPut(
                TEvBlobStorage::TEvPut::TParameters{
                    .BlobId = TargetBlobIds[Index],
                    .Buffer = std::move(response.Buffer),
                    .Deadline = TInstant::Max(),
                    .HandleClass = NKikimrBlobStorage::AsyncBlob,
                    .Tactic = TEvBlobStorage::TEvPut::TacticDefault,
                    .WriteSource = TWriteSource::BlobDepotPut,
                }));
            Become(&TThis::StatePut);
        }

        void Handle(TEvBlobStorage::TEvPutResult::TPtr ev) {
            auto& msg = *ev->Get();
            if (msg.Status != NKikimrProto::OK) {
                return ReplyAndDie(msg.Status, msg.ErrorReason);
            }

            if (++Index == SourceBlobIds.size()) {
                return ReplyAndDie(NKikimrProto::OK, {});
            }

            IssueGet();
            Become(&TThis::StateGet);
        }

        void ReplyAndDie(NKikimrProto::EReplyStatus status, TString errorReason) {
            Send(OwnerId, new TEvMoveDataBlobCopied(status, std::move(NewLocator), std::move(errorReason)));
            PassAway();
        }

        STATEFN(StateGet) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBlobStorage::TEvGetResult, Handle);
            }
        }

        STATEFN(StatePut) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBlobStorage::TEvPutResult, Handle);
            }
        }
    };

    void TBlobDepot::Handle(TEvTablet::TEvMoveData::TPtr ev) {
        YDB_LOG_DEBUG("Handle TEvMoveData",
            {"marker", "BDM01"},
            {"id", GetLogId()},
            {"ev", ev->Get()->ToString()});

        if (MoveData.IsInProgress()) {
            MoveDataRequestsQueue.push_back(ev);
            return;
        }

        TSet<ui32> moveDataGroups;
        for (const ui32 groupId : ev->Get()->Record.GetGroups()) {
            moveDataGroups.insert(groupId);
        }

        if (!ValidateMoveDataGroups(moveDataGroups, ev->Sender)) {
            return;
        }

        StartMoveData(std::move(moveDataGroups), ev->Sender);
    }

    bool TBlobDepot::ValidateMoveDataGroups(const TSet<ui32>& moveDataGroups, const TActorId& sender) const {
        ui32 channelId = 0;
        for (const auto& channel : Info()->Channels) {
            if (moveDataGroups.contains(channel.LatestEntry()->GroupID)) {
                TString errorReason = TStringBuilder()
                    << "Group " << channel.LatestEntry()->GroupID
                    << " is in latest history entry in channel " << channelId
                    << " for tablet " << TabletID();
                Send(sender, new TEvTablet::TEvMoveDataResponse(
                    TabletID(),
                    NKikimrTabletBase::TEvMoveDataResponse::ErrorGroupIdMismatch,
                    errorReason));
                return false;
            }
            ++channelId;
        }
        return true;
    }

    bool TBlobDepot::NeedMoveBlob(const NKikimrBlobDepot::TBlobLocator& locator) const {
        Y_ABORT_UNLESS(locator.HasGroupId());
        Y_ABORT_UNLESS(locator.HasBlobSeqId());

        const auto& blobSeqId = locator.GetBlobSeqId();
        const ui32 groupId = Info()->GroupFor(blobSeqId.GetChannel(), blobSeqId.GetGeneration());
        Y_ABORT_UNLESS(groupId != Max<ui32>());
        Y_ABORT_UNLESS(groupId == locator.GetGroupId());
        return MoveData.Groups.contains(groupId);
    }

    void TBlobDepot::StartMoveData(TSet<ui32>&& moveDataGroups, const TActorId& sender) {
        YDB_LOG_DEBUG("StartMoveData",
            {"marker", "BDM02"},
            {"id", GetLogId()});

        Y_ABORT_UNLESS(!MoveData.IsInProgress());
        MoveData = {
            .Phase = TMoveDataState::EPhase::ScanningIndex,
            .Groups = std::move(moveDataGroups),
            .RequestSender = sender,
        };

        ContinueMoveData();
    }

    void TBlobDepot::ContinueMoveData() {
        switch (MoveData.Phase) {
            case TMoveDataState::EPhase::ScanningIndex:
                Execute(std::make_unique<TTxMoveDataScan>(this));
                break;

            case TMoveDataState::EPhase::CopyingBlob:
                StartMoveDataBlobCopy();
                break;

            case TMoveDataState::EPhase::UpdatingIndex:
                Execute(std::make_unique<TTxMoveDataUpdateIndex>(this));
                break;

            case TMoveDataState::EPhase::CheckingTrash: {
                if (MoveData.NeedsAnotherPass) {
                    RestartMoveDataScan();
                    ContinueMoveData();
                    break;
                }

                for (const TBlobSeqId& blobSeqId : MoveData.ProtectedBlobSeqIds) {
                    TChannelInfo& channel = Channels[blobSeqId.Channel];
                    const size_t numErased = channel.AssimilatedBlobsInFlight.erase(blobSeqId.ToSequentialNumber());
                    Y_ABORT_UNLESS(numErased == 1);
                    Data->OnLeastExpectedBlobIdChange(channel.Index);
                }
                MoveData.ProtectedBlobSeqIds.clear();

                switch (Data->CheckMoveDataTrash(MoveData.Groups)) {
                    case TData::EMoveDataTrashStatus::Clear:
                        MoveData.Phase = TMoveDataState::EPhase::Vacuum;
                        Executor()->StartMoveDataVacuumFromOwner();
                        break;

                    case TData::EMoveDataTrashStatus::NeedsIndexRescan:
                        RestartMoveDataScan();
                        ContinueMoveData();
                        break;

                    case TData::EMoveDataTrashStatus::WaitingForGC:
                        TActivationContext::Schedule(TDuration::MilliSeconds(100), new IEventHandle(
                            TEvPrivate::EvMoveDataContinue, 0, SelfId(), {}, nullptr, 0));
                        break;
                }
                break;
            }

            case TMoveDataState::EPhase::Vacuum:
                break;

            case TMoveDataState::EPhase::Idle:
                Y_ABORT();
        }
    }

    void TBlobDepot::StartMoveDataBlobCopy() {
        Y_ABORT_UNLESS(MoveData.Phase == TMoveDataState::EPhase::CopyingBlob);

        std::vector<ui8> channels(1);
        if (!PickChannels(NKikimrBlobDepot::TChannelKind::Data, channels)) {
            YDB_LOG_CRIT("Move data failed to allocate target channel",
                {"marker", "BDM05"},
                {"id", GetLogId()});
            Send(SelfId(), new TEvents::TEvPoisonPill);
            return;
        }

        TChannelInfo& channel = Channels[channels.front()];
        const ui64 value = channel.NextBlobSeqId++;
        const TBlobSeqId blobSeqId = TBlobSeqId::FromSequentalNumber(
            channel.Index, Executor()->Generation(), value);
        const bool inserted = channel.AssimilatedBlobsInFlight.insert(value).second;
        Y_ABORT_UNLESS(inserted);
        const bool protectedInserted = MoveData.ProtectedBlobSeqIds.insert(blobSeqId).second;
        Y_ABORT_UNLESS(protectedInserted);

        NKikimrBlobDepot::TBlobLocator newLocator;
        newLocator.CopyFrom(MoveData.BlobLocator);
        newLocator.SetGroupId(channel.GroupId);
        blobSeqId.ToProto(newLocator.MutableBlobSeqId());

        MoveData.NewBlobSeqId = blobSeqId;
        RegisterWithSameMailbox(new TMoveDataCopyActor(
            SelfId(), TabletID(), MoveData.BlobLocator, std::move(newLocator)));
    }

    void TBlobDepot::Handle(TEvMoveDataBlobCopied::TPtr ev) {
        Y_ABORT_UNLESS(MoveData.Phase == TMoveDataState::EPhase::CopyingBlob);
        const TBlobSeqId blobSeqId = TBlobSeqId::FromProto(ev->Get()->NewLocator.GetBlobSeqId());
        Y_ABORT_UNLESS(blobSeqId == MoveData.NewBlobSeqId);

        if (ev->Get()->Status != NKikimrProto::OK) {
            ReleaseMoveDataBlobSeqId(blobSeqId);
            YDB_LOG_CRIT("Move data blob copy failed",
                {"marker", "BDM06"},
                {"id", GetLogId()},
                {"blobId", MoveData.BlobId},
                {"status", ev->Get()->Status},
                {"errorReason", ev->Get()->ErrorReason});
            Send(SelfId(), new TEvents::TEvPoisonPill);
            return;
        }

        const bool inserted = MoveData.BlobIdToNewLocator.emplace(
            MoveData.BlobId, ev->Get()->NewLocator).second;
        Y_ABORT_UNLESS(inserted);
        MoveData.NewBlobLocator.CopyFrom(ev->Get()->NewLocator);
        MoveData.Phase = TMoveDataState::EPhase::UpdatingIndex;

        YDB_LOG_DEBUG("Move data blob copied; updating index",
            {"marker", "BDM07"},
            {"id", GetLogId()},
            {"blobId", MoveData.BlobId},
            {"newBlobSeqId", blobSeqId});

        ContinueMoveData();
    }

    void TBlobDepot::ReleaseMoveDataBlobSeqId(const TBlobSeqId& blobSeqId) {
        if (!MoveData.ProtectedBlobSeqIds.erase(blobSeqId)) {
            return;
        }

        TChannelInfo& channel = Channels[blobSeqId.Channel];
        const ui32 generation = Executor()->Generation();
        const TBlobSeqId leastExpectedBlobIdBefore = channel.GetLeastExpectedBlobId(generation);
        const size_t numErased = channel.AssimilatedBlobsInFlight.erase(blobSeqId.ToSequentialNumber());
        Y_ABORT_UNLESS(numErased == 1);
        if (leastExpectedBlobIdBefore != channel.GetLeastExpectedBlobId(generation)) {
            Data->OnLeastExpectedBlobIdChange(channel.Index);
        }
    }

    void TBlobDepot::RestartMoveDataScan() {
        MoveData.Key.reset();
        MoveData.ValueChainIndex = 0;
        MoveData.ValueVersion = 0;
        MoveData.RecordTouched = false;
        MoveData.NeedsAnotherPass = false;
        MoveData.BlobId = {};
        MoveData.BlobLocator.Clear();
        MoveData.NewBlobLocator.Clear();
        MoveData.NewBlobSeqId = {};
        MoveData.Phase = TMoveDataState::EPhase::ScanningIndex;
    }

    void TBlobDepot::FinishMoveData(const TActorContext& ctx) {
        YDB_LOG_DEBUG("FinishMoveData",
            {"marker", "BDM03"},
            {"id", GetLogId()});

        Y_ABORT_UNLESS(MoveData.IsInProgress());
        Y_ABORT_UNLESS(MoveData.ProtectedBlobSeqIds.empty());
        ctx.Send(MoveData.RequestSender, new TEvTablet::TEvMoveDataResponse(
            TabletID(),
            NKikimrTabletBase::TEvMoveDataResponse::Success));

        MoveData = {};
    }

    void TBlobDepot::MoveDataCompleted(const TActorContext& ctx) {
        YDB_LOG_DEBUG("MoveDataCompleted",
            {"marker", "BDM04"},
            {"id", GetLogId()});

        FinishMoveData(ctx);

        while (!MoveDataRequestsQueue.empty()) {
            TEvTablet::TEvMoveData::TPtr ev = MoveDataRequestsQueue.front();
            MoveDataRequestsQueue.pop_front();

            TSet<ui32> moveDataGroups;
            for (const ui32 groupId : ev->Get()->Record.GetGroups()) {
                moveDataGroups.insert(groupId);
            }

            const TActorId sender = ev->Sender;
            if (!ValidateMoveDataGroups(moveDataGroups, sender)) {
                continue;
            }

            StartMoveData(std::move(moveDataGroups), sender);
            break;
        }
    }

} // NKikimr::NBlobDepot
