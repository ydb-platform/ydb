#include "impl.h"
#include "config.h"

#include <ydb/core/util/format.h>
#include <ydb/core/blobstorage/base/storage_pool_pile_override.h>
#include <ydb/core/blobstorage/base/utility.h>

#define YDB_LOG_THIS_FILE_COMPONENT BS_CONTROLLER

namespace NKikimr::NBsController {

class TBlobStorageController::TTxCheckUnsynced : public TTransactionBase<TBlobStorageController> {
    NKikimrBlobStorage::TStorageConfig Config;
    std::optional<TConfigState> State;
    THashSet<TBridgePileId> SyncedPiles;

public:
    TTxCheckUnsynced(TBlobStorageController *controller, const NKikimrBlobStorage::TStorageConfig& config)
        : TTransactionBase(controller)
        , Config(config)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_CHECK_UNSYNCED; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());

        Y_ABORT_UNLESS(Config.HasClusterStateDetails());
        const TBridgePileId pendingOperationPileId = Self->GetPendingOperationPileId();
        for (const auto& pss : Config.GetClusterStateDetails().GetPileSyncState()) {
            if (!pss.GetUnsyncedBSC()) {
                continue;
            }
            const auto unsyncedBridgePileId = TBridgePileId::FromProto(&pss, &std::decay_t<decltype(pss)>::GetBridgePileId);
            if (unsyncedBridgePileId == pendingOperationPileId) {
                continue; // groups don't have all their parts in this pile yet
            }
            bool somethingUnsynced = false;

            // find all bridged groups and check their sync states
            State->Groups.ForEach([&](TGroupId groupId, const TGroupInfo& groupInfo) {
                if (!groupInfo.BridgeGroupInfo) {
                    return; // this is not a bridge proxy group
                }
                const auto& groupState = groupInfo.BridgeGroupInfo->GetBridgeGroupState();
                if (!NBridge::GetGroupPile(groupState, unsyncedBridgePileId)) {
                    return;
                } else if (NBridge::GetAuthorityPileId(groupState) == unsyncedBridgePileId) {
                    return; // nothing is written to the group without this part, it is the one to sync others from
                } else if (const auto it = State->StoragePools.Get().find(groupInfo.StoragePoolId);
                        it != State->StoragePools.Get().end() &&
                        groupInfo.IsBridgePartDropped(unsyncedBridgePileId, it->second.BridgeMode)) {
                    return; // the part is going away, there is nothing to sync
                }
                TGroupInfo *info = State->Groups.FindForUpdate(groupId);
                auto *state = info->BridgeGroupInfo->MutableBridgeGroupState();
                if (auto *pile = NBridge::GetGroupPile(state, unsyncedBridgePileId)) {
                    const auto targetGroupId = TGroupId::FromProto(pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                    if (pile->GetBecameUnsyncedGeneration() < pss.GetBecameUnsyncedGeneration()) {
                        pile->ClearStage(); // initial stage
                        pile->SetBecameUnsyncedGeneration(pss.GetBecameUnsyncedGeneration());
                        State->GroupContentChanged.insert(groupId);

                        // create entry for syncer, 'cause we want one now
                        const auto [it, inserted] = Self->TargetGroupToSyncerState.try_emplace(targetGroupId, groupId,
                            targetGroupId);
                        if (inserted) {
                            Self->EnqueueSyncer(it->second);
                        }
                    }
                    if (pile->GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                        somethingUnsynced = true;
                    }
                }
            });

            // if there were no unsynced groups remaining, then signal to the NW
            if (!somethingUnsynced) {
                SyncedPiles.insert(unsyncedBridgePileId);
            }
        }

        Self->ValidateAndCommitConfigUpdate(State, TConfigTxFlags::SuppressAll(), txc);

        return true;
    }

    void Complete(const TActorContext&) override {
        if (State) {
            State->ApplyConfigUpdates();
        }

        Y_ABORT_UNLESS(Self->CheckingUnsyncedBridgePiles);
        Self->CheckingUnsyncedBridgePiles = !SyncedPiles.empty();
        Self->NotifyBridgeSyncFinishedErrors = false;
        Self->NumPendingBridgeSyncFinishedResponses = SyncedPiles.size();

        for (TBridgePileId bridgePileId : SyncedPiles) {
            NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot request;
            auto *cmd = request.MutableNotifyBridgeSyncFinished();
            cmd->SetGeneration(Config.GetClusterState().GetGeneration());
            bridgePileId.CopyToProto(cmd, &std::decay_t<decltype(*cmd)>::SetBridgePileId);
            cmd->SetBSC(true);

            Self->InvokeOnRoot(std::move(request), [self = Self](NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& result) {
                if (result.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                    self->NotifyBridgeSyncFinishedErrors = true;
                }
                Y_ABORT_UNLESS(self->NumPendingBridgeSyncFinishedResponses);
                if (!--self->NumPendingBridgeSyncFinishedResponses) {
                    Y_ABORT_UNLESS(self->CheckingUnsyncedBridgePiles);
                    self->CheckingUnsyncedBridgePiles = false;
                    // the check requested while the notifications were in flight didn't run, so it runs now
                    if (self->NotifyBridgeSyncFinishedErrors || self->RecheckUnsyncedBridgePiles) {
                        self->CheckUnsyncedBridgePiles();
                    }
                }
            });
        }

        if (Self->RecheckUnsyncedBridgePiles) {
            Self->CheckUnsyncedBridgePiles();
        }

        Self->ProcessSyncers();
    }
};

class TBlobStorageController::TTxUpdateBridgeGroupInfo : public TTransactionBase<TBlobStorageController> {
    const TGroupId GroupId;
    const ui32 GroupGeneration;
    NKikimrBlobStorage::TGroupInfo BridgeGroupInfo;
    const TGroupId TargetGroupId;
    std::optional<TConfigState> State;

public:
    TTxUpdateBridgeGroupInfo(TBlobStorageController *controller, TGroupId groupId, ui32 groupGeneration,
            NKikimrBlobStorage::TGroupInfo&& bridgeGroupInfo, TGroupId targetGroupId)
        : TTransactionBase(controller)
        , GroupId(groupId)
        , GroupGeneration(groupGeneration)
        , BridgeGroupInfo(std::move(bridgeGroupInfo))
        , TargetGroupId(targetGroupId)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_BRIDGE_GROUP_INFO; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());

        if (TGroupInfo *group = State->Groups.FindForUpdate(GroupId); group && group->Generation == GroupGeneration) {
            Y_ABORT_UNLESS(group->BridgeGroupInfo);
            group->BridgeGroupInfo->Swap(&BridgeGroupInfo);
            State->GroupContentChanged.insert(GroupId);
        }

        Self->ValidateAndCommitConfigUpdate(State, TConfigTxFlags::SuppressAll(), txc);

        return true;
    }

    void Complete(const TActorContext&) override {
        if (State) {
            State->ApplyConfigUpdates();
        }

        // target group may have stopped being a pile while this transaction was in flight (e.g. the bridged group was
        // decommitted), then there is nothing to sync anymore
        const auto it = Self->TargetGroupToSyncerState.find(TargetGroupId);
        if (it != Self->TargetGroupToSyncerState.end()) {
            TSyncerState& syncerState = it->second;
            Y_ABORT_UNLESS(syncerState.InCommit);
            syncerState.InCommit = false;
            Self->EnqueueSyncer(syncerState);
            if (TGroupInfo *group = Self->FindGroup(syncerState.BridgeProxyGroupId); group && group->BridgeGroupInfo) {
                if (const auto *pile = NBridge::FindGroupPileByGroupId(group->BridgeGroupInfo->GetBridgeGroupState(),
                        TargetGroupId); pile && pile->GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                    Self->RecheckUnsyncedBridgePiles = true;
                    for (TNodeId nodeId : syncerState.NodeIds | std::views::keys) {
                        const size_t numErased = Self->NodeToSyncerState.erase({nodeId, &syncerState});
                        Self->BridgeSyncNodeRatesDirty = true;
                        Y_ABORT_UNLESS(numErased == 1);
                    }
                    Self->TargetGroupToSyncerState.erase(it);
                }
            }
        }

        Self->CheckUnsyncedBridgePiles();
        Self->ProcessSyncers();
    }
};

class TBlobStorageController::TTxUpdateBridgeSyncState : public TTransactionBase<TBlobStorageController> {
public:
    struct TChangeStage {
        NKikimrBridge::TGroupState::EStage NewStage = {};
        std::optional<ui64> EstimatedBytesToCopy; // kept for the new stage, see TBridgeSyncState
    };

    struct TRegisterError {
        NKikimrBridge::TGroupState::EStage Stage;
        TString Error;
    };

    struct TGroupStateUpdate {
        TGroupId TargetGroupId;
        std::variant<TChangeStage, TRegisterError> Operation;
    };

private:
    std::vector<TGroupStateUpdate> Updates;
    TInstant Timestamp;

public:
    TTxUpdateBridgeSyncState(TBlobStorageController *controller, std::vector<TGroupStateUpdate>&& updates,
            TInstant timestamp)
        : TTransactionBase(controller)
        , Updates(std::move(updates))
        , Timestamp(timestamp)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_BRIDGE_SYNC_STATE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        auto table = db.Table<Schema::BridgeSyncState>();
        for (auto& update : Updates) {
            auto row = table.Key(update.TargetGroupId.GetRawId());
            std::visit(TOverloaded{
                [&](TChangeStage& change) {
                    // when the sync stage gets changed, we forget about any errors that happened before
                    row.Delete();
                    Self->BridgeSyncState.erase(update.TargetGroupId);
                    if (change.EstimatedBytesToCopy) {
                        Self->BridgeSyncState[update.TargetGroupId] = {
                            .Stage = change.NewStage,
                            .EstimatedBytesToCopy = change.EstimatedBytesToCopy,
                        };
                        row.Update(
                            NIceDb::TUpdate<Schema::BridgeSyncState::Stage>(change.NewStage),
                            NIceDb::TUpdate<Schema::BridgeSyncState::EstimatedBytesToCopy>(*change.EstimatedBytesToCopy)
                        );
                    }
                },
                [&](TRegisterError& error) {
                    auto& item = Self->BridgeSyncState[update.TargetGroupId];

                    if (item.Stage != error.Stage && item.EstimatedBytesToCopy) {
                        // the estimate is for another stage: the change to the stage it was made for didn't happen
                        item.EstimatedBytesToCopy.reset();
                        row.UpdateToNull<Schema::BridgeSyncState::EstimatedBytesToCopy>();
                    }
                    item.Stage = error.Stage;
                    item.LastError = std::move(error.Error);
                    item.LastErrorTimestamp = Timestamp;

                    if (item.FirstErrorTimestamp == TInstant()) {
                        item.FirstErrorTimestamp = Timestamp;
                        row.Update<Schema::BridgeSyncState::FirstErrorTimestamp>(Timestamp);
                    }

                    ++item.ErrorCount;

                    row.Update(
                        NIceDb::TUpdate<Schema::BridgeSyncState::Stage>(item.Stage),
                        NIceDb::TUpdate<Schema::BridgeSyncState::LastError>(item.LastError),
                        NIceDb::TUpdate<Schema::BridgeSyncState::LastErrorTimestamp>(item.LastErrorTimestamp),
                        NIceDb::TUpdate<Schema::BridgeSyncState::ErrorCount>(item.ErrorCount)
                    );
                }
            }, update.Operation);
            Self->SysViewChangedGroups.insert(update.TargetGroupId);
        }
        return true;
    }

    void Complete(const TActorContext&) override {
    }
};

class TBlobStorageController::TTxConvertToBridge : public TTransactionBase<TBlobStorageController> {
    std::optional<TConfigState> State;
    bool Finished = false;
    bool Failed = false;
    ui64 ConversionId = 0; // the one distconf has started, so that it can tell late notification of another one

public:
    TTxConvertToBridge(TBlobStorageController *controller)
        : TTransactionBase(controller)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_CONVERT_TO_BRIDGE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());
        ConversionId = Self->StorageConfig->GetClusterStateDetails().GetBscConversionId();

        // new groups are created bridged from now on
        auto needsBridgeMode = [](const TStoragePoolInfo& pool) { return !pool.BridgeMode && !pool.DDisk; };
        if (std::ranges::any_of(State->StoragePools.Get() | std::views::values, needsBridgeMode)) {
            for (auto& pool : State->StoragePools.Unshare() | std::views::values) {
                if (needsBridgeMode(pool)) {
                    pool.BridgeMode = true;
                }
            }
        }

        // turn existing physical groups into inline piles
        bool blocked = false;
        State->Groups.ForEach([&](TGroupId groupId, const TGroupInfo& groupInfo) {
            if (groupInfo.IsBridgeConversionCandidate()) {
                State->Groups.FindForUpdate(groupId)->MakeInlinePile(NBridge::GetInlinePileId());
                State->GroupContentChanged.insert(groupId);
            } else if (groupInfo.BlocksBridgeConversion()) {
                blocked = true;
            }
        });

        if (auto error = Self->ValidateAndCommitConfigUpdate(State, TConfigTxFlags::SuppressAll(), txc)) {
            YDB_LOG_ERROR("Failed to commit bridge conversion",
                {"marker", "BSCBR14"},
                {"error", *error});
            Failed = true;
        } else {
            Finished = !blocked;
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        if (State) {
            State->ApplyConfigUpdates();
        }

        YDB_LOG_INFO("Bridge conversion step complete",
            {"marker", "BSCBR15"},
            {"finished", Finished});

        if (!Finished) {
            Self->OnBridgeConversionStepDone(Failed);
            return;
        }

        NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot request;
        request.MutableNotifyBscConversionFinished()->SetBscConversionId(ConversionId);
        Self->InvokeOnRoot(std::move(request), [self = Self](NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& result) {
            const bool failed = result.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK;
            if (failed) {
                YDB_LOG_WARN("Failed to notify distconf about finished bridge conversion",
                    {"marker", "BSCBR16"},
                    {"result", result});
            }
            self->OnBridgeConversionStepDone(failed);
        });
    }
};

bool TBlobStorageController::StartRetriableStep(TRetriableStep& step) {
    if (step.Running) {
        step.Recheck = true;
        return false;
    }
    step.Running = true;
    step.Recheck = false;
    return true;
}

bool TBlobStorageController::FinishRetriableStep(TRetriableStep& step, bool failed) {
    Y_ABORT_UNLESS(step.Running);
    step.Running = false;
    if (step.Recheck) {
        return true;
    } else if (failed) {
        // nothing else may trigger the check on a quiet cluster, so repeat the whole step (not just the notification, as
        // the conditions might have changed meanwhile) after a while
        if (!step.RetryScheduled) {
            step.RetryScheduled = true;
            TActivationContext::Schedule(step.RetryBackoff.Next(), new IEventHandle(step.RetryEventType, 0, SelfId(),
                {}, nullptr, 0));
        }
    } else {
        step.RetryBackoff.Reset();
    }
    return false;
}

void TBlobStorageController::OnRetriableStepRetry(TRetriableStep& step) {
    Y_ABORT_UNLESS(step.RetryScheduled);
    step.RetryScheduled = false;
}

void TBlobStorageController::CheckBridgeConversion() {
    if (Loaded && StorageConfig && StorageConfig->GetClusterStateDetails().GetBscConversionPending() &&
            StartRetriableStep(BridgeConversionStep)) {
        Execute(std::make_unique<TTxConvertToBridge>(this));
    }
}

void TBlobStorageController::OnBridgeConversionStepDone(bool failed) {
    if (FinishRetriableStep(BridgeConversionStep, failed)) {
        CheckBridgeConversion();
    }
}

void TBlobStorageController::HandleRetryBridgeConversion() {
    OnRetriableStepRetry(BridgeConversionStep);
    CheckBridgeConversion();
}

// BSC part of the pending pile operation, or its validation when Response is set: the changes are rolled back then, and
// the response is sent instead of notifying distconf
class TBlobStorageController::TTxBridgePileOperation : public TTransactionBase<TBlobStorageController> {
protected:
    using TOperation = NKikimrBridge::TClusterStateDetails::TPendingPileOperation;

    const TBridgePileId PileId;
    const TOperation::EKind Kind;
    const ui64 OperationId;
    // the storage config being proposed, if the validation has to take it into account
    const std::optional<NKikimrBlobStorage::TStorageConfig> ProposedStorageConfig;
    std::unique_ptr<IEventHandle> Response;
    std::optional<TConfigState> State;
    size_t NumGroups = 0;
    std::optional<TString> Error;

public:
    TTxBridgePileOperation(TBlobStorageController *controller, TBridgePileId pileId, TOperation::EKind kind,
            ui64 operationId, std::optional<NKikimrBlobStorage::TStorageConfig>&& proposedStorageConfig,
            std::unique_ptr<IEventHandle> response)
        : TTransactionBase(controller)
        , PileId(pileId)
        , Kind(kind)
        , OperationId(operationId)
        , ProposedStorageConfig(std::move(proposedStorageConfig))
        , Response(std::move(response))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic(),
            ProposedStorageConfig ? &*ProposedStorageConfig : nullptr);

        try {
            NumGroups = DoStep(*State);
        } catch (const TExError& ex) {
            Error = ex.what();
            Self->RollbackConfigUpdate(State);
            return true;
        }

        Error = Self->ValidateAndCommitConfigUpdate(State, TConfigTxFlags::SuppressAll(), txc, nullptr,
            /*dryRun=*/ static_cast<bool>(Response));

        return true;
    }

    void Complete(const TActorContext&) override {
        if (State) {
            State->ApplyConfigUpdates();
        }

        YDB_LOG(Error ? PRI_ERROR : PRI_INFO, "Bridge pile operation step complete",
            {"marker", "BSCBR17"},
            {"kind", TOperation::EKind_Name(Kind)},
            {"dryRun", static_cast<bool>(Response)},
            {"pileId", PileId},
            {"operationId", OperationId},
            {"numGroups", NumGroups},
            {"error", Error.value_or("")});

        if (Response) {
            auto& record = Response->Get<TEvBlobStorage::TEvControllerDistconfResponse>()->Record;
            if (Error) {
                record.SetStatus(NKikimrBlobStorage::TEvControllerDistconfResponse::Error);
                record.SetErrorReason(*Error);
            }
            TActivationContext::Send(Response.release());
            return;
        }

        if (!Error) {
            OnCommitted();
        }
        Self->OnBridgePileOperationStepDone(PileId, Kind, OperationId, std::move(Error));
    }

protected:
    // make the changes in the state; returns the number of bridged groups changed, throws TExError on failure
    virtual size_t DoStep(TConfigState& state) = 0;

    // called when the changes are committed (and not when they are validated only)
    virtual void OnCommitted() {}
};

class TBlobStorageController::TTxAddBridgePile : public TTxBridgePileOperation {
    const ui64 BecameUnsyncedGeneration;
    std::vector<std::tuple<TGroupId, TGroupId>> Added;

public:
    TTxAddBridgePile(TBlobStorageController *controller, TBridgePileId pileId, ui64 operationId,
            ui64 becameUnsyncedGeneration)
        : TTxBridgePileOperation(controller, pileId, TOperation::ADD, operationId, std::nullopt, nullptr)
        , BecameUnsyncedGeneration(becameUnsyncedGeneration)
    {}

    // validate addition of the pile against the proposed storage config, which has static VDisks in the pile
    TTxAddBridgePile(TBlobStorageController *controller, TBridgePileId pileId,
            NKikimrBlobStorage::TStorageConfig&& proposedStorageConfig, std::unique_ptr<IEventHandle> response)
        : TTxBridgePileOperation(controller, pileId, TOperation::ADD, 0, std::move(proposedStorageConfig),
            std::move(response))
        , BecameUnsyncedGeneration(0)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_ADD_BRIDGE_PILE; }

private:
    size_t DoStep(TConfigState& state) override {
        Self->AddBridgePileGroups(state, PileId, BecameUnsyncedGeneration, &Added);
        return Added.size();
    }

    void OnCommitted() override {
        // groups created from now on get their parts in the pile right away
        Self->BridgePileAddedOperationId = OperationId;

        // new parts are unsynced from the very beginning, they get synced when the pile rejoins
        for (const auto& [bridgeProxyGroupId, targetGroupId] : Added) {
            const auto [it, inserted] = Self->TargetGroupToSyncerState.try_emplace(targetGroupId, bridgeProxyGroupId,
                targetGroupId);
            if (inserted) {
                Self->EnqueueSyncer(it->second);
            }
        }
        Self->ProcessSyncers();
    }
};

class TBlobStorageController::TTxRemoveBridgePile : public TTxBridgePileOperation {
public:
    TTxRemoveBridgePile(TBlobStorageController *controller, TBridgePileId pileId, ui64 operationId,
            std::unique_ptr<IEventHandle> response = nullptr)
        : TTxBridgePileOperation(controller, pileId, TOperation::REMOVE, operationId, std::nullopt, std::move(response))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_REMOVE_BRIDGE_PILE; }

private:
    size_t DoStep(TConfigState& state) override {
        return Self->RemoveBridgePileGroups(state, PileId);
    }
};

class TBlobStorageController::TTxBridgeRollback : public TTxBridgePileOperation {
public:
    TTxBridgeRollback(TBlobStorageController *controller, TBridgePileId pileId, ui64 operationId,
            std::unique_ptr<IEventHandle> response = nullptr)
        : TTxBridgePileOperation(controller, pileId, TOperation::ROLLBACK, operationId, std::nullopt, std::move(response))
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_BRIDGE_ROLLBACK; }

private:
    size_t DoStep(TConfigState& state) override {
        return Self->RollbackBridgedGroups(state, PileId);
    }
};

ITransaction* TBlobStorageController::CreateTxValidateBridgeRollback(TBridgePileId pileId,
        std::unique_ptr<IEventHandle> response) {
    return new TTxBridgeRollback(this, pileId, 0, std::move(response));
}

size_t TBlobStorageController::RollbackBridgedGroups(TConfigState& state, TBridgePileId pileId) {
    if (!pileId || !BridgeInfo || std::ranges::count_if(BridgeInfo->Piles,
            [](const auto& pile) { return !pile.IsFreeSlot; }) != 1) {
        throw TExError() << "the cluster has to have the only pile to be rolled back";
    }

    // groups of the ordinary cluster follow the settings of their pools only, and Console keeps the base settings of the
    // pools it defines, so the groups allocated with other settings for the pile can't stay as they are
    const auto *bridgePile = BridgeInfo->FindPile(pileId);
    if (!bridgePile) {
        throw TExError() << "the pile is not known";
    } else if (const auto *rules = GetPileOverrideRules();
            (StorageConfig && HasStoragePoolPileOverrides(StorageConfig->GetBlobStorageConfig(), bridgePile->Name)) ||
            (rules && HasStoragePoolPileOverrides(*rules, bridgePile->Name))) { // neither set by config, nor followed
        throw TExError() << "storage pool pile overrides for the pile '" << bridgePile->Name << "' have to be removed"
            " first";
    }

    // the pools keep their settings
    for (auto& [id, pool] : state.StoragePools.Unshare()) {
        if (pool.BridgeMode) {
            pool.BridgeMode = false;
            pool.Generation = pool.Generation.GetOrElse(0) + 1;
        }
    }

    // every bridged group has to be the inline pile in the pile, holding all the data of the group (the same rules as
    // distconf has for static groups, see MakeStaticGroupsOrdinary)
    TStringBuilder unsupported;
    std::vector<TGroupId> groupIds;
    state.Groups.ForEach([&](TGroupId groupId, const TGroupInfo& group) {
        if (!group.BridgeGroupInfo && !group.BridgePileId && !group.BridgeProxyGroupId) {
            return; // ordinary group, it stays as it is
        }
        const NKikimrBridge::TGroupState::TPile *pile = group.BridgeGroupInfo
            ? NBridge::GetGroupPile(group.BridgeGroupInfo->GetBridgeGroupState(), pileId)
            : nullptr;
        std::optional<TString> reason;
        if (!group.IsInlinePile() || group.BridgePileId != pileId) {
            reason = "it is not the inline pile in the original pile";
        } else if (group.VDisksInGroup.empty()) {
            reason = "it has no VDisks";
        } else if (group.DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::NONE) {
            reason = "it is being decommitted";
        } else if (!pile || pile->GetStage() != NKikimrBridge::TGroupState::SYNCED) {
            reason = "its part is not synced";
        } else if (const auto authority = NBridge::GetAuthorityPileId(group.BridgeGroupInfo->GetBridgeGroupState());
                authority && authority != pileId) {
            reason = "it relies on another pile";
        } else {
            size_t numParts = 0;
            NBridge::ForEachGroupPile(group.BridgeGroupInfo->GetBridgeGroupState(), [&](TBridgePileId,
                    const NKikimrBridge::TGroupState::TPile&) {
                ++numParts;
            });
            if (numParts != 1) {
                reason = "it has parts in other piles";
            }
        }
        if (reason) {
            unsupported << (unsupported.empty() ? "" : "; ") << "GroupId# " << groupId << ": " << *reason;
        } else {
            groupIds.push_back(groupId);
        }
    });
    if (!unsupported.empty()) {
        throw TExError() << "groups can't be made ordinary ones: " << unsupported;
    }
    for (const TGroupId groupId : groupIds) {
        TGroupInfo *group = state.Groups.FindForUpdate(groupId);
        Y_ABORT_UNLESS(group);
        // the group keeps its id, VDisks and data; its generation is bumped along with the content change, and the
        // syncer bookkeeping goes away with its pile (see CommitSyncerUpdates)
        group->BridgeGroupInfo.reset();
        group->BridgePileId = {};
        group->BridgeProxyGroupId.reset();
        state.GroupContentChanged.insert(groupId);
    }
    state.CheckConsistency();
    return groupIds.size();
}

ITransaction* TBlobStorageController::CreateTxValidateAddBridgePile(TBridgePileId pileId,
        NKikimrBlobStorage::TStorageConfig&& storageConfig, std::unique_ptr<IEventHandle> response) {
    return new TTxAddBridgePile(this, pileId, std::move(storageConfig), std::move(response));
}

ITransaction* TBlobStorageController::CreateTxValidateRemoveBridgePile(TBridgePileId pileId,
        std::unique_ptr<IEventHandle> response) {
    return new TTxRemoveBridgePile(this, pileId, 0, std::move(response));
}

void TBlobStorageController::DropBridgeGroupPart(TConfigState& state, TGroupId proxyGroupId, TBridgePileId pileId) {
    TGroupInfo *proxyGroup = state.Groups.FindForUpdate(proxyGroupId);
    Y_ABORT_UNLESS(proxyGroup && proxyGroup->BridgeGroupInfo);
    auto *groupState = proxyGroup->BridgeGroupInfo->MutableBridgeGroupState();
    const auto *pile = NBridge::GetGroupPile(*groupState, pileId);
    Y_ABORT_UNLESS(pile);
    const auto pileGroupId = TGroupId::FromProto(pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
    NBridge::RemoveGroupPile(groupState, pileId);
    if (pileGroupId == proxyGroupId) {
        // inline pile: the group keeps its id serving as bridge proxy group only, its VDisks go away
        for (const TVSlotInfo *vslot : proxyGroup->VDisksInGroup) {
            state.DestroyVSlot(vslot->VSlotId);
        }
        proxyGroup->VDisksInGroup.clear();
        proxyGroup->Topology = std::make_shared<TBlobStorageGroupInfo::TTopology>(proxyGroup->Topology->GType, 0, 0, 0);
        proxyGroup->BridgePileId = TBridgePileId();
        proxyGroup->BridgeProxyGroupId.reset();
    } else if (const TGroupInfo *pileGroup = state.Groups.Find(pileGroupId)) {
        // group entries are held by pointers, so deleting other groups keeps `proxyGroup` valid
        state.DeleteExistingGroupWithVSlots(*pileGroup);
    } else {
        Y_DEBUG_ABORT_S("pile group not found GroupId# " << proxyGroupId << " PileGroupId# " << pileGroupId);
    }
    state.GroupContentChanged.insert(proxyGroupId);
}

size_t TBlobStorageController::RemoveBridgePileGroups(TConfigState& state, TBridgePileId pileId) {
    const auto& bridgeInfo = state.BridgeInfo;
    Y_ABORT_UNLESS(bridgeInfo);

    // VDisks in the pile go away along with their data, so they have to belong to the parts of bridged groups
    state.VSlots.ForEach([&](const TVSlotId& vslotId, const TVSlotInfo& vslot) {
        if (vslot.IsBeingDeleted()) {
            return;
        } else if (const auto *pile = bridgeInfo->GetPileForNode(vslotId.NodeId); !pile || pile->BridgePileId != pileId) {
            return;
        } else if (const TGroupInfo *group = state.Groups.Find(vslot.GroupId); !group || group->BridgePileId != pileId) {
            throw TExError() << "GroupId# " << vslot.GroupId << " has VDisks in the pile, but it is not a part of bridged"
                " group, its data would be lost";
        }
    });

    std::vector<TGroupId> proxyGroupIds;
    state.Groups.ForEach([&](TGroupId groupId, const TGroupInfo& group) {
        if (group.BridgeGroupInfo && NBridge::GetGroupPile(group.BridgeGroupInfo->GetBridgeGroupState(), pileId)) {
            if (NBridge::GetAuthorityPileId(group.BridgeGroupInfo->GetBridgeGroupState()) == pileId) {
                throw TExError() << "GroupId# " << groupId << " relies on its part in the pile solely, its data would"
                    " be lost";
            }
            proxyGroupIds.push_back(groupId);
        }
    });

    for (const TGroupId proxyGroupId : proxyGroupIds) {
        DropBridgeGroupPart(state, proxyGroupId, pileId);
    }

    // nodes of the pile can't connect until it is added again, so nobody can tell how their drives are; INACTIVE set by
    // CMS sentinel when they went away would keep the static groups from getting their parts in the pile being added
    std::vector<TPDiskId> inactivePDiskIds;
    state.PDisks.ForEach([&](const TPDiskId& pdiskId, const TPDiskInfo& pdisk) {
        const auto *pile = bridgeInfo->GetPileForNode(pdiskId.NodeId);
        if (pile && pile->BridgePileId == pileId && pdisk.Status == NKikimrBlobStorage::EDriveStatus::INACTIVE) {
            inactivePDiskIds.push_back(pdiskId);
        }
    });
    for (const TPDiskId& pdiskId : inactivePDiskIds) {
        TPDiskInfo *pdisk = state.PDisks.FindForUpdate(pdiskId);
        pdisk->Status = NKikimrBlobStorage::EDriveStatus::ACTIVE;
        pdisk->StatusTimestamp = state.Timestamp;
        YDB_LOG_INFO("Drive of the removed pile gets ACTIVE status back",
            {"marker", "BSCBR19"},
            {"pileId", pileId},
            {"PDiskId", pdiskId});
    }

    state.CheckConsistency();
    return proxyGroupIds.size();
}

const NKikimrBridge::TClusterStateDetails::TPileSyncState *TBlobStorageController::FindPileSyncState(
        TBridgePileId pileId) const {
    if (StorageConfig) {
        for (const auto& pss : StorageConfig->GetClusterStateDetails().GetPileSyncState()) {
            if (TBridgePileId::FromProto(&pss, &NKikimrBridge::TClusterStateDetails::TPileSyncState::GetBridgePileId) ==
                    pileId) {
                return &pss;
            }
        }
    }
    return nullptr;
}

TBridgePileId TBlobStorageController::GetPendingOperationPileId() const {
    return StorageConfig ? NBridge::GetPendingPileOperationPileId(StorageConfig->GetClusterStateDetails()) : TBridgePileId();
}

bool TBlobStorageController::IsBridgePileBeingRemoved(TBridgePileId pileId) const {
    return pileId && GetPendingOperationPileId() == pileId && StorageConfig->GetClusterStateDetails()
        .GetPendingPileOperation().GetKind() == NKikimrBridge::TClusterStateDetails::TPendingPileOperation::REMOVE;
}

bool TBlobStorageController::IsBeingRolledBack() const {
    return StorageConfig && NBridge::IsRollbackPending(StorageConfig->GetClusterStateDetails());
}

bool TBlobStorageController::IsBridgePileAwaitingParts(TBridgePileId pileId) const {
    if (!pileId || GetPendingOperationPileId() != pileId) {
        return false;
    }
    const auto& op = StorageConfig->GetClusterStateDetails().GetPendingPileOperation();
    return op.GetKind() == NKikimrBridge::TClusterStateDetails::TPendingPileOperation::ADD &&
        op.GetOperationId() != BridgePileAddedOperationId;
}

void TBlobStorageController::CheckBridgePileOperation() {
    const TBridgePileId pileId = GetPendingOperationPileId();
    if (!Loaded || !pileId || !StartRetriableStep(BridgePileOperationStep)) {
        return;
    }

    const auto& details = StorageConfig->GetClusterStateDetails();
    const ui64 operationId = details.GetPendingPileOperation().GetOperationId();
    using TOperation = NKikimrBridge::TClusterStateDetails::TPendingPileOperation;
    switch (details.GetPendingPileOperation().GetKind()) {
        case TOperation::ADD: {
            ui64 becameUnsyncedGeneration = 0;
            for (const auto& pss : details.GetPileSyncState()) {
                if (TBridgePileId::FromProto(&pss, &NKikimrBridge::TClusterStateDetails::TPileSyncState::GetBridgePileId) ==
                        pileId) {
                    becameUnsyncedGeneration = pss.GetBecameUnsyncedGeneration();
                }
            }
            Y_DEBUG_ABORT_UNLESS(becameUnsyncedGeneration); // the pile being added is unsynced
            Execute(std::make_unique<TTxAddBridgePile>(this, pileId, operationId, becameUnsyncedGeneration));
            break;
        }

        case TOperation::REMOVE:
            Execute(std::make_unique<TTxRemoveBridgePile>(this, pileId, operationId));
            break;

        case TOperation::ROLLBACK:
            Execute(std::make_unique<TTxBridgeRollback>(this, pileId, operationId));
            break;

        case NKikimrBridge::TClusterStateDetails_TPendingPileOperation_EKind_TClusterStateDetails_TPendingPileOperation_EKind_INT_MIN_SENTINEL_DO_NOT_USE_:
        case NKikimrBridge::TClusterStateDetails_TPendingPileOperation_EKind_TClusterStateDetails_TPendingPileOperation_EKind_INT_MAX_SENTINEL_DO_NOT_USE_:
            Y_ABORT();
    }
}

void TBlobStorageController::OnBridgePileOperationStepDone(TBridgePileId pileId,
        NKikimrBridge::TClusterStateDetails::TPendingPileOperation::EKind kind, ui64 operationId,
        std::optional<TString> error) {
    // report the result to distconf: success finishes the operation, failure gets recorded (unless it is the same one)
    // and the operation is retried
    NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot request;
    auto *cmd = request.MutableNotifyBridgePileOperation();
    pileId.CopyToProto(cmd, &NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot::TNotifyBridgePileOperation::SetBridgePileId);
    cmd->SetKind(kind);
    cmd->SetOperationId(operationId);
    if (error) {
        cmd->SetErrorReason(*error);
    }
    InvokeOnRoot(std::move(request), [this, failed = error.has_value()](NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& result) {
        const bool notified = result.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK;
        if (!notified) {
            YDB_LOG_WARN("Failed to notify distconf about bridge pile operation",
                {"marker", "BSCBR18"},
                {"result", result});
        }
        if (FinishRetriableStep(BridgePileOperationStep, failed || !notified)) {
            CheckBridgePileOperation();
        }
    });
}

void TBlobStorageController::HandleRetryBridgePileOperation() {
    OnRetriableStepRetry(BridgePileOperationStep);
    CheckBridgePileOperation();
}

class TBlobStorageController::TTxUpdateBridgeGroupModes : public TTransactionBase<TBlobStorageController> {
    std::optional<TConfigState> State;
    bool Failed = false;
    bool RecheckLater = false; // some parts wait to be deleted
    std::vector<TGroupId> StoppedParts; // parts that have stopped getting requests now
    std::vector<TGroupId> NoticedParts; // parts that have been noticed not getting requests
    std::vector<TGroupId> DroppedParts;

public:
    TTxUpdateBridgeGroupModes(TBlobStorageController *controller)
        : TTransactionBase(controller)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_BRIDGE_GROUP_MODES; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        State.emplace(*Self, Self->HostRecords, TActivationContext::Now(), TActivationContext::Monotonic());
        const TMonotonic now = TActivationContext::Monotonic();

        std::vector<TGroupId> groupIds;
        State->Groups.ForEach([&](TGroupId groupId, const TGroupInfo& group) {
            if (group.BridgeGroupInfo && group.BridgeGroupInfo->GetBridgeGroupState().GetAuthorityPileId()) {
                groupIds.push_back(groupId);
            }
        });

        for (const TGroupId groupId : groupIds) {
            const TGroupInfo *group = State->Groups.Find(groupId);
            const auto& storagePools = State->StoragePools.Get();
            const auto it = storagePools.find(group->StoragePoolId);
            if (it == storagePools.end()) {
                Y_DEBUG_ABORT();
                continue;
            } else if (it->second.BridgeMode) {
                UpdateBridgedGroup(*group);
            } else {
                UpdateOrdinaryGroup(*group, now);
            }
        }

        if (auto error = Self->ValidateAndCommitConfigUpdate(State, TConfigTxFlags::SuppressAll(), txc)) {
            YDB_LOG_ERROR("Failed to update bridged groups by the modes of their pools",
                {"marker", "BSCBR20"},
                {"error", *error});
            Failed = true;
            StoppedParts.clear();
            NoticedParts.clear();
            DroppedParts.clear();
        }

        return true;
    }

    void Complete(const TActorContext&) override {
        if (State) {
            State->ApplyConfigUpdates();
        }
        const TMonotonic now = TActivationContext::Monotonic();
        for (const TGroupId groupId : StoppedParts) {
            Self->DroppedBridgePartsSince.insert_or_assign(groupId, now);
        }
        for (const TGroupId groupId : NoticedParts) {
            Self->DroppedBridgePartsSince.try_emplace(groupId, now);
        }
        for (const TGroupId groupId : DroppedParts) {
            Self->DroppedBridgePartsSince.erase(groupId);
        }
        Self->OnBridgeGroupModesStepDone(Failed, RecheckLater);
    }

private:
    // the group of bridged pool stops relying on the single part when it has its synced part in every pile, and every
    // such pile is synchronized
    void UpdateBridgedGroup(const TGroupInfo& group) {
        const auto& state = group.BridgeGroupInfo->GetBridgeGroupState();
        for (const auto& pile : State->BridgeInfo->Piles) {
            if (pile.State == NKikimrBridge::TClusterState::INACTIVE) {
                continue;
            } else if (const auto *part = NBridge::GetGroupPile(state, pile.BridgePileId); !part ||
                    part->GetStage() != NKikimrBridge::TGroupState::SYNCED ||
                    pile.State != NKikimrBridge::TClusterState::SYNCHRONIZED) {
                return;
            }
        }
        State->Groups.FindForUpdate(group.ID)->BridgeGroupInfo->MutableBridgeGroupState()->ClearAuthorityPileId();
        State->GroupContentChanged.insert(group.ID);
    }

    // parts of the group of ordinary pool, but the authority one, stop getting requests first (but blocks), then they
    // are deleted after a while; the group doesn't need to designate the part it relies on when it is the only one
    void UpdateOrdinaryGroup(const TGroupInfo& group, TMonotonic now) {
        const auto& state = group.BridgeGroupInfo->GetBridgeGroupState();
        const auto authorityPileId = TBridgePileId::FromProto(&state, &NKikimrBridge::TGroupState::GetAuthorityPileId);
        if (!NBridge::GetGroupPile(state, authorityPileId)) {
            Y_DEBUG_ABORT_S("authority part not found GroupId# " << group.ID);
            return; // nothing is deleted without the part to keep
        }
        std::vector<TBridgePileId> stop, drop;
        NBridge::ForEachGroupPile(state, [&](TBridgePileId pileId, const NKikimrBridge::TGroupState::TPile& part) {
            if (pileId == authorityPileId) {
                return;
            }
            const auto partGroupId = TGroupId::FromProto(&part, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            if (part.GetStage() != NKikimrBridge::TGroupState::BLOCKS) {
                stop.push_back(pileId);
                StoppedParts.push_back(partGroupId);
                RecheckLater = true;
            } else if (const auto it = Self->DroppedBridgePartsSince.find(partGroupId);
                    it == Self->DroppedBridgePartsSince.end()) {
                NoticedParts.push_back(partGroupId); // BSC has been restarted, or the part has been just created
                RecheckLater = true;
            } else if (now < it->second + BridgePartDropDelay) {
                RecheckLater = true;
            } else {
                drop.push_back(pileId);
                DroppedParts.push_back(partGroupId);
            }
        });

        if (!stop.empty()) {
            auto *mutableState = State->Groups.FindForUpdate(group.ID)->BridgeGroupInfo->MutableBridgeGroupState();
            for (const TBridgePileId pileId : stop) {
                NBridge::GetGroupPile(mutableState, pileId)->ClearStage(); // only blocks are sent to the part then
            }
            State->GroupContentChanged.insert(group.ID);
        }
        for (const TBridgePileId pileId : drop) {
            Self->DropBridgeGroupPart(*State, group.ID, pileId);
        }

        size_t numParts = 0;
        NBridge::ForEachGroupPile(State->Groups.Find(group.ID)->BridgeGroupInfo->GetBridgeGroupState(),
            [&](TBridgePileId, const NKikimrBridge::TGroupState::TPile&) { ++numParts; });
        if (numParts == 1) {
            State->Groups.FindForUpdate(group.ID)->BridgeGroupInfo->MutableBridgeGroupState()->ClearAuthorityPileId();
            State->GroupContentChanged.insert(group.ID);
        }
    }
};

void TBlobStorageController::CheckBridgeGroupModes() {
    if (Loaded && BridgeInfo && !BridgeInfo->IsImplicit && StartRetriableStep(BridgeGroupModesStep)) {
        Execute(std::make_unique<TTxUpdateBridgeGroupModes>(this));
    }
}

void TBlobStorageController::OnBridgeGroupModesStepDone(bool failed, bool recheckLater) {
    if (FinishRetriableStep(BridgeGroupModesStep, failed)) {
        CheckBridgeGroupModes();
    } else if (recheckLater && !failed && !BridgeGroupModesCheckScheduled) {
        BridgeGroupModesCheckScheduled = true;
        TActivationContext::Schedule(BridgePartDropDelay / 3, new IEventHandle(TEvPrivate::EvCheckBridgeGroupModes, 0,
            SelfId(), {}, nullptr, 0));
    }
}

void TBlobStorageController::HandleRetryBridgeGroupModes() {
    OnRetriableStepRetry(BridgeGroupModesStep);
    CheckBridgeGroupModes();
}

void TBlobStorageController::HandleCheckBridgeGroupModes() {
    Y_ABORT_UNLESS(BridgeGroupModesCheckScheduled);
    BridgeGroupModesCheckScheduled = false;
    CheckBridgeGroupModes();
}

void TBlobStorageController::CheckUnsyncedBridgePiles() {
    if (!StorageConfig->HasClusterStateDetails() || CheckingUnsyncedBridgePiles) {
        return; // no bridge mode cluster state details available
    }
    const auto& details = StorageConfig->GetClusterStateDetails();
    for (const auto& pss : details.GetPileSyncState()) {
        if (pss.GetUnsyncedBSC()) {
            CheckingUnsyncedBridgePiles = true;
            RecheckUnsyncedBridgePiles = false;
            Execute(std::make_unique<TTxCheckUnsynced>(this, *StorageConfig));
            break;
        }
    }
}

void TBlobStorageController::UpdateBridgeSyncCounters() {
    if (!BridgeInfo || BridgeInfo->IsImplicit) {
        return;
    }

    std::map<TString, TBridgeSyncCounters::TPileValues> piles;
    BridgeInfo->ForEachPile([&](TBridgePileId pileId) {
        if (const auto *pile = BridgeInfo->GetPile(pileId); !pile->IsFreeSlot) {
            const auto *pss = FindPileSyncState(pileId);
            piles[pile->Name] = {.State = pile->State, .UnsyncedBSC = pss && pss->GetUnsyncedBSC()};
        }
    });

    std::map<TBridgeSyncCounters::TKey, TBridgeSyncCounters::TValues> values;
    auto scan = [&](const NKikimrBridge::TGroupState& state, const TString& pool) {
        NBridge::ForEachGroupPile(state, [&](TBridgePileId pileId, const NKikimrBridge::TGroupState::TPile& pile) {
            const auto *bridgePile = BridgeInfo->FindPile(pileId);
            if (!bridgePile) {
                return;
            }
            auto& value = values[{bridgePile->Name, pool}];
            if (static_cast<size_t>(pile.GetStage()) < value.GroupsInStage.size()) {
                ++value.GroupsInStage[pile.GetStage()];
            }
            const auto targetGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
            const auto it = TargetGroupToSyncerState.find(targetGroupId);
            if (it != TargetGroupToSyncerState.end()) {
                value.BytesPerSecond += it->second.CopyRate;
            }
            if (it != TargetGroupToSyncerState.end() && it->second.NodeIds) {
                ++value.SyncersRunning;
                for (const auto& [nodeId, perNodeInfo] : it->second.NodeIds) {
                    value.BytesDone += perNodeInfo.Progress.BytesDone;
                    value.BytesTotal += perNodeInfo.Progress.BytesTotal;
                }
            }
            if (pile.GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                const auto remaining = GetBridgeSyncRemaining(targetGroupId, pile.GetStage());
                value.BytesRemaining += remaining ? remaining->Bytes : 0;
                value.GroupsNotEstimated += !remaining || !remaining->Complete;
            }
        });
    };
    for (const auto& [groupId, group] : GroupMap) {
        if (group->BridgeGroupInfo && group->BridgeGroupInfo->HasBridgeGroupState()) {
            scan(group->BridgeGroupInfo->GetBridgeGroupState(), StoragePools.at(group->StoragePoolId).Name);
        }
    }
    for (const auto& [groupId, group] : StaticGroups) {
        if (group.Info && group.Info->Group && group.Info->Group->HasBridgeGroupState()) {
            scan(group.Info->Group->GetBridgeGroupState(), group.Info->GetStoragePoolName());
        }
    }

    BridgeSyncCounters->Update(piles, values);
}

std::optional<TBlobStorageController::TBridgeSyncRemaining> TBlobStorageController::GetBridgeSyncRemaining(
        TGroupId targetGroupId, NKikimrBridge::TGroupState::EStage stage) const {
    if (stage == NKikimrBridge::TGroupState::SYNCED) {
        return TBridgeSyncRemaining{.Bytes = 0, .Complete = true};
    }

    const TSyncerProgress *progress = nullptr;
    std::optional<ui64> base; // what was left after a scan over all the blobs, before the copying of the current syncer
    if (const auto it = TargetGroupToSyncerState.find(targetGroupId); it != TargetGroupToSyncerState.end() &&
            it->second.CopyStage == stage) {
        progress = it->second.LastProgress ? &*it->second.LastProgress : nullptr;
        base = it->second.KnownRemaining;
    }
    auto left = [](ui64 x, ui64 y) { return x > y ? x - y : 0; };
    if (progress && progress->ScanFinished) {
        return TBridgeSyncRemaining{.Bytes = left(progress->BytesToCopy, progress->BytesCopied), .Complete = true};
    }
    if (!base) { // no scan of this stage has finished yet, the one of the previous stage is the best guess
        if (const auto it = BridgeSyncState.find(targetGroupId);
                it != BridgeSyncState.end() && it->second.Stage == stage) {
            base = it->second.EstimatedBytesToCopy;
        }
    }

    // the scan of the current syncer is under way: what it has found so far is a lower bound
    const ui64 copied = progress ? progress->BytesCopied : 0;
    const ui64 partial = progress ? left(progress->BytesToCopy, copied) : 0;
    if (base) {
        return TBridgeSyncRemaining{.Bytes = Max(left(*base, copied), partial), .Complete = true};
    } else if (progress) {
        return TBridgeSyncRemaining{.Bytes = partial, .Complete = false};
    } else {
        return std::nullopt;
    }
}

std::optional<NKikimrBridge::TGroupState::EStage> TBlobStorageController::GetBridgeSyncStage(TGroupId targetGroupId) {
    if (const TGroupInfo *group = FindGroup(targetGroupId); group && group->BridgeProxyGroupId && group->BridgePileId) {
        if (const TGroupInfo *proxyGroup = FindGroup(*group->BridgeProxyGroupId);
                proxyGroup && proxyGroup->BridgeGroupInfo) {
            const auto& bridgeGroupState = proxyGroup->BridgeGroupInfo->GetBridgeGroupState();
            if (const auto *pile = NBridge::GetGroupPile(bridgeGroupState, group->BridgePileId)) {
                return pile->GetStage();
            }
        }
    } else if (const auto it = StaticGroups.find(targetGroupId); it != StaticGroups.end() &&
            it->second.Info &&
            it->second.Info->Group &&
            it->second.Info->Group->HasBridgeProxyGroupId() &&
            it->second.Info->Group->HasBridgePileId()) {
        if (const auto proxyIt = StaticGroups.find(TGroupId::FromProto(&it->second.Info->Group.value(),
                    &NKikimrBlobStorage::TGroupInfo::GetBridgeProxyGroupId));
                proxyIt != StaticGroups.end() &&
                proxyIt->second.Info &&
                proxyIt->second.Info->Group &&
                proxyIt->second.Info->Group->HasBridgeGroupState()) {
            const auto& bridgeGroupState = proxyIt->second.Info->Group->GetBridgeGroupState();
            const auto bridgePileId = TBridgePileId::FromProto(&it->second.Info->Group.value(),
                &NKikimrBlobStorage::TGroupInfo::GetBridgePileId);
            if (const auto *pile = NBridge::GetGroupPile(bridgeGroupState, bridgePileId)) {
                return pile->GetStage();
            }
        }
    }
    return std::nullopt;
}

ui64 TBlobStorageController::UpdateBridgeSyncCopyProgress(TSyncerState& syncerState,
        NKikimrBridge::TGroupState::EStage stage, const TSyncerProgress& progress) {
    if (syncerState.CopyStage != stage) {
        syncerState.CopyStage = stage;
        syncerState.LastProgress.reset();
        syncerState.KnownRemaining.reset();
    }
    const ui64 prevCopied = syncerState.LastProgress ? syncerState.LastProgress->BytesCopied : 0;
    // a syncer started anew reports from zero
    const ui64 copied = prevCopied <= progress.BytesCopied ? progress.BytesCopied - prevCopied : progress.BytesCopied;
    syncerState.CopiedTotal += copied;
    syncerState.LastProgress = progress;
    if (progress.ScanFinished) {
        syncerState.KnownRemaining = progress.BytesToCopy > progress.BytesCopied
            ? progress.BytesToCopy - progress.BytesCopied
            : 0;
    }
    return copied;
}

void TBlobStorageController::UpdateBridgeSyncCopyRates() {
    const TMonotonic now = TActivationContext::Monotonic();
    for (auto& [targetGroupId, syncerState] : TargetGroupToSyncerState) {
        auto& samples = syncerState.CopiedSamples;
        samples.emplace_back(now, syncerState.CopiedTotal);
        // the first sample stays at the beginning of the window or before it, so that the rate covers all of it
        while (samples.size() > 1 && std::get<0>(samples[1]) <= now - BridgeSyncRateWindow) {
            samples.pop_front();
        }
        const auto& [since, copiedSince] = samples.front();
        const ui64 rate = since < now
            ? static_cast<ui64>((syncerState.CopiedTotal - copiedSince) / (now - since).SecondsFloat())
            : 0;
        if (rate != syncerState.CopyRate) {
            syncerState.CopyRate = rate;
            SysViewChangedGroups.insert(targetGroupId);
        }
    }
}

void TBlobStorageController::ApplySyncerState(TNodeId nodeId, const NKikimrBlobStorage::TEvControllerUpdateSyncerState& update,
        TSet<TGroupId>& groupIdsToRead, bool comprehensive) {
    YDB_LOG_DEBUG("ApplySyncerState",
        {"marker", "BSCBR00"},
        {"nodeId", nodeId},
        {"update", update},
        {"comprehensive", comprehensive});

    // make set of existing target-source tuples to drop unused ones
    THashSet<TSyncerState*> unlistedSyncerState;
    if (comprehensive) {
        for (auto it = NodeToSyncerState.lower_bound({nodeId, nullptr}); it != NodeToSyncerState.end() &&
                std::get<0>(*it) == nodeId; ++it) {
            unlistedSyncerState.insert(std::get<1>(*it));
        }
    }

    bool updateNodeWarden = false; // should we send notification in response
    std::vector<TTxUpdateBridgeSyncState::TGroupStateUpdate> updates;

    // scan update
    for (const auto& syncer : update.GetSyncers()) {
        const auto groupId = TGroupId::FromProto(&syncer, &std::decay_t<decltype(syncer)>::GetBridgeProxyGroupId);
        const ui32 generation = syncer.GetBridgeProxyGroupGeneration();
        const auto sourceGroupId = TGroupId::FromProto(&syncer, &std::decay_t<decltype(syncer)>::GetSourceGroupId);
        const auto targetGroupId = TGroupId::FromProto(&syncer, &std::decay_t<decltype(syncer)>::GetTargetGroupId);

        // find this syncer in our map; if we don't have one, we don't need to do anything about it
        const auto it = TargetGroupToSyncerState.find(targetGroupId);
        if (it == TargetGroupToSyncerState.end()) {
            updateNodeWarden = true; // node warden has reported excessive entry, we should send an update
            continue;
        }
        TSyncerState& syncerState = it->second;

        bool updateGroupInfo = false;
        NKikimrBlobStorage::TGroupInfo bridgeGroupInfo;
        bool correct = false;
        bool staticGroup = false;
        TString pool;
        if (const TGroupInfo *group = FindGroup(groupId)) {
            if (generation < group->Generation) {
                updateGroupInfo = true;
            } else if (group->Generation < generation) { // NodeWarden has newer state of group?
                Y_DEBUG_ABORT();
            } else if (group->BridgeGroupInfo && group->BridgeGroupInfo->HasBridgeGroupState()) {
                correct = true;
                bridgeGroupInfo.CopyFrom(*group->BridgeGroupInfo);
                pool = StoragePools.at(group->StoragePoolId).Name;
            } else { // not a bridged group
                Y_DEBUG_ABORT();
            }
        } else if (const auto it = StaticGroups.find(groupId); it != StaticGroups.end()) {
            const auto& info = it->second.Info;
            const auto& group = info->Group;
            if (generation != info->GroupGeneration) {
                // either NodeWarden, or BSC has obsolete static config generation; we can't accept this report, but
                // we have to handle it anyway
                // FIXME
                YDB_LOG_DEBUG("Incorrect static group generation reported",
                    {"marker", "BSCBR13"},
                    {"groupId", groupId},
                    {"reportedGeneration", generation},
                    {"knownGeneration", info->GroupGeneration},
                    {"item", syncer});
            } else if (group && group->HasBridgeGroupState()) {
                correct = true;
                staticGroup = true;
                bridgeGroupInfo.MutableBridgeGroupState()->CopyFrom(group->GetBridgeGroupState());
                pool = info->GetStoragePoolName();
            } else { // not a bridged static group
                Y_DEBUG_ABORT();
            }
        }
        if (updateGroupInfo) {
            // report fresh groups to the NodeWarden
            groupIdsToRead.insert({groupId, sourceGroupId, targetGroupId});
        }
        if (!correct) {
            updateNodeWarden = true;
            continue;
        }

        // a finished syncer not assigned to this node is a repeated report, e.g. after the node re-registers
        const bool assigned = syncerState.NodeIds.contains(nodeId);

        // see if we can drop this syncer in favor of already running one
        if (!assigned) {
            bool dropNewSyncer = false;
            for (auto it = syncerState.NodeIds.begin(); it != syncerState.NodeIds.end(); ) {
                auto& [existingNodeId, item] = *it;
                if (TNodeInfo *node = FindNode(existingNodeId); node && node->Registered) {
                    // this node already has a working syncer, delete incoming one
                    dropNewSyncer = true;
                    ++it;
                } else {
                    // we have to remove existing syncer; don't need to update it, 'cause node is not connected/working
                    syncerState.NodeIds.erase(it++);
                    NodeToSyncerState.erase({existingNodeId, &syncerState});
                    BridgeSyncNodeRatesDirty = true;
                }
            }
            if (dropNewSyncer) {
                updateNodeWarden = true;
                continue;
            }
        }

        unlistedSyncerState.erase(&syncerState);
        auto& perNodeInfo = syncerState.NodeIds[nodeId];
        perNodeInfo.SourceGroupId = sourceGroupId;
        if (NodeToSyncerState.emplace(nodeId, &syncerState).second) { // usually it's a report of a known syncer
            BridgeSyncNodeRatesDirty = true;
        }
        syncerState.Unlink();

        // the pile of the target, and its stage the syncer has been working on
        TBridgePileId targetPileId;
        const auto *targetPile = NBridge::FindGroupPile(bridgeGroupInfo.GetBridgeGroupState(), [&](TBridgePileId id,
                const NKikimrBridge::TGroupState::TPile& item) {
            targetPileId = id;
            return TGroupId::FromProto(&item, &NKikimrBridge::TGroupState::TPile::GetGroupId) == targetGroupId;
        });
        Y_DEBUG_ABORT_UNLESS(targetPile);

        // update any progress
        if (syncer.HasBytesDone() || syncer.HasBytesTotal() || syncer.HasBytesError() ||
                syncer.HasBlobsDone() || syncer.HasBlobsTotal() || syncer.HasBlobsError() ||
                syncer.HasBytesToCopy() || syncer.HasBytesCopied() || syncer.HasScanFinished()) {
            auto& progress = perNodeInfo.Progress;
#define FETCH_METRIC(NAME) \
            if (syncer.Has##NAME()) { \
                progress.NAME = syncer.Get##NAME(); \
            }

            FETCH_METRIC(BytesDone)
            FETCH_METRIC(BytesTotal)
            FETCH_METRIC(BytesError)
            FETCH_METRIC(BlobsDone)
            FETCH_METRIC(BlobsTotal)
            FETCH_METRIC(BlobsError)
            FETCH_METRIC(BytesToCopy)
            FETCH_METRIC(BytesCopied)
            FETCH_METRIC(ScanFinished)

#undef FETCH_METRIC

            if (targetPile) {
                const ui64 copied = UpdateBridgeSyncCopyProgress(syncerState, targetPile->GetStage(), progress);
                const auto *bridgePile = BridgeInfo ? BridgeInfo->FindPile(targetPileId) : nullptr;
                if (copied && bridgePile) {
                    BridgeSyncCounters->AddCopied({bridgePile->Name, pool}, copied);
                }
            }
            SysViewChangedGroups.insert(targetGroupId);
        }

        if (syncer.GetFinished()) {
            // syncer is finished, so we remove it from working set
            size_t numErased = NodeToSyncerState.erase({nodeId, &syncerState});
            BridgeSyncNodeRatesDirty = true;
            Y_ABORT_UNLESS(numErased == 1);
            numErased = syncerState.NodeIds.erase(nodeId);
            Y_ABORT_UNLESS(numErased == 1);
            EnqueueSyncer(syncerState);
            updateNodeWarden = true;

            if (syncer.HasErrorReason()) {
                syncerState.NotBefore = TActivationContext::Monotonic() + syncerState.RetryBackoff.Next();
                NKikimrBridge::TGroupState::EStage stage = NKikimrBridge::TGroupState::BLOCKS;
                TBridgePileId pileId;
                const auto *pile = NBridge::FindGroupPile(bridgeGroupInfo.GetBridgeGroupState(), [&](TBridgePileId id,
                        const NKikimrBridge::TGroupState::TPile& item) {
                    pileId = id;
                    return TGroupId::FromProto(&item, &NKikimrBridge::TGroupState::TPile::GetGroupId) == targetGroupId;
                });
                Y_DEBUG_ABORT_UNLESS(pile);
                if (pile) {
                    stage = pile->GetStage();
                    if (const auto *bridgePile = BridgeInfo ? BridgeInfo->FindPile(pileId) : nullptr; bridgePile && assigned) {
                        BridgeSyncCounters->IncErrors({bridgePile->Name, pool});
                    }
                }
                updates.emplace_back(targetGroupId, TTxUpdateBridgeSyncState::TRegisterError{
                    .Stage = stage,
                    .Error = syncer.GetErrorReason(),
                });
                continue;
            }
            if (syncerState.InCommit) {
                // this is just a repeat
                continue;
            }
            syncerState.RetryBackoff.Reset();
            syncerState.NotBefore = {};

            // let's update syncer state (switch it to next one)
            auto *state = bridgeGroupInfo.MutableBridgeGroupState();
            TTxUpdateBridgeSyncState::TChangeStage changeStage;
            if (auto *pile = NBridge::FindGroupPileByGroupId(state, targetGroupId)) {
                switch (const auto finishedStage = pile->GetStage()) {
                    case NKikimrBridge::TGroupState::BLOCKS:
                    case NKikimrBridge::TGroupState::WRITE_KEEP:
                    case NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP:
                    case NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA:
                        pile->SetStage(static_cast<NKikimrBridge::TGroupState::EStage>(finishedStage + 1));
                        changeStage.NewStage = pile->GetStage();
                        if (changeStage.NewStage != NKikimrBridge::TGroupState::SYNCED &&
                                syncerState.CopyStage == finishedStage) {
                            // the blob scan of the finished stage tells what the next one is going to copy
                            changeStage.EstimatedBytesToCopy = syncerState.KnownRemaining;
                        }
                        break;

                    case NKikimrBridge::TGroupState::SYNCED:
                        break; // already synced, syncer just did nothing

                    default:
                        Y_DEBUG_ABORT_S("incorrect pile Stage# " << NKikimrBridge::TGroupState::EStage_Name(pile->GetStage())
                            << " GroupId# " << targetGroupId);
                }
            } else {
                Y_DEBUG_ABORT("target group not found in group state");
            }

            syncerState.InCommit = true; // we are committing syncer state right now
            syncerState.Unlink();

            updates.emplace_back(targetGroupId, std::move(changeStage));

            if (staticGroup) {
                UpdateStaticGroupBridgeGroupInfo(groupId, generation, std::move(bridgeGroupInfo), targetGroupId);
            } else {
                Execute(std::make_unique<TTxUpdateBridgeGroupInfo>(this, groupId, generation,
                    std::move(bridgeGroupInfo), targetGroupId));
            }
        }
    }

    // delete missing syncers
    for (TSyncerState *syncerState : unlistedSyncerState) {
        size_t numErased = syncerState->NodeIds.erase(nodeId);
        Y_ABORT_UNLESS(numErased == 1);
        numErased = NodeToSyncerState.erase({nodeId, syncerState});
        BridgeSyncNodeRatesDirty = true;
        Y_ABORT_UNLESS(numErased == 1);
        EnqueueSyncer(*syncerState);
    }

    if (!updates.empty()) {
        Execute(std::make_unique<TTxUpdateBridgeSyncState>(this, std::move(updates), TActivationContext::Now()));
    }

    THashSet<TNodeId> nodesToUpdate;
    if (updateNodeWarden) {
        nodesToUpdate.insert(nodeId);
    }
    ProcessSyncers(std::move(nodesToUpdate));
}

void TBlobStorageController::UpdateStaticGroupBridgeGroupInfo(TGroupId groupId, ui32 generation,
        NKikimrBlobStorage::TGroupInfo bridgeGroupInfo, TGroupId targetGroupId) {
    auto& syncerState = TargetGroupToSyncerState.at(targetGroupId);
    Y_ABORT_UNLESS(syncerState.InCommit);
    const ui64 commitCookie = syncerState.StaticCommitCookie = ++NextStaticSyncerCommitCookie;
    NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot request;
    auto *cmd = request.MutableUpdateBridgeGroupInfo();
    groupId.CopyToProto(cmd, &std::decay_t<decltype(*cmd)>::SetGroupId);
    cmd->SetGroupGeneration(generation);
    cmd->MutableBridgeGroupInfo()->CopyFrom(bridgeGroupInfo);
    InvokeOnRoot(std::move(request), [this, targetGroupId, commitCookie](NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult& result) {
        const auto it = TargetGroupToSyncerState.find(targetGroupId);
        if (it == TargetGroupToSyncerState.end() || it->second.StaticCommitCookie != commitCookie) {
            return;
        }
        if (result.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
            YDB_LOG_ERROR("UpdateBridgeGroupInfo has unexpectedly failed",
                {"marker", "BSCBR08"},
                {"result", result});
            // Another pile may have advanced the group generation. Release this commit and let
            // the syncer retry using the current group configuration instead of resending stale state.
        }
        TSyncerState& syncerState = it->second;
        Y_ABORT_UNLESS(syncerState.InCommit);
        syncerState.InCommit = false;
        EnqueueSyncer(syncerState);
        if (result.GetStatus() != NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
            ProcessSyncers();
        }
    });
}

void TBlobStorageController::CheckSyncerDisconnectedNodes() {
    const auto now = TActivationContext::Monotonic();

    for (auto it = NodeToSyncerState.begin(); it != NodeToSyncerState.end(); ) {
        const auto& [nodeId, syncerState] = *it;
        const TNodeInfo *node = FindNode(nodeId);
        Y_ABORT_UNLESS(node);

        // find all items for this nodeId
        auto beginIt = it;
        while (it != NodeToSyncerState.end() && std::get<0>(*it) == nodeId) {
            ++it;
        }

        if (node->DisconnectedTimestampMono <= now - DisconnectedSyncerReactionTime) {
            for (auto jt = beginIt; jt != it; ++jt) {
                const auto& [nodeId, syncerState] = *jt;
                const size_t numErased = syncerState->NodeIds.erase(nodeId);
                Y_ABORT_UNLESS(numErased == 1);
                EnqueueSyncer(*syncerState);
            }
            NodeToSyncerState.erase(beginIt, it);
            BridgeSyncNodeRatesDirty = true;
        }
    }

    UpdateBridgeSyncCopyRates(); // reuse this 5 s timer of the syncer machinery
    UpdateBridgeSyncCounters();

    TActivationContext::Schedule(TDuration::Seconds(5), new IEventHandle(TEvPrivate::EvCheckSyncerDisconnectedNodes, 0,
        SelfId(), {}, nullptr, 0));
}

void TBlobStorageController::EnqueueSyncer(TSyncerState& syncerState) {
    if (syncerState.NodeIds || syncerState.InCommit) {
        return; // it is running or being committed, and gets here again when that's over
    }
    // ProcessSyncers stops at the first syncer that has to wait, and syncers of static groups don't wait
    if (StaticGroups.contains(syncerState.BridgeProxyGroupId)) {
        SyncersRequiringAction.PushFront(&syncerState);
    } else {
        SyncersRequiringAction.PushBack(&syncerState);
    }
}

void TBlobStorageController::HandleRetrySyncers() {
    SyncersRetryAt = TMonotonic::Max();
    ProcessSyncers();
}

void TBlobStorageController::ProcessSyncers(THashSet<TNodeId> nodesToUpdate) {
    const TMonotonic now = TActivationContext::Monotonic();
    if (now < LoadedAt + SyncerReportingTime) {
        // give nodes some time to connect to BSC and report their syncers: until then BSC doesn't know how many of them
        // are running, so it could exceed the limit
        SendSyncersToNodes(nodesToUpdate);
        return;
    }

    auto limitReached = [&] {
        return BridgeSyncMaxSyncers && BridgeSyncMaxSyncers <= NodeToSyncerState.size();
    };

    TMonotonic retryAt = TMonotonic::Max();
    // groups being made fully bridged ones take the slots left by the piles getting back, which regain redundancy
    for (const bool upgradePass : {false, true}) {
        for (auto it = SyncersRequiringAction.begin(); it != SyncersRequiringAction.end(); ) {
            TSyncerState& syncerState = *it++;

            if (limitReached() && !StaticGroups.contains(syncerState.BridgeProxyGroupId)) {
                break; // only syncers of static groups don't wait, and they are all ahead; this runs on every event
            }
            if (syncerState.InCommit || syncerState.NodeIds) { // can't happen
                Y_FAIL_S("InCommit# " << syncerState.InCommit
                    << " NodeIds# " << FormatList(syncerState.NodeIds | std::views::keys));
            }
            if (now < syncerState.NotBefore) {
                retryAt = Min(retryAt, syncerState.NotBefore);
                continue; // the syncer has failed recently, it is started later
            }

            const NKikimrBlobStorage::TGroupInfo *bridgeGroupInfo = nullptr;
            const TGroupInfo *proxyGroup = FindGroup(syncerState.BridgeProxyGroupId);
            bool poolBridgeMode = true;
            if (const TGroupInfo *group = proxyGroup) {
                Y_ABORT_UNLESS(group->BridgeGroupInfo);
                Y_ABORT_UNLESS(group->BridgeGroupInfo->HasBridgeGroupState());
                bridgeGroupInfo = &group->BridgeGroupInfo.value();
                if (const auto it = StoragePools.find(group->StoragePoolId); it != StoragePools.end()) {
                    poolBridgeMode = it->second.BridgeMode;
                }
            } else if (const auto it = StaticGroups.find(syncerState.BridgeProxyGroupId); it != StaticGroups.end()) {
                const auto& group = it->second.Info->Group;
                Y_ABORT_UNLESS(group);
                Y_ABORT_UNLESS(group->HasBridgeGroupState());
                bridgeGroupInfo = &group.value();
            } else {
                Y_DEBUG_ABORT();
                continue;
            }

            const auto& state = bridgeGroupInfo->GetBridgeGroupState();
            const TBridgePileId authorityPileId = NBridge::GetAuthorityPileId(state);
            // parts of the group being made fully bridged one get synced in synchronized piles too
            const bool upgrading = poolBridgeMode && state.GetAuthorityPileId();
            if (upgradePass && !upgrading) {
                continue; // the group isn't being made a fully bridged one, so this pass has nothing to do for it
            }
            for (size_t pileIndex = 0; pileIndex < state.PileSize(); ++pileIndex) {
                const auto bridgePileId = TBridgePileId::FromPileIndex(pileIndex);
                const auto *bridgePile = BridgeInfo->FindPile(bridgePileId);
                const bool upgradeSync = upgrading && bridgePile &&
                    bridgePile->State == NKikimrBridge::TClusterState::SYNCHRONIZED;
                if (!bridgePile || (!upgradeSync &&
                        bridgePile->State != NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2)) {
                    continue; // can't start syncing yet
                } else if (proxyGroup && proxyGroup->IsBridgePartDropped(bridgePileId, poolBridgeMode)) {
                    continue; // the part is going away
                }

                const auto *groupPile = NBridge::GetGroupPile(state, bridgePileId);
                if (!groupPile) {
                    continue; // the group has no part in this pile
                }
                const auto& pile = *groupPile;
                const auto targetGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                if (targetGroupId != syncerState.TargetGroupId) {
                    continue;
                }
                if (pile.GetStage() == NKikimrBridge::TGroupState::SYNCED) {
                    continue; // this group is synced
                }
                if (const auto *pss = FindPileSyncState(TBridgePileId::FromPileIndex(pileIndex));
                        pss && pile.GetBecameUnsyncedGeneration() < pss->GetBecameUnsyncedGeneration()) {
                    // the part is not reset for the current sync of the pile yet, its stage and generation are the ones
                    // writers that skip the pile may be using; TTxCheckUnsynced (or distconf for static groups) resets it
                    continue;
                }

                if (upgradeSync != upgradePass) {
                    continue;
                }

                // pick the pile to sync from, the primary one preferably; only the part of the synchronized pile is sure to
                // hold everything written to the group, as the pile that has been out may show the SYNCED stage of its
                // previous sync until it gets reset
                auto isSource = [&](TBridgePileId bridgePileId, const NKikimrBridge::TGroupState::TPile& pile) {
                    const auto *bridgePile = BridgeInfo->FindPile(bridgePileId);
                    return bridgePile && bridgePile->State == NKikimrBridge::TClusterState::SYNCHRONIZED &&
                        pile.GetStage() == NKikimrBridge::TGroupState::SYNCED;
                };
                const NKikimrBridge::TGroupState::TPile *source = nullptr;
                if (authorityPileId) {
                    // the group relies on this part solely, it can't be synced while its pile is out
                    const auto *authorityPile = BridgeInfo->FindPile(authorityPileId);
                    source = NBridge::GetGroupPile(state, authorityPileId);
                    if (!authorityPile || !NBridge::PileStateTraits(authorityPile->State).RequiresDataQuorum || !source ||
                            source->GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                        continue;
                    }
                } else if (const auto *primary = BridgeInfo->PrimaryPile) {
                    source = NBridge::GetGroupPile(state, primary->BridgePileId);
                    if (source && !isSource(primary->BridgePileId, *source)) {
                        source = nullptr;
                    }
                }
                if (!source) {
                    source = NBridge::FindGroupPile(state, isSource);
                }
                std::optional<TGroupId> sourceGroupId;
                if (source) {
                    sourceGroupId.emplace(TGroupId::FromProto(source, &NKikimrBridge::TGroupState::TPile::GetGroupId));
                }
                if (!sourceGroupId) {
                    // can't find origin group
                    Y_DEBUG_ABORT();
                    continue;
                }

                // find available nodes
                std::vector<TNodeId> nodes;
                if (const TGroupInfo *group = FindGroup(targetGroupId)) {
                    for (const auto& vdisk : group->VDisksInGroup) {
                        nodes.push_back(vdisk->VSlotId.NodeId);
                    }
                } else if (const auto it = StaticGroups.find(targetGroupId); it != StaticGroups.end()) {
                    for (const auto& actorId : it->second.Info->GetDynamicInfo().ServiceIdForOrderNumber) {
                        nodes.push_back(actorId.NodeId());
                    }
                } else {
                    Y_ABORT();
                }
                if (nodes.empty()) {
                    for (const auto& [nodeId, node] : Nodes) {
                        nodes.push_back(nodeId);
                    }
                }
                auto pred = [&](TNodeId nodeId) {
                    const TNodeInfo *nodeInfo = FindNode(nodeId);
                    return !nodeInfo || !nodeInfo->Registered;
                };
                std::erase_if(nodes, pred);
                if (nodes.empty()) {
                    YDB_LOG_DEBUG("ProcessSyncers: no nodes to start syncer",
                        {"marker", "BSCBR02"},
                        {"targetGroupId", targetGroupId},
                        {"sourceGroupId", *sourceGroupId});
                    continue;
                }

                // pick a random one of the nodes having the fewest syncers, so that syncers run on as many nodes as possible
                size_t minSyncers = Max<size_t>();
                std::vector<TNodeId> leastLoadedNodes;
                for (TNodeId candidateId : nodes) {
                    size_t numSyncers = 0;
                    for (auto jt = NodeToSyncerState.lower_bound({candidateId, nullptr}); jt != NodeToSyncerState.end() &&
                            std::get<0>(*jt) == candidateId; ++jt) {
                        ++numSyncers;
                    }
                    if (numSyncers < minSyncers) {
                        minSyncers = numSyncers;
                        leastLoadedNodes.clear();
                    }
                    if (numSyncers == minSyncers) {
                        leastLoadedNodes.push_back(candidateId);
                    }
                }
                const TNodeId nodeId = leastLoadedNodes[RandomNumber(leastLoadedNodes.size())];

                YDB_LOG_DEBUG("ProcessSyncers: starting syncer",
                    {"marker", "BSCBR03"},
                    {"targetGroupId", targetGroupId},
                    {"sourceGroupId", *sourceGroupId},
                    {"nodes", nodes},
                    {"nodeId", nodeId},
                    {"groupPile", pile});

                syncerState.NodeIds[nodeId].SourceGroupId = *sourceGroupId;
                NodeToSyncerState.emplace(nodeId, &syncerState);
                BridgeSyncNodeRatesDirty = true;
                nodesToUpdate.insert(nodeId);
                syncerState.Unlink();
            }
        }
    }

    if (retryAt < SyncersRetryAt) {
        SyncersRetryAt = retryAt;
        TActivationContext::Schedule(retryAt, new IEventHandle(TEvPrivate::EvRetrySyncers, 0, SelfId(), {}, nullptr, 0));
    }

    UpdateBridgeSyncNodeRates(nodesToUpdate);
    SendSyncersToNodes(nodesToUpdate);
}

void TBlobStorageController::SendSyncersToNodes(const THashSet<TNodeId>& nodeIds) {
    for (TNodeId nodeId : nodeIds) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        TSet<TGroupId> groupIdsToRead;
        SerializeSyncers(nodeId, &ev->Record, groupIdsToRead);
        FillBridgeSyncNodeRate(nodeId, &ev->Record);
        ReadGroups(groupIdsToRead, false, ev.get(), nodeId);
        YDB_LOG_DEBUG("SendSyncersToNodes: sending an update",
            {"marker", "BSCBR09"},
            {"nodeId", nodeId},
            {"record", ev->Record});
        SendToWarden(nodeId, std::move(ev), 0);
    }
}

bool TBlobStorageController::IsNodeHostingSyncers(TNodeId nodeId) const {
    const auto it = NodeToSyncerState.lower_bound({nodeId, nullptr});
    return it != NodeToSyncerState.end() && std::get<0>(*it) == nodeId;
}

void TBlobStorageController::UpdateBridgeSyncNodeRates(const THashSet<TNodeId>& nodesToUpdate) {
    if (!BridgeSyncNodeRatesDirty || TActivationContext::Monotonic() < LoadedAt + SyncerReportingTime) {
        return; // while nodes are still reporting their syncers, any share would exceed the limit
    }
    BridgeSyncNodeRatesDirty = false;

    // syncers of disconnected nodes count until they get dropped, as they may still be running
    std::vector<TNodeId> hostingNodes;
    for (auto it = NodeToSyncerState.begin(); it != NodeToSyncerState.end();
            it = NodeToSyncerState.lower_bound({std::get<0>(*it) + 1, nullptr})) {
        hostingNodes.push_back(std::get<0>(*it));
    }
    const ui64 rate = BridgeSyncTotalRate && !hostingNodes.empty()
        ? Max<ui64>(BridgeSyncTotalRate / hostingNodes.size(), 1)
        : 0;
    BridgeSyncNodeRate = rate;

    // nodes from nodesToUpdate get the rate along with their syncers
    std::vector<TNodeId> changedNodes;
    for (TNodeId nodeId : hostingNodes) {
        // a node not known to have got any rate may still run on the share got from the previous BSC instance
        if (const auto it = SentBridgeSyncNodeRates.find(nodeId);
                (it == SentBridgeSyncNodeRates.end() || it->second != rate) && !nodesToUpdate.contains(nodeId)) {
            changedNodes.push_back(nodeId);
        }
    }
    for (const auto& [nodeId, sentRate] : SentBridgeSyncNodeRates) {
        if (sentRate && !IsNodeHostingSyncers(nodeId) && !nodesToUpdate.contains(nodeId)) {
            changedNodes.push_back(nodeId);
        }
    }

    for (TNodeId nodeId : changedNodes) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        FillBridgeSyncNodeRate(nodeId, &ev->Record);
        YDB_LOG_DEBUG("UpdateBridgeSyncNodeRates: sending an update",
            {"marker", "BSCBR14"},
            {"nodeId", nodeId},
            {"record", ev->Record});
        SendToWarden(nodeId, std::move(ev), 0);
    }
}

void TBlobStorageController::FillBridgeSyncNodeRate(TNodeId nodeId,
        NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate *update) {
    update->SetControllerGeneration(Executor()->Generation());
    if (!BridgeSyncNodeRate) {
        update->SetKeepBridgeSyncNodeRate(true);
        return;
    }
    const ui64 rate = IsNodeHostingSyncers(nodeId) ? *BridgeSyncNodeRate : 0;
    update->SetBridgeSyncNodeRateBytesPerSecond(rate);
    SentBridgeSyncNodeRates[nodeId] = rate;
}

void TBlobStorageController::SerializeSyncers(TNodeId nodeId, NKikimrBlobStorage::TEvControllerNodeServiceSetUpdate *update,
        TSet<TGroupId>& groupIdsToRead) {
    for (auto it = NodeToSyncerState.lower_bound({nodeId, nullptr}); it != NodeToSyncerState.end() && std::get<0>(*it) == nodeId; ++it) {
        const auto& [nodeId, syncerState] = *it;

        const auto jt = syncerState->NodeIds.find(nodeId);
        Y_ABORT_UNLESS(jt != syncerState->NodeIds.end());
        const TGroupId sourceGroupId = jt->second.SourceGroupId;

        auto *syncer = update->AddSyncers();
        using T = std::decay_t<decltype(*syncer)>;

        syncerState->BridgeProxyGroupId.CopyToProto(syncer, &T::SetBridgeProxyGroupId);
        sourceGroupId.CopyToProto(syncer, &T::SetSourceGroupId);
        syncerState->TargetGroupId.CopyToProto(syncer, &T::SetTargetGroupId);

        if (const TGroupInfo *group = FindGroup(syncerState->BridgeProxyGroupId)) {
            syncer->SetBridgeProxyGroupGeneration(group->Generation);
            groupIdsToRead.insert({
                syncerState->BridgeProxyGroupId,
                syncerState->TargetGroupId,
                sourceGroupId,
            });
        } else if (const auto it = StaticGroups.find(syncerState->BridgeProxyGroupId); it != StaticGroups.end()) {
            syncer->SetBridgeProxyGroupGeneration(it->second.Info->GroupGeneration);
        } else {
            Y_DEBUG_ABORT();
            continue;
        }
    }
    update->SetUpdateSyncers(true);
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerUpdateSyncerState::TPtr ev) {
    const TNodeId nodeId = ev->Sender.NodeId();
    YDB_LOG_DEBUG("TEvControllerUpdateSyncerState",
        {"marker", "BSCBR04"},
        {"nodeId", nodeId},
        {"msg", ev->Get()->Record});
    TSet<TGroupId> groupIdsToRead;
    ApplySyncerState(nodeId, ev->Get()->Record, groupIdsToRead, false);
    if (groupIdsToRead) {
        auto update = std::make_unique<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>();
        ReadGroups(groupIdsToRead, false, update.get(), nodeId);
        YDB_LOG_DEBUG("TEvControllerUpdateSyncerState: sending update",
            {"marker", "BSCBR05"},
            {"msg", update->Record});
        SendToWarden(nodeId, std::move(update), 0);
    }
}

void TBlobStorageController::RenderBridge(IOutputStream& out) {
    RenderHeader(out);

    HTML(out) {
        if (PileOverridesError) {
            DIV_CLASS("alert alert-danger") {
                out << "Storage pool pile overrides of StorageConfig are not applied, the previous ones (of StorageConfig"
                    " generation " << (AppliedPileOverrides ? AppliedPileOverrides->GetStorageConfigGeneration() : 0)
                    << ") are in effect: ";
                out << *PileOverridesError;
            }
        }

        TAG(TH3) {
            out << "Working syncers";
        }

        TABLE_CLASS("table") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Bridge proxy group id"; }
                    TABLEH() { out << "Target group id"; }
                    TABLEH() { out << "State"; }
                    TABLEH() { out << "Node id"; }
                    TABLEH() { out << "Source group id"; }
                    TABLEH() { out << "Progress"; }
                    TABLEH() { out << "Bytes"; }
                    TABLEH() { out << "Blobs"; }
                    TABLEH() { out << "Left to copy"; }
                    TABLEH() { out << "Copy rate"; }
                }
            }
            TABLEBODY() {
                static const char *bytesSuffixes[] = {"B", "KiB", "MiB", "GiB", "TiB", nullptr};
                for (auto& [targetGroupId, syncerState] : TargetGroupToSyncerState) {
                    TString state;
                    std::optional<NKikimrBridge::TGroupState::EStage> stage;
                    auto scanPiles = [&](auto& info) {
                        if (!info) {
                            return;
                        }
                        if (const auto *pile = NBridge::FindGroupPileByGroupId(info->GetBridgeGroupState(), targetGroupId)) {
                            stage = pile->GetStage();
                            state = NKikimrBridge::TGroupState::EStage_Name(*stage);
                        }
                    };
                    auto outCopy = [&] {
                        TABLED() {
                            const auto remaining = stage ? GetBridgeSyncRemaining(targetGroupId, *stage) : std::nullopt;
                            if (remaining) {
                                if (!remaining->Complete) {
                                    out << "&ge;";
                                }
                                FormatHumanReadable(out, remaining->Bytes, 1024, 1, bytesSuffixes);
                            } else {
                                out << "-";
                            }
                        }
                        TABLED() {
                            FormatHumanReadable(out, syncerState.CopyRate, 1024, 1, bytesSuffixes);
                            out << "/s";
                        }
                    };
                    if (const TGroupInfo *group = FindGroup(syncerState.BridgeProxyGroupId)) {
                        scanPiles(group->BridgeGroupInfo);
                    } else if (const auto it = StaticGroups.find(syncerState.BridgeProxyGroupId); it != StaticGroups.end()) {
                        scanPiles(it->second.Info->Group);
                    }

                    if (syncerState.NodeIds.empty()) {
                        TABLER() {
                            TABLED() { out << syncerState.BridgeProxyGroupId; }
                            TABLED() { out << targetGroupId; }
                            TABLED() { out << state; }
                            TABLED() { out << "-"; }
                            TABLED() { out << "-"; }
                            TABLED() { out << "-"; }
                            TABLED() { out << "-"; }
                            TABLED() { out << "-"; }
                            outCopy();
                        }
                    }
                    for (const auto& [nodeId, perNodeInfo] : syncerState.NodeIds) {
                        TABLER() {
                            TABLED() { out << syncerState.BridgeProxyGroupId; }
                            TABLED() { out << targetGroupId; }
                            TABLED() { out << state; }
                            TABLED() {
                                out << "<a href='/node/" << nodeId << "/actors/nodewarden#syncer-" << targetGroupId << "'>" << nodeId << "</a>";
                            }
                            TABLED() {
                                out << perNodeInfo.SourceGroupId;
                            }

                            auto& progress = perNodeInfo.Progress;
                            TABLED() {
                                if (progress.BytesTotal) {
                                    const int percent = 10'000 * progress.BytesDone / progress.BytesTotal;
                                    out << Sprintf("%d.%02d%%", percent / 100, percent % 100);
                                }
                            }
                            TABLED() {
                                FormatHumanReadable(out, progress.BytesDone, 1024, 1, bytesSuffixes);
                                out << '/';
                                FormatHumanReadable(out, progress.BytesTotal, 1024, 1, bytesSuffixes);
                                out << '(';
                                FormatHumanReadable(out, progress.BytesError, 1024, 1, bytesSuffixes);
                                out << ')';
                            }
                            TABLED() {
                                static const char *blobsSuffixes[] = {"", "K", "M", nullptr};
                                FormatHumanReadable(out, progress.BlobsDone, 1000, 1, blobsSuffixes);
                                out << '/';
                                FormatHumanReadable(out, progress.BlobsTotal, 1000, 1, blobsSuffixes);
                                out << '(';
                                FormatHumanReadable(out, progress.BlobsError, 1000, 1, blobsSuffixes);
                                out << ')';
                            }
                            outCopy();
                        }
                    }
                }
            }
        }
    }
}

void TBlobStorageController::ApplyStaticGroupUpdateForSyncers(std::map<TGroupId, TStaticGroupInfo>& prevStaticGroups) {
    // make a list of all currently unsynced groups
    THashSet<TGroupId> targetGroupsToDelete;
    for (const auto& [groupId, group] : prevStaticGroups) {
        if (group.Info && group.Info->Group && group.Info->Group->HasBridgeGroupState()) {
            NBridge::ForEachGroupPile(group.Info->Group->GetBridgeGroupState(), [&](TBridgePileId,
                    const NKikimrBridge::TGroupState::TPile& pile) {
                const auto targetGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                if (pile.GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                    Y_ABORT_UNLESS(TargetGroupToSyncerState.contains(targetGroupId));
                    const bool inserted = targetGroupsToDelete.insert(targetGroupId).second;
                    Y_ABORT_UNLESS(inserted);
                } else {
                    Y_ABORT_UNLESS(!TargetGroupToSyncerState.contains(targetGroupId));
                }
            });
        }
    }

    // process fresh group list
    THashSet<TNodeId> nodesToUpdate;
    for (const auto& group : StorageConfig->GetBlobStorageConfig().GetServiceSet().GetGroups()) {
        if (group.HasBridgeGroupState()) {
            const auto groupId = TGroupId::FromProto(&group, &NKikimrBlobStorage::TGroupInfo::GetGroupID);
            const auto& bridgeGroupState = group.GetBridgeGroupState();
            NBridge::ForEachGroupPile(bridgeGroupState, [&](TBridgePileId, const NKikimrBridge::TGroupState::TPile& pile) {
                if (pile.GetStage() != NKikimrBridge::TGroupState::SYNCED) {
                    const auto targetGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                    const auto [it, inserted] = TargetGroupToSyncerState.try_emplace(targetGroupId, groupId,
                        targetGroupId);
                    if (inserted) {
                        EnqueueSyncer(it->second);
                    } else {
                        const size_t n = targetGroupsToDelete.erase(targetGroupId);
                        Y_ABORT_UNLESS(n == 1);

                        const auto prevIt = prevStaticGroups.find(it->second.BridgeProxyGroupId);
                        const auto curIt = StaticGroups.find(it->second.BridgeProxyGroupId);
                        if (prevIt != prevStaticGroups.end() && curIt != StaticGroups.end() &&
                                prevIt->second.Info->GroupGeneration < curIt->second.Info->GroupGeneration) {
                            // bridge proxy group generation has changed and hence we have to issue new command with
                            // the relevant generation
                            for (TNodeId nodeId : it->second.NodeIds | std::views::keys) {
                                YDB_LOG_DEBUG("Refreshing syncers with obsolete static group config",
                                    {"marker", "BSCBR01"},
                                    {"nodeId", nodeId},
                                    {"pile", pile},
                                    {"bridgeGroupState", bridgeGroupState});
                                nodesToUpdate.insert(nodeId);
                            }
                        }
                    }
                }
            });
        }
    }

    // remove obsolete entries; errors of the syncers of the targets gone (synced ones, or the parts in the pile removed)
    // are forgotten as well
    std::vector<TTxUpdateBridgeSyncState::TGroupStateUpdate> syncStateUpdates;
    for (TGroupId targetGroupId : targetGroupsToDelete) {
        const auto it = TargetGroupToSyncerState.find(targetGroupId);
        Y_ABORT_UNLESS(it != TargetGroupToSyncerState.end());
        TSyncerState& syncerState = it->second;
        for (TNodeId nodeId : syncerState.NodeIds | std::views::keys) {
            const size_t n = NodeToSyncerState.erase({nodeId, &syncerState});
            BridgeSyncNodeRatesDirty = true;
            Y_ABORT_UNLESS(n == 1);
        }
        TargetGroupToSyncerState.erase(it);
        if (BridgeSyncState.contains(targetGroupId)) {
            syncStateUpdates.push_back({targetGroupId, TTxUpdateBridgeSyncState::TChangeStage{}});
        }
    }
    if (!syncStateUpdates.empty()) {
        Execute(std::make_unique<TTxUpdateBridgeSyncState>(this, std::move(syncStateUpdates), TActivationContext::Now()));
    }

    // kick syncers for changed groups
    SendSyncersToNodes(nodesToUpdate);
}

void TBlobStorageController::CommitSyncerUpdates(TConfigState& state, TTransactionContext& txc) {
    // collect groups that are not sync targets anymore: deleted groups and piles that stopped being piles (e.g. when the
    // whole bridged group gets decommitted, inline pile stays, but it isn't a pile anymore); a group is a pile iff it has
    // BridgePileId, which is checked when loading
    THashSet<TGroupId> targetGroupIds;
    THashSet<TGroupId> proxyGroupsWithNewGeneration;
    for (const auto& [base, overlay] : state.Groups.Diff()) {
        if (base && (!overlay->second || (base->second->BridgePileId && !overlay->second->BridgePileId))) {
            targetGroupIds.insert(base->first);
        }
        if (base && overlay->second && overlay->second->BridgeGroupInfo &&
                base->second->Generation != overlay->second->Generation) {
            proxyGroupsWithNewGeneration.insert(overlay->first);
        }
    }

    if (!proxyGroupsWithNewGeneration.empty()) {
        // queries of a syncer running with the old generation get rejected, so its node has to know at once; a report of
        // the syncer would tell BSC about it only after the syncer has made some progress, which it can't
        state.Callbacks.push_back([this, groupIds = std::move(proxyGroupsWithNewGeneration)] {
            THashSet<TNodeId> nodeIds;
            for (TGroupId groupId : groupIds) {
                if (const TGroupInfo *group = FindGroup(groupId); group && group->BridgeGroupInfo) {
                    NBridge::ForEachGroupPile(group->BridgeGroupInfo->GetBridgeGroupState(), [&](TBridgePileId,
                            const NKikimrBridge::TGroupState::TPile& pile) {
                        const auto targetGroupId = TGroupId::FromProto(&pile, &NKikimrBridge::TGroupState::TPile::GetGroupId);
                        if (const auto it = TargetGroupToSyncerState.find(targetGroupId); it != TargetGroupToSyncerState.end()) {
                            for (TNodeId nodeId : it->second.NodeIds | std::views::keys) {
                                nodeIds.insert(nodeId);
                            }
                        }
                    });
                }
            }
            SendSyncersToNodes(nodeIds);
        });
    }

    for (const TGroupId groupId : targetGroupIds) {
        if (const auto it = TargetGroupToSyncerState.find(groupId); it != TargetGroupToSyncerState.end()) {
            TSyncerState& syncerState = it->second;
            for (TNodeId nodeId : syncerState.NodeIds | std::views::keys) {
                const size_t n = NodeToSyncerState.erase({nodeId, &syncerState});
                BridgeSyncNodeRatesDirty = true;
                Y_ABORT_UNLESS(n == 1);
            }
            TargetGroupToSyncerState.erase(it);
        }
        if (BridgeSyncState.erase(groupId)) {
            NIceDb::TNiceDb(txc.DB).Table<Schema::BridgeSyncState>().Key(groupId.GetRawId()).Delete();
        }
    }

    if (!targetGroupIds.empty()) {
        // the pile may have been waiting only for the dropped targets to get synced
        state.Callbacks.push_back([this] {
            RecheckUnsyncedBridgePiles = true;
            CheckUnsyncedBridgePiles();
        });
    }
}

} // NKikimr::NBsController
