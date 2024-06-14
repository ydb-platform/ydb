#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/protos/blob_depot_config.pb.h>

namespace NKikimr::NSchemeShard {

    namespace {

        enum class EAction {
            Create,
            Alter,
            Drop,
        };

        class TBlobDepot : public TSubOperation {
            const EAction Action;

        public:
            template<TTxState::ETxState ExpectedState>
            class TSubOperationStateBase : public TSubOperationState {
            protected:
                const TOperationId OperationId;

            public:
                TSubOperationStateBase(TOperationId id)
                    : OperationId(id)
                {}

                TTxState *GetTxState(TOperationContext& context) const {
                    TTxState *txState = context.SS->FindTx(OperationId);
                    Y_ABORT_UNLESS(txState);
                    Y_ABORT_UNLESS(txState->TxType == TTxState::TxCreateBlobDepot || txState->TxType == TTxState::TxAlterBlobDepot ||
                        txState->TxType == TTxState::TxDropBlobDepot);
                    Y_ABORT_UNLESS(txState->State == ExpectedState);
                    return txState;
                }

                TString DebugHint() const override {
                    return TStringBuilder() << "TBlobDepot OperationId# " << OperationId;
                }
            };

            class TConfigureBlobDepotParts : public TSubOperationStateBase<TTxState::ConfigureParts> {
            public:
                using TSubOperationStateBase::TSubOperationStateBase;

                bool ProgressState(TOperationContext& context) override {
                    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, DebugHint()
                            << " TConfigureBlobDepotParts::ProgressState"
                            << " at schemeshard# " << context.SS->SelfTabletId());

                    TTxState *txState = GetTxState(context);
                    txState->ClearShardsInProgress();

                    auto path = TPath::Init(txState->TargetPathId, context.SS);
                    Y_ABORT_UNLESS(path.IsResolved());

                    auto blobDepotInfo = context.SS->BlobDepots[path->PathId];
                    Y_ABORT_UNLESS(blobDepotInfo);

                    for (auto& shard : txState->Shards) {
                        auto shardIdx = shard.Idx;
                        blobDepotInfo->BlobDepotShardIdx = shard.Idx;
                        auto tabletId = context.SS->ShardInfos[shardIdx].TabletID;
                        blobDepotInfo->BlobDepotTabletId = tabletId;
                        Y_ABORT_UNLESS(shard.TabletType == ETabletType::BlobDepot);
                        auto event = std::make_unique<TEvBlobDepot::TEvApplyConfig>(static_cast<ui64>(OperationId.GetTxId()));
                        event->Record.MutableConfig()->CopyFrom(blobDepotInfo->Description.GetConfig());
                        context.OnComplete.BindMsgToPipe(OperationId, tabletId, shardIdx, event.release());
                        txState->ShardsInProgress.insert(shardIdx);
                    }

                    Y_ABORT_UNLESS(txState->ShardsInProgress);
                    return false;
                }

                bool HandleReply(TEvBlobDepot::TEvApplyConfigResult::TPtr& ev, TOperationContext& context) override {
                    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, DebugHint()
                            << " TConfigureBlobDepotParts::HandleReply"
                            << " at schemeshard# " << context.SS->SelfTabletId());

                    TTxState *txState = GetTxState(context);
                    Y_ABORT_UNLESS(txState->ShardsInProgress);

                    const auto& record = ev->Get()->Record;
                    const TTabletId tabletId(record.GetTabletId());
                    const TShardIdx shardIdx = context.SS->MustGetShardIdx(tabletId);
                    txState->ShardsInProgress.erase(shardIdx);
                    context.OnComplete.UnbindMsgFromPipe(OperationId, tabletId, shardIdx);

                    return !txState->ShardsInProgress;
                }
                
                TString DebugHint() const override {
                    return TStringBuilder() << "TConfigureBlobDepotParts id# " << OperationId;
                }
            };

            class TProposeBlobDepotCreate : public TSubOperationStateBase<TTxState::Propose> {
            public:
                using TSubOperationStateBase::TSubOperationStateBase;

                bool ProgressState(TOperationContext& context) override {
                    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, DebugHint()
                            << " TProposeBlobDepotCreate::ProgressState"
                            << " at schemeshard# " << context.SS->SelfTabletId());

                    TTxState *txState = GetTxState(context);
                    context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));
                    return false;
                }

                bool HandleReply(TEvPrivate::TEvOperationPlan::TPtr& ev, TOperationContext& context) override {
                    LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, DebugHint()
                            << " TProposeBlobDepotCreate::HandleReply"
                            << " at schemeshard# " << context.SS->SelfTabletId());

                    TStepId step = TStepId(ev->Get()->StepId);

                    TTxState *txState = GetTxState(context);
                    // TODO: maybe null?

                    TPathId pathId = txState->TargetPathId;
                    TPathElement::TPtr path = context.SS->PathsById.at(pathId);

                    NIceDb::TNiceDb db(context.GetDB());

                    path->StepCreated = step;
                    context.SS->PersistCreateStep(db, pathId, step);

                    IncParentDirAlterVersionWithRepublish(OperationId, TPath::Init(pathId, context.SS), context);

                    return true;
                }
            };

        public:
            TBlobDepot(EAction action, TOperationId id, const TTxTransaction& tx)
                : TSubOperation(id, tx)
                , Action(action)
            {}

            TBlobDepot(EAction action, TOperationId id, TTxState::ETxState state)
                : TSubOperation(id, state)
                , Action(action)
            {
                Y_ABORT_UNLESS(state != TTxState::Invalid);
                SetState(state);
            }

            THolder<TProposeResponse> Propose(const TString& owner, TOperationContext& context) override {
                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TBlobDepot::Propose"
                    << " OperationId# " << OperationId
                    << " at schemeshard# " << context.SS->SelfTabletId());

                switch (Action) {
                    case EAction::Create: return ProposeCreate(owner, context);
                    case EAction::Alter: return ProposeAlter(owner, context);
                    case EAction::Drop: return ProposeDrop(owner, context);
                }
                Y_ABORT("unreachable code");
            }

            void AbortPropose(TOperationContext& context) override {
                LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TBlobDepot::AbortPropose"
                    << " OperationId# " << OperationId
                    << " at schemeshard# " << context.SS->SelfTabletId());

                Y_ABORT();
            }

            void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
                LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TBlobDepot::AbortUnsafe"
                    << " OperationId# " << OperationId
                    << " forceDropId# " << forceDropTxId
                    << " at schemeshard# " << context.SS->TabletID());

                context.OnComplete.DoneOperation(OperationId);
            }

            void StateDone(TOperationContext& context) override {
                static const std::unordered_map<std::pair<EAction, TTxState::ETxState>, TTxState::ETxState> stateMachine{
                    // create
                    {{EAction::Create, TTxState::Waiting}, TTxState::ConfigureParts},
                    {{EAction::Create, TTxState::CreateParts}, TTxState::ConfigureParts},
                    {{EAction::Create, TTxState::ConfigureParts}, TTxState::Propose},
                    {{EAction::Create, TTxState::Propose}, TTxState::Done},
                    {{EAction::Create, TTxState::Done}, TTxState::Invalid},
                    // alter
                    {{EAction::Alter, TTxState::Waiting}, TTxState::Propose},
                    {{EAction::Alter, TTxState::ConfigureParts}, TTxState::Propose},
                    // drop
                    {{EAction::Drop, TTxState::Waiting}, TTxState::Propose},
                    {{EAction::Drop, TTxState::DeleteParts}, TTxState::Propose},
                };

                const auto it = stateMachine.find({Action, GetState()});
                Y_ABORT_UNLESS(it != stateMachine.end());

                LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "TBlobDepot::StateDone"
                    << " OperationId# " << OperationId
                    << " at schemeshard# " << context.SS->TabletID()
                    << " State# " << TTxState::StateName(GetState())
                    << " next State# " << TTxState::StateName(it->second));

                if (it->second != TTxState::Invalid) {
                    NIceDb::TNiceDb db(context.GetDB());
                    context.SS->ChangeTxState(db, OperationId, it->second);
                    SetState(it->second);
                    context.OnComplete.ActivateTx(OperationId);
                }
            }

        private:
            std::optional<TString> ValidateConfig(const NKikimrSchemeOp::TBlobDepotDescription& desc) {
                ui32 numChannels = 0;
                for (const auto& item : desc.GetConfig().GetChannelProfiles()) {
                    if (!item.HasCount()) {
                        return "missing Count field in channel profile item";
                    } else if (!item.GetCount()) {
                        return "Count field can't contain zero in channel profile item";
                    }
                    numChannels += item.GetCount();
                }
                if (numChannels < 3) {
                    return "too few channels specified in channel profile";
                } else if (numChannels >= 256) {
                    return "too many channels specified in channel profile";
                }

                return std::nullopt;
            }

        private:
            THolder<TProposeResponse> ProposeCreate(const TString& owner, TOperationContext& context) {
                const auto ssId = static_cast<ui64>(context.SS->SelfTabletId());
                const auto txId = static_cast<ui64>(OperationId.GetTxId());
                const TString& parentPathStr = Transaction.GetWorkingDir();
                const auto& description = Transaction.GetBlobDepot();

                if (const auto& error = ValidateConfig(description)) {
                    return MakeHolder<TProposeResponse>(NKikimrScheme::StatusInvalidParameter, txId, ssId, *error);
                }

                TPath parentPath = TPath::Resolve(parentPathStr, context.SS);
                {
                    TPath::TChecker checks = parentPath.Check();
                    checks
                        .NotUnderDomainUpgrade()
                        .IsAtLocalSchemeShard()
                        .IsResolved()
                        .NotDeleted()
                        .NotUnderDeleting()
                        .IsCommonSensePath()
                        .IsLikeDirectory();

                    if (!checks) {
                        return MakeHolder<TProposeResponse>(checks.GetStatus(), txId, ssId, checks.GetError());
                    }
                }

                const TString acl = Transaction.GetModifyACL().GetDiffACL();

                TPath dstPath = parentPath.Child(description.GetName());
                {
                    TPath::TChecker checks = dstPath.Check();
                    checks.IsAtLocalSchemeShard();
                    if (dstPath.IsResolved()) {
                        checks
                            .IsResolved()
                            .NotUnderDeleting()
                            .FailOnExist(TPathElement::EPathType::EPathTypeBlobDepot, !Transaction.GetFailOnExist());
                    } else {
                        checks
                            .NotEmpty()
                            .NotResolved();
                    }

                    if (checks) {
                        checks
                            .IsValidLeafName()
                            .DepthLimit()
                            .PathsLimit()
                            .DirChildrenLimit()
                            .ShardsLimit()
                            .PathShardsLimit()
                            .IsValidACL(acl);
                    }

                    if (!checks) {
                        auto resp = MakeHolder<TProposeResponse>(checks.GetStatus(), txId, ssId, checks.GetError());
                        if (dstPath.IsResolved()) {
                            resp->SetPathCreateTxId(ui64(dstPath->CreateTxId));
                            resp->SetPathId(dstPath->PathId.LocalPathId);
                        }
                        return resp;
                    }
                }

                // extract storage pool kinds into separate array
                const auto& config = description.GetConfig();
                TVector<TStringBuf> storagePoolKinds;
                for (const auto& item : config.GetChannelProfiles()) {
                    for (ui32 num = item.GetCount(); num; --num) {
                        storagePoolKinds.emplace_back(item.GetStoragePoolKind());
                    }
                }

                // bind channels to storage pools
                TChannelsBindings channelBindings;
                if (!context.SS->ResolveChannelsByPoolKinds(storagePoolKinds, dstPath.GetPathIdForDomain(), channelBindings)) {
                    return MakeHolder<TProposeResponse>(NKikimrScheme::StatusInvalidParameter, txId, ssId,
                        "Unable to construct channel binding with the storage pool");
                }

                TString reason;
                if (!context.SS->CheckApplyIf(Transaction, reason)) {
                    return MakeHolder<TProposeResponse>(NKikimrScheme::StatusPreconditionFailed, txId, ssId, reason);
                }

                dstPath.MaterializeLeaf(owner);

                context.SS->TabletCounters->Simple()[COUNTER_BLOB_DEPOT_COUNT].Add(1);
                auto blobDepot = MakeIntrusive<TBlobDepotInfo>(1u, description);

                NIceDb::TNiceDb db(context.GetDB());

                dstPath->CreateTxId = OperationId.GetTxId();
                dstPath->LastTxId = OperationId.GetTxId();
                dstPath->PathState = TPathElement::EPathState::EPathStateCreate;
                dstPath->PathType = TPathElement::EPathType::EPathTypeBlobDepot;
                const TPathId pathId = dstPath->PathId;

                TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxCreateBlobDepot, pathId);

                const TShardIdx shardIdx = context.SS->RegisterShardInfo(
                    TShardInfo::BlobDepotInfo(OperationId.GetTxId(), pathId).WithBindedChannels(channelBindings));
                context.SS->TabletCounters->Simple()[COUNTER_BLOB_DEPOT_SHARD_COUNT].Add(1);
                txState.Shards.emplace_back(shardIdx, ETabletType::BlobDepot, TTxState::CreateParts);
                blobDepot->BlobDepotShardIdx = shardIdx;

                if (parentPath->HasActiveChanges()) {
                    TTxId parentTxId = parentPath->PlannedToCreate() ? parentPath->CreateTxId : parentPath->LastTxId;
                    context.OnComplete.Dependence(parentTxId, OperationId.GetTxId());
                }

                context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);
                context.OnComplete.ActivateTx(OperationId);

                if (!acl.empty()) {
                    dstPath->ApplyACL(acl);
                }
                context.SS->PersistPath(db, dstPath->PathId);

                context.SS->BlobDepots[pathId] = blobDepot;
                context.SS->PersistBlobDepot(db, pathId, *blobDepot);
                context.SS->IncrementPathDbRefCount(pathId);

                context.SS->PersistTxState(db, OperationId);
                context.SS->PersistUpdateNextPathId(db);
                context.SS->PersistUpdateNextShardIdx(db);
                for (auto shard : txState.Shards) {
                    Y_ABORT_UNLESS(shard.Operation == TTxState::CreateParts);
                    context.SS->PersistChannelsBinding(db, shard.Idx, context.SS->ShardInfos[shard.Idx].BindedChannels);
                    context.SS->PersistShardMapping(db, shard.Idx, InvalidTabletId, pathId, OperationId.GetTxId(), shard.TabletType);
                }

                ++parentPath->DirAlterVersion;
                context.SS->PersistPathDirAlterVersion(db, parentPath.Base());
                context.SS->ClearDescribePathCaches(parentPath.Base());
                context.OnComplete.PublishToSchemeBoard(OperationId, parentPath->PathId);
                context.SS->ClearDescribePathCaches(dstPath.Base());
                context.OnComplete.PublishToSchemeBoard(OperationId, dstPath->PathId);
                dstPath.DomainInfo()->IncPathsInside();
                dstPath.DomainInfo()->AddInternalShards(txState);
                dstPath->IncShardsInside();
                parentPath->IncAliveChildren();

                SetState(TTxState::CreateParts);
                
                auto resp = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, txId, ssId);
                resp->SetPathId(pathId.LocalPathId);
                return resp;
            }

            THolder<TProposeResponse> ProposeAlter(const TString& owner, TOperationContext& context) {
                (void)owner, (void)context;
                SetState(TTxState::ConfigureParts);
                return nullptr;
            }

            THolder<TProposeResponse> ProposeDrop(const TString& owner, TOperationContext& context) {
                (void)owner, (void)context;
                SetState(TTxState::DeleteParts);
                return nullptr;
            }

        private:
            template<typename T>
            struct TFactoryImpl {
                TSubOperationState::TPtr operator ()(TOperationId id) const {
                    return MakeHolder<T>(id);
                }
            };

            TTxState::ETxState NextState(TTxState::ETxState) const override {
                Y_ABORT("unreachable");
            }

            TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
                using TFactory = std::function<TSubOperationState::TPtr(TOperationId)>;
                static const std::unordered_map<std::pair<EAction, TTxState::ETxState>, TFactory> FactoryMap{
                    {{EAction::Create, TTxState::Waiting}, TFactoryImpl<TCreateParts>()},
                    {{EAction::Create, TTxState::CreateParts}, TFactoryImpl<TCreateParts>()},
                    {{EAction::Create, TTxState::ConfigureParts}, TFactoryImpl<TConfigureBlobDepotParts>()},
                    {{EAction::Create, TTxState::Propose}, TFactoryImpl<TProposeBlobDepotCreate>()},
                    {{EAction::Create, TTxState::Done}, TFactoryImpl<TDone>()},
                };

                Y_ABORT_UNLESS(state != TTxState::Invalid);
                const auto it = FactoryMap.find({Action, state});
                Y_ABORT_UNLESS(it != FactoryMap.end());
                return it->second(OperationId);
            }
        };

    }

    ISubOperation::TPtr CreateNewBlobDepot(TOperationId id, const TTxTransaction& tx) { return MakeIntrusive<TBlobDepot>(EAction::Create, id, tx); }
    ISubOperation::TPtr CreateNewBlobDepot(TOperationId id, TTxState::ETxState state) { return MakeIntrusive<TBlobDepot>(EAction::Create, id, state); }
    ISubOperation::TPtr CreateAlterBlobDepot(TOperationId id, const TTxTransaction& tx) { return MakeIntrusive<TBlobDepot>(EAction::Alter, id, tx); }
    ISubOperation::TPtr CreateAlterBlobDepot(TOperationId id, TTxState::ETxState state) { return MakeIntrusive<TBlobDepot>(EAction::Alter, id, state); }
    ISubOperation::TPtr CreateDropBlobDepot(TOperationId id, const TTxTransaction& tx) { return MakeIntrusive<TBlobDepot>(EAction::Drop, id, tx); }
    ISubOperation::TPtr CreateDropBlobDepot(TOperationId id, TTxState::ETxState state) { return MakeIntrusive<TBlobDepot>(EAction::Drop, id, state); }

}
