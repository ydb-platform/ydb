#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common_subdomain.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>


#define LOG_D(stream) LOG_DEBUG_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_E(stream) LOG_ERROR_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

namespace {

void AddShardsTo(TTxState& txState, TTxId txId, TPathId pathId,
    ui32 count, TTabletTypes::EType type,
    const TChannelsBindings& channelsBindings,
    TSchemeShard* ss)
{
    txState.Shards.reserve(count);
    for (ui64 i = 0; i < count; ++i) {
        auto shardId = ss->RegisterShardInfo(
            TShardInfo(txId, pathId, type).WithBindedChannels(channelsBindings)
        );
        txState.Shards.emplace_back(shardId, type, TTxState::CreateParts);
    }
}

struct TParamsDelta {
    uint64_t CoordinatorsAdded = 0;
    uint64_t MediatorsAdded = 0;
    uint64_t TimeCastBucketsPerMediatorAdded = 0;
    uint8_t AddExternalSchemeShard = 0;
    uint8_t AddExternalHive = 0;
    uint8_t AddExternalSysViewProcessor = 0;
    uint8_t AddExternalStatisticsAggregator = 0;
    uint8_t AddGraphShard = 0;
    uint8_t AddBackupController = 0;
    bool SharedTxSupportAdded = false;
    TVector<TStoragePool> StoragePoolsAdded;
    bool ServerlessComputeResourcesModeChanged = false;
};

std::tuple<NKikimrScheme::EStatus, TString>
VerifyParams(TParamsDelta* delta, const TPathId pathId, const TSubDomainInfo::TPtr& current,
             const NKikimrSubDomains::TSubDomainSettings& input, const bool isServerlessExclusiveDynamicNodesEnabled) {
    auto paramError = [](const TStringBuf& msg) {
        return std::make_tuple(NKikimrScheme::EStatus::StatusInvalidParameter,
            TStringBuilder() << "Invalid ExtSubDomain request: " << msg
        );
    };

    // Process input TSubDomainSetting using diff semantics:
    // - present diff.param indicate change to the state
    // - unset diff.param does not matter
    // - state with applied change should be valid and workable
    //
    // Currently this operation support very few workable result states:
    // 1. extsubdomain with full SharedTxSupport (ExternalSchemeShard, Coordinators, Mediators + required params),
    //   with or without ExternalHive, ExternalSysViewProcessor and ExternalStatisticsAggregator
    //

    // First params check: single values

    // PlanResolution
    uint64_t planResolutionAdded = 0;
    if (input.HasPlanResolution()) {
        const auto prev = current->GetPlanResolution();
        const auto next = input.GetPlanResolution();
        const bool changed = (prev != next);

        if (changed) {
            if (prev != 0) {
                return paramError("PlanResolution could be set only once");
            }
            planResolutionAdded = next;
        }
    }

    // Coordinators (also mediators) check:
    // if state.param unset, then diff.param:
    // - could be 0 -> state.param stays unset
    // - could be non-zero value -> state.param become set
    // if state.param set, then state.param:
    // - couldn't be changed
    // - couldn't be unset
    //
    uint64_t coordinatorsAdded = 0;
    if (input.HasCoordinators()) {
        const auto prev = current->GetProcessingParams().CoordinatorsSize();
        const auto next = input.GetCoordinators();
        const bool changed = (prev != next);

        if (changed) {
            if (prev != 0) {
                return paramError("Coordinators could be set only once");
            }
            coordinatorsAdded = next;
        }
    }
    // Mediators checks
    uint64_t mediatorsAdded = 0;
    if (input.HasMediators()) {
        const auto prev = current->GetProcessingParams().MediatorsSize();
        const auto next = input.GetMediators();
        const bool changed = (prev != next);

        if (changed) {
            if (prev != 0) {
                return paramError("Mediators could be set only once");
            }
            mediatorsAdded = next;
        }
    }

    // TimeCastBucketsPerMediator
    uint64_t timeCastBucketsPerMediatorAdded = 0;
    if (input.HasTimeCastBucketsPerMediator()) {
        const auto prev = current->GetProcessingParams().GetTimeCastBucketsPerMediator();
        const auto next = input.GetTimeCastBucketsPerMediator();
        const bool changed = (prev != next);

        if (changed) {
            if (prev != 0) {
                return paramError("TimeCastBucketsPerMediator could be set only once");
            }
            timeCastBucketsPerMediatorAdded = next;
        }
    }

    // ExternalSchemeShard checks
    uint8_t addExternalSchemeShard = 0;
    if (input.HasExternalSchemeShard()) {
        const bool prev = bool(current->GetTenantSchemeShardID());
        const bool next = input.GetExternalSchemeShard();
        const bool changed = (prev != next);

        if (changed) {
            if (next == false) {
                return paramError("ExternalSchemeShard could only be added, not removed");
            }
            addExternalSchemeShard = 1;
        }
    }

    // ExternalHive checks
    uint8_t addExternalHive = 0;
    if (input.HasExternalHive()) {
        const bool prev = bool(current->GetTenantHiveID());
        const bool next = input.GetExternalHive();
        const bool changed = (prev != next);

        if (changed) {
            if (next == false) {
                return paramError("ExternalHive could only be added, not removed");
            }
            addExternalHive = 1;
        }
    }

    // ExternalSysViewProcessor checks
    uint8_t addExternalSysViewProcessor = 0;
    if (input.HasExternalSysViewProcessor()) {
        const bool prev = bool(current->GetTenantSysViewProcessorID());
        const bool next = input.GetExternalSysViewProcessor();
        const bool changed = (prev != next);

        if (changed) {
            if (next == false) {
                return paramError("ExternalSysViewProcessor could only be added, not removed");
            }
            addExternalSysViewProcessor = 1;
        }
    }

    // ExternalStatisticsAggregator checks
    uint8_t addExternalStatisticsAggregator = 0;
    if (input.HasExternalStatisticsAggregator()) {
        const bool prev = bool(current->GetTenantStatisticsAggregatorID());
        const bool next = input.GetExternalStatisticsAggregator();
        const bool changed = (prev != next);

        if (changed) {
            if (next == false) {
                return paramError("ExternalStatisticsAggregator could only be added, not removed");
            }
            addExternalStatisticsAggregator = 1;
        }
    }

    // GraphShard checks
    uint8_t addGraphShard = 0;
    if (input.GetGraphShard()) {
        const bool prev = bool(current->GetTenantGraphShardID());
        const bool next = input.GetGraphShard();
        const bool changed = (prev != next);

        if (changed) {
            if (next == false) {
                return paramError("GraphShard could only be added, not removed");
            }
            addGraphShard = 1;
        }
    }

    // BackupController checks
    uint8_t addBackupController = 0;
    if (input.GetExternalBackupController()) {
        const bool prev = bool(current->GetTenantBackupControllerID());
        const bool next = input.GetExternalBackupController();
        const bool changed = (prev != next);

        if (changed) {
            if (next == false) {
                return paramError("BackupController could only be added, not removed");
            }
            addBackupController = 1;
        }
    }

    // Second params check: combinations

    bool sharedTxSupportAdded = (coordinatorsAdded + mediatorsAdded) > 0;

    if (sharedTxSupportAdded) {
        if (0 == coordinatorsAdded) {
            return paramError("can not create ExtSubDomain with zero Coordinators");
        }
        if (0 == mediatorsAdded) {
            return paramError("can not create ExtSubDomain with zero Mediators");
        }

        if (0 == timeCastBucketsPerMediatorAdded) {
            return paramError("can not create ExtSubDomain with TimeCastBucketsPerMediator not set");
        }
        if (0 == planResolutionAdded) {
            return paramError("can not create ExtSubDomain with PlanResolution not set");
        }
    }
    if (!addExternalSchemeShard && sharedTxSupportAdded) {
        return paramError("ExtSubDomain without ExternalSchemeShard is useless, use SubDomain");
    }
    if (addExternalSchemeShard && !sharedTxSupportAdded) {
        return paramError("ExtSubDomain without coordinators/mediators is useful for NBS, but not supported yet, use SubDomain");
    }

    // Storage pools check
    TVector<TStoragePool> storagePoolsAdded;
    {
        auto actualPools = TStoragePools(current->GetStoragePools());
        std::sort(actualPools.begin(), actualPools.end());

        auto requestedPools = TStoragePools(input.GetStoragePools().begin(), input.GetStoragePools().end());
        std::sort(requestedPools.begin(), requestedPools.end());

        auto uniqEnd = std::unique(requestedPools.begin(), requestedPools.end());
        if (uniqEnd != requestedPools.end()) {
            return paramError(TStringBuilder() << "requested storage pools are not unique, for example, the pool '" << uniqEnd->GetName() << "' repeats several times");
        }

        {
            TStoragePools omittedPools;
            std::set_difference(
                actualPools.begin(), actualPools.end(),
                requestedPools.begin(), requestedPools.end(),
                std::back_inserter(omittedPools)
            );

            if (omittedPools && requestedPools) {
                return paramError(TStringBuilder() << "deleting storage pools is not allowed, for example, deletion of '" << omittedPools.begin()->GetName() << "' requested");
            }
        }

        // storage pools quotas check
        TString error;
        if (const auto& effectivePools = requestedPools.empty()
                ? actualPools
                : requestedPools;
            !CheckStoragePoolsInQuotas(input.GetDatabaseQuotas(), effectivePools, input.GetName(), error)
        ) {
            return paramError(error);
        }

        std::set_difference(requestedPools.begin(), requestedPools.end(),
                            actualPools.begin(), actualPools.end(),
                            std::back_inserter(storagePoolsAdded));
    }

    // ServerlessComputeResourcesMode check
    bool serverlessComputeResourcesModeChanged = false;
    if (input.HasServerlessComputeResourcesMode()) {
        if (!isServerlessExclusiveDynamicNodesEnabled) {
            return std::make_tuple(NKikimrScheme::EStatus::StatusPreconditionFailed,
                "Unsupported: feature flag EnableServerlessExclusiveDynamicNodes is off"
            );
        }

        switch (input.GetServerlessComputeResourcesMode()) {
            case EServerlessComputeResourcesMode::EServerlessComputeResourcesModeUnspecified:
                return paramError("can not set ServerlessComputeResourcesMode to EServerlessComputeResourcesModeUnspecified");
            case EServerlessComputeResourcesMode::EServerlessComputeResourcesModeExclusive:
            case EServerlessComputeResourcesMode::EServerlessComputeResourcesModeShared:
                break; // ok
            default:
                return paramError("unknown ServerlessComputeResourcesMode");
        }

        const bool isServerless = pathId != current->GetResourcesDomainId();
        if (!isServerless) {
            return paramError("ServerlessComputeResourcesMode can be changed only for serverless");
        }

        serverlessComputeResourcesModeChanged = current->GetServerlessComputeResourcesMode() != input.GetServerlessComputeResourcesMode();
    }

    delta->CoordinatorsAdded = coordinatorsAdded;
    delta->MediatorsAdded = mediatorsAdded;
    delta->TimeCastBucketsPerMediatorAdded = timeCastBucketsPerMediatorAdded;
    delta->AddExternalSchemeShard = addExternalSchemeShard;
    delta->AddExternalHive = addExternalHive;
    delta->AddExternalSysViewProcessor = addExternalSysViewProcessor;
    delta->AddExternalStatisticsAggregator = addExternalStatisticsAggregator;
    delta->AddGraphShard = addGraphShard;
    delta->AddBackupController = addBackupController;
    delta->SharedTxSupportAdded = sharedTxSupportAdded;
    delta->StoragePoolsAdded = std::move(storagePoolsAdded);
    delta->ServerlessComputeResourcesModeChanged = serverlessComputeResourcesModeChanged;

    return {NKikimrScheme::EStatus::StatusAccepted, {}};
}

void VerifyParams(TProposeResponse* result, TParamsDelta* delta, const TPathId pathId,
                  const TSubDomainInfo::TPtr& current, const NKikimrSubDomains::TSubDomainSettings& input,
                  const bool isServerlessExclusiveDynamicNodesEnabled) {
    // TProposeRespose should come in assuming positive outcome (status NKikimrScheme::StatusAccepted, no errors)
    Y_ABORT_UNLESS(result->IsAccepted());
    auto [status, reason] = VerifyParams(delta, pathId, current, input, isServerlessExclusiveDynamicNodesEnabled);
    result->SetStatus(status, reason);
}

void RegisterChanges(const TTxState& txState, const TTxId operationTxId, TOperationContext& context, TPath& path, TSubDomainInfo::TPtr& subdomainInfo, TSubDomainInfo::TPtr& alter) {
    const auto& basenameId = path.Base()->PathId;

    context.MemChanges.GrabPath(context.SS, basenameId);

    // Registering shards is a bit complicated as every shard should be registered
    // in many places:
    //  - in schemeshard.ShardInfo as a "wannabe tablet" (this is done at AddShardsTo())
    //  - in extsubdomain path as a "shard that leaves inside" that path
    //  - in extsubdomain alter as a "private/system part of the subdomain entity"
    for (auto& shard: txState.Shards) {
        auto shardIdx = shard.Idx;

        // Schemeshard local db
        context.DbChanges.PersistShard(shardIdx);

        // Path
        path.DomainInfo()->AddInternalShard(shardIdx);
        path.Base()->IncShardsInside(1);

        // Extsubdomain data
        alter->AddPrivateShard(shardIdx);
    }

    // Path state change
    {
        path.Base()->LastTxId = operationTxId;
        path.Base()->PathState = TPathElement::EPathState::EPathStateAlter;
        context.DbChanges.PersistPath(path.Base()->PathId);
    }

    // Subdomain alter state
    {
        subdomainInfo->SetAlter(alter);
    }
}

class TCreateHive: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TCreateHive, operationId " << OperationId << ", ";
    }

public:
    TCreateHive(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    void SendCreateTabletEvent(const TPathId& pathId, TShardIdx shardIdx, TOperationContext& context) {
        auto path = context.SS->PathsById.at(pathId);

        auto ev = CreateEvCreateTablet(path, shardIdx, context);
        auto rootHiveId = context.SS->GetGlobalHive(context.Ctx);

        LOG_D(DebugHint() << "Send CreateTablet event to Hive: " << rootHiveId << " msg:  "<< ev->Record.DebugString());

        context.OnComplete.BindMsgToPipe(OperationId, rootHiveId, shardIdx, ev.Release());

        context.OnComplete.RouteByShardIdx(OperationId, shardIdx);
    }

    void SendPublishPathRequest(const TPathId& pathId, TOperationContext& context) {
        context.OnComplete.PublishAndWaitPublication(OperationId, pathId);
    }


    bool ProgressState(TOperationContext& context) override {
        LOG_I(DebugHint() << "ProgressState");

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterExtSubDomainCreateHive);
        Y_ABORT_UNLESS(txState->Shards.size() == 1);
        Y_ABORT_UNLESS(txState->Shards.back().TabletType == ETabletType::Hive,
            "expected tablet type HIVE, actual type %s", ETabletType::TypeToStr(txState->Shards.back().TabletType)
        );

        // In the case of schemeshard reboots hive could have already created.
        // If so, operation should skip tablet creation step but still perform publishing step.

        auto shard = txState->Shards.back();

        auto getSubdomainHiveTabletId = [](const TPathId& pathId, TShardIdx shardIdx, TOperationContext& context) {
            auto subdomain = context.SS->SubDomains.at(pathId);
            Y_ABORT_UNLESS(context.SS->ShardInfos.contains(shardIdx));
            auto& shardInfo = context.SS->ShardInfos.at(shardIdx);
            Y_ABORT_UNLESS(shardInfo.TabletType == ETabletType::Hive);
            return shardInfo.TabletID;
        };

        auto subdomainHiveTabletId = getSubdomainHiveTabletId(txState->TargetPathId, shard.Idx, context);

        if (subdomainHiveTabletId == InvalidTabletId) {
            SendCreateTabletEvent(txState->TargetPathId, shard.Idx, context);

        } else {
            LOG_I(DebugHint() << "ProgressState, ExtSubDomain hive already exist, tabletId: " << subdomainHiveTabletId);
            SendPublishPathRequest(txState->TargetPathId, context);
        }

        return false;
    }

    bool HandleReply(TEvHive::TEvCreateTabletReply::TPtr& ev, TOperationContext& context) override {
        LOG_I(DebugHint() << "HandleReply TEvCreateTabletReply");
        LOG_D(DebugHint() << "HandleReply TEvCreateTabletReply, msg: " << DebugReply(ev));

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterExtSubDomainCreateHive);

        const auto& record = ev->Get()->Record;
        auto shardIdx = TShardIdx(record.GetOwner(), TLocalShardIdx(record.GetOwnerIdx()));
        auto createdTabletId = TTabletId(record.GetTabletID());  // global id from hive
        Y_ABORT_UNLESS(createdTabletId != InvalidTabletId);

        NKikimrProto::EReplyStatus status = record.GetStatus();
        Y_VERIFY_S((
                status ==  NKikimrProto::OK
                || status == NKikimrProto::ALREADY
            ),
            "Unexpected status " << NKikimrProto::EReplyStatus_Name(status)
                    << " in CreateTabletReply shard idx " << shardIdx << " tabletId " << createdTabletId
        );

        auto rootHiveId = TTabletId(record.GetOrigin());
        Y_ABORT_UNLESS(rootHiveId == context.SS->GetGlobalHive(context.Ctx));

        TShardInfo& shardInfo = context.SS->ShardInfos.at(shardIdx);

        Y_ABORT_UNLESS(shardInfo.TabletType == ETabletType::Hive);
        Y_ABORT_UNLESS(shardInfo.TabletID == InvalidTabletId || shardInfo.TabletID == createdTabletId);
        Y_ABORT_UNLESS(shardInfo.CurrentTxId == OperationId.GetTxId());

        if (shardInfo.TabletID == InvalidTabletId) {
            context.SS->TabletCounters->Simple()[COUNTER_SUB_DOMAIN_HIVE_COUNT].Add(1);
        }

        shardInfo.TabletID = createdTabletId;
        context.SS->TabletIdToShardIdx[createdTabletId] = shardIdx;

        NIceDb::TNiceDb db(context.GetDB());

        // commit new extsubdomain's hive data to the subdomain, db and memory,
        // (and publish that change to the extsubdomain's path below)
        // so that next stages would get extsubdomain's hive tablet id
        // by requesting on extsubdomain's path
        {
            auto subdomain = context.SS->SubDomains.at(txState->TargetPathId);
            subdomain->AddPrivateShard(shardIdx);
            subdomain->AddInternalShard(shardIdx);

            subdomain->SetTenantHiveIDPrivate(createdTabletId);

            Y_ABORT_UNLESS(subdomain->GetVersion() + 2 == subdomain->GetAlter()->GetVersion());
            subdomain->SetVersion(subdomain->GetVersion() + 1);

            context.SS->PersistSubDomainVersion(db, txState->TargetPathId, *subdomain);
            context.SS->PersistSubDomainPrivateShards(db, txState->TargetPathId, *subdomain);
        }

        context.SS->PersistShardMapping(db, shardIdx, createdTabletId, shardInfo.PathId, OperationId.GetTxId(), ETabletType::Hive);

        context.OnComplete.UnbindMsgFromPipe(OperationId, rootHiveId, shardIdx);
        context.OnComplete.ActivateShardCreated(shardIdx, OperationId.GetTxId());

        LOG_I(DebugHint() << "ExtSubDomain hive created, tabletId " << createdTabletId);

        // no need to configure new hive by a separate EvConfigureHive
        // as new hive is already configured to serve new subdomain at creation
        // (by getting proper ObjectDomain, see ydb/core/mind/hive/tx_create_tablet.cpp)

        // publish new path info to make this transient extsubdomain state
        // (nothing + hive) visible to entire system, or rather to local services
        // that should join extsubdomain resource pool and in order to do so must register
        // to the newly created extsubdomain's hive
        SendPublishPathRequest(txState->TargetPathId, context);

        return false;
    }

    bool HandleReply(TEvPrivate::TEvCompletePublication::TPtr& ev, TOperationContext& context) override {
        LOG_I(DebugHint() << "HandleReply TEvCompletePublication");
        LOG_D(DebugHint() << "HandleReply TEvCompletePublication" << ", msg: " << DebugReply(ev));

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        Y_ABORT_UNLESS(txState->TxType == TTxState::TxAlterExtSubDomainCreateHive);

        Y_ABORT_UNLESS(txState->TargetPathId == ev->Get()->PathId);

        NIceDb::TNiceDb db(context.GetDB());

        // Register barrier to release fellow suboperation waiting on hive creation.
        // This is a sync point with TAlterExtSubDomain.
        context.OnComplete.Barrier(OperationId, "extsubdomain-hive-created");

        return false;
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr& ev, TOperationContext& context) override {
        LOG_I(DebugHint() << "HandleReply TEvPrivate:TEvCompleteBarrier, msg: " << ev->Get()->ToString());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        NIceDb::TNiceDb db(context.GetDB());

        context.SS->ChangeTxState(db, OperationId, TTxState::Propose);

        return true;
    }
};

class TEmptyPropose: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TEmptyPropose, operationId " << OperationId << ", ";
    }

public:
    TEmptyPropose(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_I(DebugHint() << "ProgressState, operation type " << TTxState::TypeName(txState->TxType));

        context.OnComplete.ProposeToCoordinator(OperationId, txState->TargetPathId, TStepId(0));

        return true;
    }
};

class TAlterExtSubDomainCreateHive: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return MakeHolder<TCreateHive>(OperationId);
        case TTxState::Propose:
            return MakeHolder<TEmptyPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId schemeshardTabletId = context.SS->SelfTabletId();
        const NKikimrSubDomains::TSubDomainSettings& inputSettings = Transaction.GetSubDomain();

        TPath path = TPath::Resolve(Transaction.GetWorkingDir(), context.SS).Dive(inputSettings.GetName());

        LOG_I("TAlterExtSubDomainCreateHive Propose"
            << ", opId: " << OperationId
            << ", path: " << path.PathString()
        );

        // No need to check conditions on extsubdomain path: checked in CreateCompatibleAlterExtSubDomain() already

        const auto& basenameId = path.Base()->PathId;

        // Get existing extsubdomain
        Y_ABORT_UNLESS(context.SS->SubDomains.contains(basenameId));
        auto subdomainInfo = context.SS->SubDomains.at(basenameId);
        Y_ABORT_UNLESS(subdomainInfo);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(schemeshardTabletId));
        result->SetPathId(basenameId.LocalPathId);

        // Check params and build change delta
        TParamsDelta delta;
        VerifyParams(result.Get(), &delta, basenameId, subdomainInfo, inputSettings, context.SS->EnableServerlessExclusiveDynamicNodes);
        if (!result->IsAccepted()) {
            return result;
        }

        // No need to check (shard) limits on path: hive goes above any limits

        // This suboperation can't be used as no-op, so check that hive creation is required
        Y_ABORT_UNLESS(delta.AddExternalHive);

        // Generate changes in: operation object, path, schemeshard in-memory object and local db

        // Create in-flight operation object
        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterExtSubDomainCreateHive, basenameId);

        // Create subdomain alter
        TSubDomainInfo::TPtr alter = new TSubDomainInfo(*subdomainInfo, 0, 0, delta.StoragePoolsAdded);

        LOG_D("TAlterExtSubDomainCreateHive Propose"
            << ", opId: " << OperationId
            << ", subdomain ver " << subdomainInfo->GetVersion()
            << ", alter ver " << alter->GetVersion()
        );

        auto guard = context.DbGuard();

        // Create shard for the hive to-be.
        {
            TChannelsBindings channelsBinding;
            if (!context.SS->ResolveSubdomainsChannels(alter->GetStoragePools(), channelsBinding)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "failed to construct channels binding");
                return result;
            }

            AddShardsTo(txState, OperationId.GetTxId(), basenameId, 1, TTabletTypes::Hive, channelsBinding, context.SS);
            Y_ABORT_UNLESS(txState.Shards.size() == 1);
        }

        // Register extsubdomain changes in shards, path, alter
        RegisterChanges(txState, OperationId.GetTxId(), context, path, subdomainInfo, alter);
        //NOTE: alter do not get persisted here, this is intentional

        // Operation in-flight state change
        {
            txState.State = TTxState::CreateParts;
            context.DbChanges.PersistTxState(OperationId);
        }

        context.OnComplete.ActivateTx(OperationId);

        // Set initial operation state
        SetState(NextState());

        return result;
    }

    void AbortPropose(TOperationContext& context) override {
        LOG_N("TAlterExtSubDomainCreateHive AbortPropose"
            << ", opId " << OperationId
        );
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TAlterExtSubDomainCreateHive AbortUnsafe"
            << ", opId: " << OperationId
            << ", forceDropId: " << forceDropTxId
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

class TWaitHiveCreated: public TSubOperationState {
private:
    TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TWaitHiveCreated, operationId " << OperationId << ", ";
    }

public:
    TWaitHiveCreated(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {});
    }

    bool ProgressState(TOperationContext& context) override {
        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_I(DebugHint() << "ProgressState, operation type " << TTxState::TypeName(txState->TxType));

        // Register barrier which this suboperation will wait on.
        // This is a sync point with TAlterExtSubDomainCreateHive suboperation.
        context.OnComplete.Barrier(OperationId, "extsubdomain-hive-created");

        return false;
    }

    bool HandleReply(TEvPrivate::TEvCompleteBarrier::TPtr& ev, TOperationContext& context) override {
        LOG_I(DebugHint() << "HandleReply TEvPrivate:TEvCompleteBarrier, msg: " << ev->Get()->ToString());

        TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        NIceDb::TNiceDb db(context.GetDB());

        context.SS->ChangeTxState(db, OperationId, TTxState::CreateParts);

        return true;
    }
};

class TSyncHive: public TSubOperationState {
private:
    const TOperationId OperationId;

    TString DebugHint() const override {
        return TStringBuilder() << "TSyncHive, operationId " << OperationId << ", ";
    }

public:
    TSyncHive(TOperationId id)
        : OperationId(id)
    {
        IgnoreMessages(DebugHint(), {
            TEvPrivate::TEvOperationPlan::EventType
        });
    }

    bool ProgressState(TOperationContext& context) override {
        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);

        LOG_I(DebugHint() << "ProgressState, NeedSyncHive: " << txState->NeedSyncHive);

        if (txState->NeedSyncHive) {
            const TPathId pathId = txState->TargetPathId;
            Y_ABORT_UNLESS(context.SS->SubDomains.contains(pathId));
            TSubDomainInfo::TConstPtr subDomain = context.SS->SubDomains.at(pathId);

            const TTabletId hiveToSync = context.SS->ResolveHive(pathId, context.Ctx);

            auto event = MakeHolder<TEvHive::TEvUpdateDomain>();
            event->Record.SetTxId(ui64(OperationId.GetTxId()));
            event->Record.MutableDomainKey()->SetSchemeShard(pathId.OwnerId);
            event->Record.MutableDomainKey()->SetPathId(pathId.LocalPathId);
            const auto& serverlessComputeResourcesMode = subDomain->GetServerlessComputeResourcesMode();
            if (serverlessComputeResourcesMode.Defined()) {
                event->Record.SetServerlessComputeResourcesMode(*serverlessComputeResourcesMode);
            }

            LOG_D(DebugHint() << "ProgressState"
                << ", Syncing hive: " << hiveToSync
                << ", msg: {" << event->Record.ShortDebugString() << "}");

            context.OnComplete.BindMsgToPipe(OperationId, hiveToSync, pathId, event.Release());
            return false;
        } else {
            NIceDb::TNiceDb db(context.GetDB());
            context.SS->ChangeTxState(db, OperationId, TTxState::Done);
            return true;
        }
    }

    bool HandleReply(TEvHive::TEvUpdateDomainReply::TPtr& ev, TOperationContext& context) override {
        const TTabletId hive = TTabletId(ev->Get()->Record.GetOrigin()); 

        LOG_I(DebugHint() << "HandleReply TEvUpdateDomainReply"
            << ", from hive: " << hive);

        const TTxState* txState = context.SS->FindTx(OperationId);
        Y_ABORT_UNLESS(txState);
        context.OnComplete.UnbindMsgFromPipe(OperationId, hive, txState->TargetPathId);
        NIceDb::TNiceDb db(context.GetDB());
        context.SS->ChangeTxState(db, OperationId, TTxState::Done);
        return true;
    }
};

class TAlterExtSubDomain: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::Waiting;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch(state) {
        case TTxState::Waiting:
            return TTxState::CreateParts;
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::SyncHive;
        case TTxState::SyncHive:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch(state) {
        case TTxState::Waiting:
            return MakeHolder<TWaitHiveCreated>(OperationId);
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<NSubDomainState::TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<NSubDomainState::TPropose>(OperationId);
        case TTxState::SyncHive:
            return MakeHolder<TSyncHive>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId schemeshardTabletId = context.SS->SelfTabletId();
        const NKikimrSubDomains::TSubDomainSettings& inputSettings = Transaction.GetSubDomain();

        TPath path = TPath::Resolve(Transaction.GetWorkingDir(), context.SS).Dive(inputSettings.GetName());

        LOG_I("TAlterExtSubDomain Propose"
            << ", opId: " << OperationId
            << ", path: " << path.PathString()
        );

        // No need to check conditions on extsubdomain path: checked in CreateCompatibleAlterExtSubDomain() already

        const auto& basenameId = path.Base()->PathId;

        // Get existing extsubdomain
        Y_ABORT_UNLESS(context.SS->SubDomains.contains(basenameId));
        auto subdomainInfo = context.SS->SubDomains.at(basenameId);
        Y_ABORT_UNLESS(subdomainInfo);

        auto result = MakeHolder<TProposeResponse>(NKikimrScheme::StatusAccepted, ui64(OperationId.GetTxId()), ui64(schemeshardTabletId));
        result->SetPathId(basenameId.LocalPathId);

        // Check params and build change delta
        TParamsDelta delta;
        VerifyParams(result.Get(), &delta, basenameId, subdomainInfo, inputSettings, context.SS->EnableServerlessExclusiveDynamicNodes);
        if (!result->IsAccepted()) {
            return result;
        }

        // Count tablets to create

        //NOTE: ExternalHive, ExternalSysViewProcessor and ExternalStatisticsAggregator are _not_ counted against limits
        ui64 tabletsToCreateUnderLimit = delta.AddExternalSchemeShard + delta.CoordinatorsAdded + delta.MediatorsAdded;
        ui64 tabletsToCreateOverLimit = delta.AddExternalSysViewProcessor
            + delta.AddExternalStatisticsAggregator
            + delta.AddGraphShard
            + delta.AddBackupController;
        ui64 tabletsToCreateTotal = tabletsToCreateUnderLimit + tabletsToCreateOverLimit;

        // Check path limits

        {
            TPath::TChecker checks = path.Check();
            checks
                .ShardsLimit(tabletsToCreateUnderLimit)
                .PathShardsLimit(tabletsToCreateUnderLimit);

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        // Generate changes in: operation object, path, schemeshard in-memory object and local db

        // Create in-flight operation object
        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterExtSubDomain, basenameId);

        // Create or derive alter.
        // (We could have always created new alter from a current subdomainInfo but
        // we need to take into account possible version increase from CreateHive suboperation.)
        auto createAlterFrom = [&inputSettings, &delta](auto prototype) {
            return MakeIntrusive<TSubDomainInfo>(
                *prototype,
                inputSettings.GetPlanResolution(),
                inputSettings.GetTimeCastBucketsPerMediator(),
                delta.StoragePoolsAdded
            );
        };
        TSubDomainInfo::TPtr alter = [&delta, &subdomainInfo, &createAlterFrom, &context]() {
            if (delta.AddExternalHive && context.SS->EnableAlterDatabaseCreateHiveFirst) {
                Y_ABORT_UNLESS(subdomainInfo->GetAlter());
                return createAlterFrom(subdomainInfo->GetAlter());
            } else {
                Y_ABORT_UNLESS(!subdomainInfo->GetAlter());
                return createAlterFrom(subdomainInfo);
            }
        }();

        if (inputSettings.HasDeclaredSchemeQuotas()) {
            alter->SetDeclaredSchemeQuotas(inputSettings.GetDeclaredSchemeQuotas());
        }
        if (inputSettings.HasDatabaseQuotas()) {
            alter->SetDatabaseQuotas(inputSettings.GetDatabaseQuotas());
        }

        if (const auto& auditSettings = subdomainInfo->GetAuditSettings()) {
            alter->SetAuditSettings(*auditSettings);
        }
        if (inputSettings.HasAuditSettings()) {
            alter->ApplyAuditSettings(inputSettings.GetAuditSettings());
        }

        if (inputSettings.HasServerlessComputeResourcesMode()) {
            alter->SetServerlessComputeResourcesMode(inputSettings.GetServerlessComputeResourcesMode());
        }

        LOG_D("TAlterExtSubDomain Propose"
            << ", opId: " << OperationId
            << ", subdomain ver " << subdomainInfo->GetVersion()
            << ", alter ver " << alter->GetVersion()
        );

        auto guard = context.DbGuard();

        // Create shards for the requested tablets (except hive)
        {
            TChannelsBindings channelsBinding;
            if (delta.SharedTxSupportAdded ||
                delta.AddExternalSchemeShard ||
                delta.AddExternalSysViewProcessor ||
                delta.AddExternalHive ||
                delta.AddExternalStatisticsAggregator ||
                delta.AddGraphShard ||
                delta.AddBackupController)
            {
                if (!context.SS->ResolveSubdomainsChannels(alter->GetStoragePools(), channelsBinding)) {
                    result->SetError(NKikimrScheme::StatusInvalidParameter, "failed to construct channels binding");
                    return result;
                }
            }

            // Declare shards.
            // - hive always comes first (OwnerIdx 1)
            // - schemeshard always comes second (OwnerIdx 2)
            // - others follow
            //
            if (delta.AddExternalHive && !context.SS->EnableAlterDatabaseCreateHiveFirst) {
                AddShardsTo(txState, OperationId.GetTxId(), basenameId, 1, TTabletTypes::Hive, channelsBinding, context.SS);
                ++tabletsToCreateTotal;
            }
            if (delta.AddExternalSchemeShard) {
                AddShardsTo(txState, OperationId.GetTxId(), basenameId, 1, TTabletTypes::SchemeShard, channelsBinding, context.SS);
            }
            if (delta.SharedTxSupportAdded) {
                AddShardsTo(txState, OperationId.GetTxId(), basenameId, delta.CoordinatorsAdded, TTabletTypes::Coordinator, channelsBinding, context.SS);
                AddShardsTo(txState, OperationId.GetTxId(), basenameId, delta.MediatorsAdded, TTabletTypes::Mediator, channelsBinding, context.SS);
            }
            if (delta.AddExternalSysViewProcessor) {
                AddShardsTo(txState, OperationId.GetTxId(), basenameId, 1, TTabletTypes::SysViewProcessor, channelsBinding, context.SS);
            }
            if (delta.AddExternalStatisticsAggregator) {
                AddShardsTo(txState, OperationId.GetTxId(), basenameId, 1, TTabletTypes::StatisticsAggregator, channelsBinding, context.SS);
            }
            if (delta.AddGraphShard) {
                AddShardsTo(txState, OperationId.GetTxId(), basenameId, 1, TTabletTypes::GraphShard, channelsBinding, context.SS);
            }
            if (delta.AddBackupController) {
                AddShardsTo(txState, OperationId.GetTxId(), basenameId, 1, TTabletTypes::BackupController, channelsBinding, context.SS);
            }
            Y_ABORT_UNLESS(txState.Shards.size() == tabletsToCreateTotal);
        }

        // Register extsubdomain changes in shards, path, alter
        RegisterChanges(txState, OperationId.GetTxId(), context, path, subdomainInfo, alter);
        // Persist alter
        context.DbChanges.PersistSubDomainAlter(basenameId);

        // Operation in-flight state change
        {
            // txState.State = TTxState::CreateParts;
            txState.State = TTxState::Waiting;
            txState.NeedSyncHive = delta.ServerlessComputeResourcesModeChanged;
            context.DbChanges.PersistTxState(OperationId);
        }

        context.OnComplete.ActivateTx(OperationId);

        // Set initial operation state
        SetState(NextState());

        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterExtSubDomain");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_N("TAlterExtSubDomain AbortUnsafe"
            << ", opId: " << OperationId
            << ", forceDropId: " << forceDropTxId
        );

        context.OnComplete.DoneOperation(OperationId);
    }
};

} // anonymous namespace

ISubOperation::TPtr CreateAlterExtSubDomainCreateHive(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterExtSubDomainCreateHive>(id, tx);
}

ISubOperation::TPtr CreateAlterExtSubDomainCreateHive(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterExtSubDomainCreateHive>(id, state);
}

ISubOperation::TPtr CreateAlterExtSubDomain(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterExtSubDomain>(id, tx);
}

ISubOperation::TPtr CreateAlterExtSubDomain(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterExtSubDomain>(id, state);
}

TVector<ISubOperation::TPtr> CreateCompatibleAlterExtSubDomain(TOperationId id, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterExtSubDomain);

    LOG_I("CreateCompatibleAlterExtSubDomain, opId " << id
        << ", feature flag EnableAlterDatabaseCreateHiveFirst " << context.SS->EnableAlterDatabaseCreateHiveFirst
        << ", tx " << tx.ShortDebugString()
    );

    const TString& parentPathStr = tx.GetWorkingDir();
    const auto& inputSettings = tx.GetSubDomain();
    const TString& name = inputSettings.GetName();

    LOG_I("CreateCompatibleAlterExtSubDomain, opId " << id << ", path " << parentPathStr << "/" << name);

    auto errorResult = [&id](NKikimrScheme::EStatus status, const TStringBuf& msg) -> TVector<ISubOperation::TPtr> {
        return {CreateReject(id, status, TStringBuilder() << "Invalid AlterExtSubDomain request: " << msg)};
    };

    if (!parentPathStr) {
        return errorResult(NKikimrScheme::StatusInvalidParameter, "no working dir");
    }
    if (!name) {
        return errorResult(NKikimrScheme::StatusInvalidParameter, "no name");
    }

    TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);

    // check extsubdomain path and its condition
    {
        TPath::TChecker checks = path.Check();
        checks
            .NotEmpty()
            .NotUnderDomainUpgrade()
            .IsAtLocalSchemeShard()
            .IsResolved()
            .NotDeleted()
            .IsExternalSubDomain()
            .NotUnderOperation()
            .IsCommonSensePath();  // dirname consist of directories and subdomain roots (and olapstores!!)

        if (!checks) {
            return errorResult(checks.GetStatus(), checks.GetError());
        }
    }

    // check if extsubdomain is already being altered
    //NOTE: (didn't TChecker::NotUnderOperation() checked that already?)
    const auto& basenameId = path.Base()->PathId;

    Y_ABORT_UNLESS(context.SS->SubDomains.contains(basenameId));
    auto subdomainInfo = context.SS->SubDomains.at(basenameId);
    Y_ABORT_UNLESS(subdomainInfo);

    if (subdomainInfo->GetAlter()) {
        return errorResult(NKikimrScheme::StatusMultipleModifications, "extsubdomain is under another alter operation");
    }

    // check operation condition and limits
    {
        TString explain;
        if (!context.SS->CheckApplyIf(tx, explain)) {
            return errorResult(NKikimrScheme::StatusPreconditionFailed, explain);
        }
    }

    // Check params and build change delta
    TParamsDelta delta;
    {
        auto [status, reason] = VerifyParams(&delta, basenameId, subdomainInfo, inputSettings, context.SS->EnableServerlessExclusiveDynamicNodes);
        if (status != NKikimrScheme::EStatus::StatusAccepted) {
            return errorResult(status, reason);
        }
    }

    // create suboperations
    TVector<ISubOperation::TPtr> result;

    if (delta.AddExternalHive && context.SS->EnableAlterDatabaseCreateHiveFirst) {
        auto msg = TransactionTemplate(parentPathStr, NKikimrSchemeOp::ESchemeOpAlterExtSubDomainCreateHive);
        msg.MutableSubDomain()->CopyFrom(inputSettings);

        result.push_back(CreateAlterExtSubDomainCreateHive(NextPartId(id, result), msg));
    }
    {
        auto msg = TransactionTemplate(parentPathStr, NKikimrSchemeOp::ESchemeOpAlterExtSubDomain);
        msg.MutableSubDomain()->CopyFrom(inputSettings);

        result.push_back(CreateAlterExtSubDomain(NextPartId(id, result), msg));
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
