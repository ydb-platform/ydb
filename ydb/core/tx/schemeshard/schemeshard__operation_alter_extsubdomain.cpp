#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/persqueue/config/config.h>

namespace {


using namespace NKikimr;
using namespace NSchemeShard;

void DeclareShards(TTxState& txState, TTxId txId, TPathId pathId,
                   ui32 count, TTabletTypes::EType type,
                   const TChannelsBindings& channelsBindings,
                   TSchemeShard* ss)
{
    txState.Shards.reserve(count);
    for (ui64 i = 0; i < count; ++i) {
        auto shardId = ss->RegisterShardInfo(
            TShardInfo(txId, pathId, type)
                .WithBindedChannels(channelsBindings));
        txState.Shards.emplace_back(shardId, type, TTxState::CreateParts);
    }
}

void PersistShards(NIceDb::TNiceDb& db, TTxState& txState, ui64 shardsToCreate, TSchemeShard* ss) {
    for (const auto& shard : txState.Shards) {
        Y_VERIFY(shard.Operation == TTxState::ETxState::CreateParts);
        Y_VERIFY(ss->ShardInfos.contains(shard.Idx), "shard info is set before");
        auto& shardInfo = ss->ShardInfos[shard.Idx];
        ss->PersistShardMapping(db, shard.Idx, InvalidTabletId, shardInfo.PathId, shardInfo.CurrentTxId, shardInfo.TabletType);
        ss->PersistChannelsBinding(db, shard.Idx, shardInfo.BindedChannels);
    }
    Y_VERIFY(shardsToCreate == txState.Shards.size());
}

class TAlterExtSubDomain: public TSubOperation {
    const TOperationId OperationId;
    const TTxTransaction Transaction;
    TTxState::ETxState State = TTxState::Invalid;

    TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose; // DONE ???
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
        return TTxState::Invalid;
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) {
        switch(state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return THolder(new TCreateParts(OperationId));
        case TTxState::ConfigureParts:
            return THolder(new NSubDomainState::TConfigureParts(OperationId));
        case TTxState::Propose:
            return THolder(new NSubDomainState::TPropose(OperationId));
        case TTxState::Done:
            return THolder(new TDone(OperationId));
        default:
            return nullptr;
        }
    }

    void StateDone(TOperationContext& context) override {
        State = NextState(State);

        if (State != TTxState::Invalid) {
            SetState(SelectStateFunc(State));
            context.OnComplete.ActivateTx(OperationId);
        }
    }

public:
    TAlterExtSubDomain(TOperationId id, const TTxTransaction& tx)
        : OperationId(id)
        , Transaction(tx)
    {
    }

    TAlterExtSubDomain(TOperationId id, TTxState::ETxState state)
        : OperationId(id)
          , State(state)
    {
        SetState(SelectStateFunc(state));
    }

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& settings = Transaction.GetSubDomain();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = settings.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterExtSubDomain Propose"
                         << ", path: " << parentPathStr << "/" << name
                         << ", opId: " << OperationId
                         << ", at schemeshard: " << ssId);

        TEvSchemeShard::EStatus status = NKikimrScheme::StatusAccepted;
        auto result = MakeHolder<TProposeResponse>(status, ui64(OperationId.GetTxId()), ui64(ssId));

        if (!parentPathStr) {
            result->SetError(NKikimrScheme::StatusInvalidParameter,
                             "Malformed subdomain request: no working dir");
            return result;
        }

        if (!name) {
            result->SetError(
                NKikimrScheme::StatusInvalidParameter,
                "Malformed subdomain request: no name");
            return result;
        }

        TPath path = TPath::Resolve(parentPathStr, context.SS).Dive(name);

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
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPathElement::TPtr subDomain = path.Base();

        Y_VERIFY(context.SS->SubDomains.contains(subDomain->PathId));
        auto subDomainInfo = context.SS->SubDomains.at(subDomain->PathId);
        Y_VERIFY(subDomainInfo);

        if (subDomainInfo->GetAlter()) {
            result->SetError(NKikimrScheme::StatusPathDoesNotExist, "SubDomain is under another alter 2");
            return result;
        }

        result->SetPathId(subDomain->PathId.LocalPathId);

        if (0 != settings.GetPlanResolution()) {
            if (subDomainInfo->GetPlanResolution() != 0 && subDomainInfo->GetPlanResolution() != settings.GetPlanResolution()) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: unable to change PlanResolution, only set it up");
                return result;
            }
            if (subDomain->IsRoot()) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: unable to change PlanResolution at root, only additiong storage pools is allowed");
                return result;
            }
        }

        if (0 != settings.GetTimeCastBucketsPerMediator()) {
            if (subDomainInfo->GetTCB() != 0 && subDomainInfo->GetTCB() != settings.GetTimeCastBucketsPerMediator()) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: unable to change TimeCastBucketsPerMediator, only set it up");
                return result;
            }
            if (subDomain->IsRoot()) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: unable to change TimeCastBucketsPerMediator at root, only additiong storage pools is allowed");
                return result;
            }
        }

        if (0 == settings.GetCoordinators() && 0 != settings.GetMediators()) {
            result->SetError(
                NKikimrScheme::StatusInvalidParameter,
                "Malformed subdomain request: cant create subdomain with mediators, but no coordinators");
            return result;
        }

        if (0 != settings.GetCoordinators() && 0 == settings.GetMediators()) {
            result->SetError(
                NKikimrScheme::StatusInvalidParameter,
                "Malformed subdomain request: cant create subdomain with coordinators, but no mediators");
            return result;
        }

        const bool wasSharedTxSupported = subDomainInfo->IsSupportTransactions();
        const bool setSupportSharedTx = bool(settings.GetCoordinators()) || bool(settings.GetMediators());

        const bool wasExternalSchemeShard = bool(subDomainInfo->GetTenantSchemeShardID());
        const bool setExternalSchemeShard = settings.HasExternalSchemeShard();
        const bool addExternalSchemeShard = !wasExternalSchemeShard && setExternalSchemeShard && settings.GetExternalSchemeShard();

        const bool wasExternalHive = bool(subDomainInfo->GetTenantHiveID());
        const bool setExternalHive = settings.HasExternalHive();
        const bool addExternalHive = !wasExternalHive && setExternalHive && settings.GetExternalHive();

        const bool wasViewProcessors = bool(subDomainInfo->GetTenantSysViewProcessorID());
        const bool setViewProcessors = settings.HasExternalSysViewProcessor();
        const bool addViewProcessors = !wasViewProcessors && setViewProcessors && settings.GetExternalSysViewProcessor();

        ui64 shardsToCreate = 0;
        ui64 allowOverTheLimitShards = 0;

        if (wasExternalSchemeShard && setExternalSchemeShard) {
            if (bool(subDomainInfo->GetTenantSchemeShardID()) != settings.GetExternalSchemeShard()) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: unable to change ExternalSchemeShard, only set it up");
                return result;
            }
        }

        if (addExternalSchemeShard) {
            shardsToCreate += 1;

            if (!wasSharedTxSupported && !setSupportSharedTx) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: ExtSubdomain without coordinators/mediators is usefull for NBS, but not supported yet, use SubDomain");
                return result;
            }
        }

        if (wasExternalHive && setExternalHive) {
            if (bool(subDomainInfo->GetTenantHiveID()) != settings.GetExternalHive()) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: unable to change ExternalHive, only set it up");
                return result;
            }
        }

        if (addExternalHive) {
            shardsToCreate += 1;
            allowOverTheLimitShards += 1;
        }

        if (wasViewProcessors && setViewProcessors) {
            if (bool(subDomainInfo->GetTenantSysViewProcessorID()) != settings.GetExternalSysViewProcessor()) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: unable to change ViewProcessors, only set it up");
                return result;
            }
        }

        if (addViewProcessors) {
            shardsToCreate += 1;
            allowOverTheLimitShards += 1;
        }

        if (wasSharedTxSupported && setSupportSharedTx) {
            if (subDomainInfo->GetProcessingParams().CoordinatorsSize() != settings.GetCoordinators()) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: unable to change Coordinators count, only set it up");
                return result;
            }

            if (subDomainInfo->GetProcessingParams().MediatorsSize() != settings.GetMediators()) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: unable to change Mediators count, only set it up");
                return result;
            }
        }

        if (!wasSharedTxSupported && setSupportSharedTx) {
            shardsToCreate += settings.GetCoordinators() + settings.GetMediators();

            if (settings.GetTimeCastBucketsPerMediator() == 0) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: TimeCastBucketsPerMediator should be set when coordinators create");
                return result;
            }

            if (settings.GetPlanResolution() == 0) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: PlanResolution should be set when coordinators create");
                return result;
            }

            if (!wasExternalSchemeShard && !setExternalSchemeShard) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: ExtSubdomain without External SchemeShard is useless, use SubDomain");
                return result;
            }
        }

        {
            TPath::TChecker checks = path.Check();
            checks
                .ShardsLimit(shardsToCreate - allowOverTheLimitShards)
                .PathShardsLimit(shardsToCreate - allowOverTheLimitShards);

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        auto actualPools = TStoragePools(subDomainInfo->GetStoragePools());
        std::sort(actualPools.begin(), actualPools.end());

        auto requestedPools = TVector<TStoragePool>(settings.GetStoragePools().begin(), settings.GetStoragePools().end());
        std::sort(requestedPools.begin(), requestedPools.end());

        auto uniqEnd = std::unique(requestedPools.begin(), requestedPools.end());
        if (uniqEnd != requestedPools.end()) {
            result->SetError(
                NKikimrScheme::StatusInvalidParameter,
                "Malformed subdomain request: requested storage pools is not qunique, for example, the pool '" + uniqEnd->GetName() +"' repeats several times");
            return result;
        }

        {
            TVector<TStoragePool> omitedPools;
            std::set_difference(actualPools.begin(), actualPools.end(),
                                requestedPools.begin(), requestedPools.end(),
                                std::back_inserter(omitedPools));

            if (omitedPools && requestedPools) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: deleting storage pool is not allowed, for example, requested deletion '" + omitedPools.begin()->GetName() +"'");
                return result;
            }
        }

        TVector<TStoragePool> addedPools;
        std::set_difference(requestedPools.begin(), requestedPools.end(),
                            actualPools.begin(), actualPools.end(),
                            std::back_inserter(addedPools));

        TSubDomainInfo::TPtr alterData = new TSubDomainInfo(*subDomainInfo,
                                                            settings.GetPlanResolution(),
                                                            settings.GetTimeCastBucketsPerMediator(),
                                                            addedPools);

        TChannelsBindings channelBindings;
        if (setSupportSharedTx || setExternalSchemeShard || setExternalHive || setViewProcessors) {
            if (!context.SS->ResolveSubdomainsChannels(alterData->GetStoragePools(), channelBindings)) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, "Unable construct channels binding");
                return result;
            }
        }

        TString errStr;
        if (!context.SS->CheckApplyIf(Transaction, errStr)) {
            result->SetError(NKikimrScheme::StatusPreconditionFailed, errStr);
            return result;
        }
        if (!context.SS->CheckInFlightLimit(TTxState::TxAlterExtSubDomain, errStr))
        {
            result->SetError(NKikimrScheme::StatusResourceExhausted, errStr);
            return result;
        }

        if (settings.HasDeclaredSchemeQuotas()) {
            alterData->SetDeclaredSchemeQuotas(settings.GetDeclaredSchemeQuotas());
        }

        if (settings.HasDatabaseQuotas()) {
            alterData->SetDatabaseQuotas(settings.GetDatabaseQuotas());
        }

        NIceDb::TNiceDb db(context.GetDB());

        subDomain->LastTxId = OperationId.GetTxId();
        subDomain->PathState = TPathElement::EPathState::EPathStateAlter;
        context.SS->PersistPath(db, subDomain->PathId);

        Y_VERIFY(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterExtSubDomain, subDomain->PathId);
        txState.State = TTxState::CreateParts;

        if (!wasSharedTxSupported && setSupportSharedTx) {
            DeclareShards(txState, OperationId.GetTxId(), subDomain->PathId, settings.GetCoordinators(), TTabletTypes::Coordinator, channelBindings, context.SS);
            DeclareShards(txState, OperationId.GetTxId(), subDomain->PathId, settings.GetMediators(), TTabletTypes::Mediator, channelBindings, context.SS);
        }

        if (addExternalSchemeShard) {
            DeclareShards(txState, OperationId.GetTxId(), subDomain->PathId, 1, TTabletTypes::SchemeShard, channelBindings, context.SS);
        }

        if (addExternalHive) {
            DeclareShards(txState, OperationId.GetTxId(), subDomain->PathId, 1, TTabletTypes::Hive, channelBindings, context.SS);
        } else if (!alterData->GetSharedHive()) {
            alterData->SetSharedHive(context.SS->GetGlobalHive(context.Ctx));
        }

        if (addViewProcessors) {
            DeclareShards(txState, OperationId.GetTxId(), subDomain->PathId, 1, TTabletTypes::SysViewProcessor, channelBindings, context.SS);
        }

        for (auto& shard: txState.Shards) {
            alterData->AddPrivateShard(shard.Idx);
        }

        PersistShards(db, txState, shardsToCreate, context.SS);
        context.SS->PersistUpdateNextShardIdx(db);

        subDomainInfo->SetAlter(alterData);
        context.SS->PersistSubDomainAlter(db, subDomain->PathId, *alterData);

        context.SS->PersistTxState(db, OperationId);
        context.OnComplete.ActivateTx(OperationId);

        path.DomainInfo()->AddInternalShards(txState);
        path.Base()->IncShardsInside(shardsToCreate);

        State = NextState();
        SetState(SelectStateFunc(State));
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_FAIL("no AbortPropose for TAlterSubDomain");
    }

    void AbortUnsafe(TTxId forceDropTxId, TOperationContext& context) override {
        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterSubDomain AbortUnsafe"
                         << ", opId: " << OperationId
                         << ", forceDropId: " << forceDropTxId
                         << ", at schemeshard: " << context.SS->TabletID());

        context.OnComplete.DoneOperation(OperationId);
    }
};

}

namespace NKikimr {
namespace NSchemeShard {

ISubOperationBase::TPtr CreateAlterExtSubDomain(TOperationId id, const TTxTransaction& tx) {
    return new TAlterExtSubDomain(id, tx);
}

ISubOperationBase::TPtr CreateAlterExtSubDomain(TOperationId id, TTxState::ETxState state) {
    Y_VERIFY(state != TTxState::Invalid);
    return new TAlterExtSubDomain(id, state);
}

}
}
