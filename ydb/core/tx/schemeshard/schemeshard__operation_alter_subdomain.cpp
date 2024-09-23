#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common_subdomain.h"
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
        Y_ABORT_UNLESS(shard.Operation == TTxState::ETxState::CreateParts);
        Y_ABORT_UNLESS(ss->ShardInfos.contains(shard.Idx), "shard info is set before");
        auto& shardInfo = ss->ShardInfos[shard.Idx];
        ss->PersistShardMapping(db, shard.Idx, InvalidTabletId, shardInfo.PathId, shardInfo.CurrentTxId, shardInfo.TabletType);
        ss->PersistChannelsBinding(db, shard.Idx, shardInfo.BindedChannels);
    }
    Y_ABORT_UNLESS(shardsToCreate == txState.Shards.size());
}

class TAlterSubDomain: public TSubOperation {
    static TTxState::ETxState NextState() {
        return TTxState::CreateParts;
    }

    TTxState::ETxState NextState(TTxState::ETxState state) const override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return TTxState::ConfigureParts;
        case TTxState::ConfigureParts:
            return TTxState::Propose;
        case TTxState::Propose:
            return TTxState::Done;
        default:
            return TTxState::Invalid;
        }
    }

    TSubOperationState::TPtr SelectStateFunc(TTxState::ETxState state) override {
        switch (state) {
        case TTxState::Waiting:
        case TTxState::CreateParts:
            return MakeHolder<TCreateParts>(OperationId);
        case TTxState::ConfigureParts:
            return MakeHolder<NSubDomainState::TConfigureParts>(OperationId);
        case TTxState::Propose:
            return MakeHolder<NSubDomainState::TPropose>(OperationId);
        case TTxState::Done:
            return MakeHolder<TDone>(OperationId);
        default:
            return nullptr;
        }
    }

public:
    using TSubOperation::TSubOperation;

    THolder<TProposeResponse> Propose(const TString&, TOperationContext& context) override {
        const TTabletId ssId = context.SS->SelfTabletId();

        const auto& settings = Transaction.GetSubDomain();

        const TString& parentPathStr = Transaction.GetWorkingDir();
        const TString& name = settings.GetName();

        LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "TAlterSubDomain Propose"
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
                .IsSubDomain()
                .NotUnderOperation()
                .IsCommonSensePath();

            if (!checks) {
                result->SetError(checks.GetStatus(), checks.GetError());
                return result;
            }
        }

        TPathElement::TPtr subDomain = path.Base();

        Y_ABORT_UNLESS(context.SS->SubDomains.contains(subDomain->PathId));
        auto subDomainInfo = context.SS->SubDomains.at(subDomain->PathId);
        Y_ABORT_UNLESS(subDomainInfo);

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
                            "Malformed subdomain request: unable to change PlanResolution at root, only addition storage pools is allowed");
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
                            "Malformed subdomain request: unable to change TimeCastBucketsPerMediator at root, only addition storage pools is allowed");
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
        const ui64 shardsToCreate = settings.GetCoordinators() + settings.GetMediators();

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


            {
                TPath::TChecker checks = path.Check();
                checks
                    .ShardsLimit(shardsToCreate)
                    .PathShardsLimit(shardsToCreate);

                if (!checks) {
                    result->SetError(checks.GetStatus(), checks.GetError());
                    return result;
                }
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
                "Malformed subdomain request: requested storage pools is not unique, for example, the pool '" + uniqEnd->GetName() +"' repeats several times");
            return result;
        }

        {
            TVector<TStoragePool> omittedPools;
            std::set_difference(actualPools.begin(), actualPools.end(),
                                requestedPools.begin(), requestedPools.end(),
                                std::back_inserter(omittedPools));

            if (omittedPools && requestedPools) {
                result->SetError(
                    NKikimrScheme::StatusInvalidParameter,
                    "Malformed subdomain request: deleting storage pool is not allowed, for example, requested deletion '" + omittedPools.begin()->GetName() +"'");
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
        if (wasSharedTxSupported || setSupportSharedTx) {
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

        if (settings.HasDeclaredSchemeQuotas()) {
            alterData->SetDeclaredSchemeQuotas(settings.GetDeclaredSchemeQuotas());
        }

        if (settings.HasDatabaseQuotas()) {
            if (const auto& effectivePools = requestedPools.empty()
                    ? actualPools
                    : requestedPools;
                !CheckStoragePoolsInQuotas(settings.GetDatabaseQuotas(), effectivePools, path.PathString(), errStr)
            ) {
                result->SetError(NKikimrScheme::StatusInvalidParameter, errStr);
                return result;
            }
            alterData->SetDatabaseQuotas(settings.GetDatabaseQuotas());
        }

        if (const auto& auditSettings = subDomainInfo->GetAuditSettings()) {
            alterData->SetAuditSettings(*auditSettings);
        }
        if (settings.HasAuditSettings()) {
            alterData->ApplyAuditSettings(settings.GetAuditSettings());
        }

        NIceDb::TNiceDb db(context.GetDB());

        subDomain->LastTxId = OperationId.GetTxId();
        subDomain->PathState = TPathElement::EPathState::EPathStateAlter;
        context.SS->PersistPath(db, subDomain->PathId);

        Y_ABORT_UNLESS(!context.SS->FindTx(OperationId));
        TTxState& txState = context.SS->CreateTx(OperationId, TTxState::TxAlterSubDomain, subDomain->PathId);
        txState.State = TTxState::CreateParts;

        if (!wasSharedTxSupported && setSupportSharedTx) {
            DeclareShards(txState, OperationId.GetTxId(), subDomain->PathId, settings.GetCoordinators(), TTabletTypes::Coordinator, channelBindings, context.SS);
            DeclareShards(txState, OperationId.GetTxId(), subDomain->PathId, settings.GetMediators(), TTabletTypes::Mediator, channelBindings, context.SS);

            for (auto& shard: txState.Shards) {
                alterData->AddPrivateShard(shard.Idx);
            }

            PersistShards(db, txState, shardsToCreate, context.SS);
            context.SS->PersistUpdateNextShardIdx(db);
        }
        subDomainInfo->SetAlter(alterData);
        context.SS->PersistSubDomainAlter(db, subDomain->PathId, *alterData);

        context.SS->PersistTxState(db, OperationId);
        context.OnComplete.ActivateTx(OperationId);

        path.DomainInfo()->AddInternalShards(txState);
        path.Base()->IncShardsInside(shardsToCreate);

        SetState(NextState());
        return result;
    }

    void AbortPropose(TOperationContext&) override {
        Y_ABORT("no AbortPropose for TAlterSubDomain");
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

namespace NKikimr::NSchemeShard {

ISubOperation::TPtr CreateAlterSubDomain(TOperationId id, const TTxTransaction& tx) {
    return MakeSubOperation<TAlterSubDomain>(id, tx);
}

ISubOperation::TPtr CreateAlterSubDomain(TOperationId id, TTxState::ETxState state) {
    Y_ABORT_UNLESS(state != TTxState::Invalid);
    return MakeSubOperation<TAlterSubDomain>(id, state);
}

}
