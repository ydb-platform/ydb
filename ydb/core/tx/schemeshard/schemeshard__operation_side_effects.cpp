#include "schemeshard__operation_side_effects.h"
#include "schemeshard__operation_db_changes.h"
#include "schemeshard__operation_memory_changes.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/tx_processing.h>

namespace NKikimr {
namespace NSchemeShard {

void TSideEffects::ProposeToCoordinator(TOperationId opId, TPathId pathId, TStepId minStep) {
    CoordinatorProposes.push_back(TProposeRec(opId, pathId, minStep));
}

void TSideEffects::CoordinatorAck(TActorId coordinator, TStepId stepId, TTxId txId) {
    CoordinatorAcks.push_back(TCoordinatorAck(coordinator, stepId, txId));
}

void TSideEffects::MediatorAck(TActorId mediator, TStepId stepId) {
    MediatorAcks.push_back(TMediatorAck(mediator, stepId));
}

void TSideEffects::UpdateTenant(TPathId pathId) {
    TenantsToUpdate.insert(pathId);
}

void TSideEffects::UpdateTenants(THashSet<TPathId>&& pathIds) {
    if (TenantsToUpdate.empty()) {
        TenantsToUpdate = std::move(pathIds);
    } else {
        TenantsToUpdate.insert(pathIds.begin(), pathIds.end());
    }
}

void TSideEffects::Send(TActorId dst, IEventBase* message, ui64 cookie, ui32 flags) {
    Messages.push_back(TSendRec(dst, message, cookie, flags));
}

void TSideEffects::BindMsgToPipe(TOperationId opId, TTabletId dst, TPipeMessageId cookie, TAutoPtr<IEventBase> message) {
    BindedMessages.push_back(TBindMsgRec(opId, dst, cookie, message));
    AttachOperationToPipe(opId, dst);
}

void TSideEffects::BindMsgToPipe(TOperationId opId, TTabletId dst, TShardIdx shardIdx, TAutoPtr<IEventBase> message) {
    BindMsgToPipe(opId, dst, TPipeMessageId(shardIdx.GetOwnerId(), shardIdx.GetLocalId()), message);
}

void TSideEffects::BindMsgToPipe(TOperationId opId, TTabletId dst, TPathId pathId, TAutoPtr<IEventBase> message) {
    BindMsgToPipe(opId, dst, TPipeMessageId(pathId.OwnerId, pathId.LocalPathId), message);
}

void TSideEffects::UnbindMsgFromPipe(TOperationId opId, TTabletId dst, TPipeMessageId cookie) {
    BindedMessageAcks.push_back(TBindMsgAck(opId, dst, cookie));
}

void  TSideEffects::UpdateTempDirsToMakeState(const TActorId& ownerActorId, const TPathId& pathId) {
    auto it = TempDirsToMakeState.find(ownerActorId);
    if (it == TempDirsToMakeState.end()) {
        TempDirsToMakeState[ownerActorId] = { pathId };
    } else {
        it->second.push_back(pathId);
    }
}

void  TSideEffects::UpdateTempDirsToRemoveState(const TActorId& ownerActorId, const TPathId& pathId) {
    auto it = TempDirsToRemoveState.find(ownerActorId);
    if (it == TempDirsToRemoveState.end()) {
        TempDirsToRemoveState[ownerActorId] = { pathId };
    } else {
        it->second.push_back(pathId);
    }
}

void TSideEffects::RouteByTabletsFromOperation(TOperationId opId) {
    RelationsByTabletsFromOperation.push_back(opId);
}

void TSideEffects::UnbindMsgFromPipe(TOperationId opId, TTabletId dst, TPathId pathId) {
    UnbindMsgFromPipe(opId, dst, TPipeMessageId(pathId.OwnerId, pathId.LocalPathId));
}

void TSideEffects::UnbindMsgFromPipe(TOperationId opId, TTabletId dst, TShardIdx shardIdx) {
    UnbindMsgFromPipe(opId, dst, TPipeMessageId(shardIdx.GetOwnerId(), shardIdx.GetLocalId()));
}

void TSideEffects::AttachOperationToPipe(TOperationId opId, TTabletId dst) {
    PendingPipeTrackerCommands.AttachTablet(ui64(opId.GetTxId()), ui64(dst), opId.GetSubTxId());
    RouteByTablet(opId, dst);
}

void TSideEffects::RouteByShardIdx(TOperationId opId, TShardIdx shardIdx) {
    RelationsByShardIdx.push_back(TRelationByShardIdx(opId, shardIdx));
}

void TSideEffects::ReleasePathState(TOperationId opId, TPathId pathId, NKikimrSchemeOp::EPathState state) {
    ReleasePathStateRecs.push_back(TPathStateRec(opId, pathId, state));
}

void TSideEffects::RouteByTablet(TOperationId opId, TTabletId tabletId) {
    RelationsByTabletId.push_back(TRelationByTabletId(opId, tabletId));
}

void TSideEffects::DetachOperationFromPipe(TOperationId opId, TTabletId dst) {
    PendingPipeTrackerCommands.DetachTablet(ui64(opId.GetTxId()), ui64(dst), opId.GetSubTxId());
}

void TSideEffects::ActivateTx(TOperationId opId) {
    ActivationParts.insert(opId);
}

void TSideEffects::ActivateOperation(TTxId txId) {
    ActivationOps.insert(txId);
}

void TSideEffects::WaitShardCreated(TShardIdx idx, TOperationId opId) {
    PendingWaitShardCreated.emplace_back(idx, opId);
}

void TSideEffects::ActivateShardCreated(TShardIdx idx, TTxId txId) {
    PendingActivateShardCreated.emplace_back(idx, txId);
}

void TSideEffects::PublishAndWaitPublication(TOperationId opId, TPathId pathId) {
    PublishToSchemeBoard(opId, pathId);
    WaitPublications.emplace_back(opId, pathId);
}

void TSideEffects::DoneOperation(TOperationId opId) {
    DoneOperations.insert(opId);
    ReadyToNotify(opId);
}

void TSideEffects::DeleteShard(TShardIdx idx) {
    if (!idx) {
        return; //KIKIMR-8507
    }
    ToDeleteShards.insert(idx);
}

void TSideEffects::ToProgress(TIndexBuildId id) {
    IndexToProgress.push_back(id);
}

void TSideEffects::PublishToSchemeBoard(TOperationId opId, TPathId pathId) {
    PublishPaths[opId.GetTxId()].push_back(pathId);
}

void TSideEffects::RePublishToSchemeBoard(TOperationId opId, TPathId pathId) {
    RePublishPaths[opId.GetTxId()].push_back(pathId);
}

void TSideEffects::ReadyToNotify(TOperationId opId) {
    ReadyToNotifyOperations.insert(opId);
}

void TSideEffects::Dependence(TTxId parent, TTxId child) {
    Dependencies.push_back(TDependence(parent, child));
}

void TSideEffects::ApplyOnExecute(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TSideEffects ApplyOnExecute"
                << " at tablet# " << ss->TabletID());

    DoDoneParts(ss, ctx);
    DoSetBarriers(ss, ctx);
    DoCheckBarriers(ss, txc, ctx);

    DoWaitShardCreated(ss, ctx);
    DoActivateShardCreated(ss, ctx);

    DoPersistPublishPaths(ss, txc, ctx); // before DoReadyToNotify

    DoReadyToNotify(ss, ctx);
    ExpandCoordinatorProposes(ss, ctx);
    DoReleasePathState(ss, ctx);
    DoBindMsgAcks(ss, ctx);

    DoUpdateTenant(ss, txc, ctx);

    DoDoneTransactions(ss, txc, ctx);

    DoPersistDependencies(ss,txc, ctx);

    DoPersistDeleteShards(ss, txc, ctx);

    SetupRoutingLongOps(ss, ctx);
}

TSideEffects::TPublications TSideEffects::ExtractPublicationsToSchemeBoard() {
    TPublications tmp;
    tmp.swap(PublishPaths);
    return tmp;
}

void TSideEffects::Barrier(TOperationId opId, TString barrierName) {
    Barriers.emplace_back(opId, barrierName);
}


void TSideEffects::ApplyOnComplete(TSchemeShard* ss, const TActorContext& ctx) {
    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "TSideEffects ApplyOnComplete"
                    << " at tablet# " << ss->TabletID());

    DoCoordinatorAck(ss, ctx);
    DoMediatorsAck(ss, ctx);

    DoActivateOps(ss, ctx);

    DoWaitPublication(ss, ctx);
    DoPublishToSchemeBoard(ss, ctx);

    DoUpdateTempDirsToMakeState(ss, ctx);
    DoUpdateTempDirsToRemoveState(ss, ctx);

    DoSend(ss, ctx);
    DoBindMsg(ss, ctx);

    //attach/detach tablets
    PendingPipeTrackerCommands.Apply(ss->PipeTracker, ctx);  //it's better to decompose attach and detach, detach should be applied at ApplyOnExecute
    DoRegisterRelations(ss, ctx);

    DoTriggerDeleteShards(ss, ctx);

    ResumeLongOps(ss, ctx);
}

void TSideEffects::DoActivateOps(TSchemeShard* ss, const TActorContext& ctx) {
    for (auto txId: ActivationOps) {
        if (!ss->Operations.contains(txId)) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Unable to activate " << txId);
            continue;
        }

        auto operation = ss->Operations.at(txId);

        if (operation->WaitOperations.size()) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Delay activating"
                           << ", operation: " << txId
                           << ", there is await operations num " << operation->WaitOperations.size());
            continue;
        }

        for (ui32 partIdx = 0; partIdx < operation->Parts.size(); ++partIdx) {
            LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Activate send for " << TOperationId(txId, partIdx));
            ctx.Send(ctx.SelfID, new TEvPrivate::TEvProgressOperation(ui64(txId), partIdx));
        }
    }

    for (auto& opPart: ActivationParts) {
        if (!ss->Operations.contains(opPart.GetTxId())) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Unable to activate " << opPart);
            continue;
        }

        auto operation = ss->Operations.at(opPart.GetTxId());

        if (operation->WaitOperations.size()) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Delay activating"
                           << ", operation part: " << opPart
                           << ", there is await operations num " << operation->WaitOperations.size());
            continue;
        }

        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Activate send for " << opPart);
        ctx.Send(ctx.SelfID, new TEvPrivate::TEvProgressOperation(ui64(opPart.GetTxId()), opPart.GetSubTxId()));
    }
}

bool TSideEffects::CheckDecouplingProposes(TString& errExpl) const {
    THashMap<TTabletId, TOperationId> checkDecoupling;
    for (auto& rec: CoordinatorProposesShards) {
        TOperationId opId;
        TTabletId shard;
        std::tie(opId, shard) = rec;

        bool inserted = false;
        auto position = checkDecoupling.end();
        std::tie(position, inserted) = checkDecoupling.emplace(shard, opId);
        if (!inserted && position->second != opId) {
            errExpl = TStringBuilder()
                    << "can't propose more then one operation to the shard with the same txId"
                    << ", here shardId is " << shard
                    << " has collision with operations " << opId
                    << " and " << position->second;
            return false;
        }
    }
    return true;
}


void TSideEffects::ExpandCoordinatorProposes(TSchemeShard* ss, const TActorContext& ctx) {
    TString errExpl;
    Y_ABORT_UNLESS(CheckDecouplingProposes(errExpl), "check decoupling: %s", errExpl.c_str());

    TSet<TTxId> touchedTxIds;
    for (auto& rec: CoordinatorProposes) {
        TOperationId opId;
        TPathId pathId;
        TStepId minStep;
        std::tie(opId, pathId, minStep) = rec;

        ss->Operations.at(opId.GetTxId())->ProposePart(opId.GetSubTxId(), pathId, minStep);
        touchedTxIds.insert(opId.GetTxId());
    }

    for (auto& rec: CoordinatorProposesShards) {
        TOperationId opId;
        TTabletId shard;
        std::tie(opId, shard) = rec;

        ss->Operations.at(opId.GetTxId())->ProposePart(opId.GetSubTxId(), shard);
        touchedTxIds.insert(opId.GetTxId());
    }

    for (TTxId txId: touchedTxIds) {
        TOperation::TPtr operation = ss->Operations.at(txId);
        if (operation->IsReadyToPropose(ctx)) {
            operation->DoPropose(ss, *this, ctx);
        }
    }
}

void TSideEffects::DoReadyToNotify(TSchemeShard* ss, const TActorContext& ctx) {
    for (auto& opId: ReadyToNotifyOperations) {
        TOperation::TPtr operation = ss->Operations.at(opId.GetTxId());
        operation->ReadyToNotifyPart(opId.GetSubTxId());

        if (operation->IsReadyToNotify(ctx)) {
            operation->DoNotify(ss, *this, ctx);
        }
    }
}

void TSideEffects::DoMediatorsAck(TSchemeShard* ss, const TActorContext& ctx) {
    for (auto& rec: MediatorAcks) {
        TActorId mediator;
        TStepId step;
        std::tie(mediator, step) = rec;

        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Ack mediator"
                    << " stepId#" << step);

        ctx.Send(mediator, new TEvTxProcessing::TEvPlanStepAccepted(
                     ss->TabletID(),
                     ui64(step)));
    }
}

void TSideEffects::DoCoordinatorAck(TSchemeShard* ss, const TActorContext& ctx) {
    //aggregate
    TMap<TActorId, TMap<TStepId, TSet<TTxId>>> toCoordinatorAck;
    for (auto& rec: CoordinatorAcks) {
        TActorId coordinator;
        TStepId step;
        TTxId txId;
        std::tie(coordinator, step, txId) = rec;

        toCoordinatorAck[coordinator][step].insert(txId);
    }
    //and send coordinator acks
    for (auto& byCoordinator: toCoordinatorAck) {
        TActorId coordinator = byCoordinator.first;
        for (auto& byStep: byCoordinator.second) {
            auto step = byStep.first;
            TSet<TTxId>& txIds = byStep.second;

            LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Ack coordinator"
                        << " stepId#" << step
                        << " first txId#" << *txIds.begin()
                        << " countTxs#" << txIds.size());

            ctx.Send(coordinator, new TEvTxProcessing::TEvPlanStepAck(
                         ss->TabletID(),
                         ui64(step),
                         txIds.begin(), txIds.end()));
        }
    }
}

void TSideEffects::DoUpdateTenant(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext& ctx) {
    for (const TPathId pathId : TenantsToUpdate) {
        Y_ABORT_UNLESS(ss->PathsById.contains(pathId));

        if (!ss->PathsById.at(pathId)->IsExternalSubDomainRoot()) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "DoUpdateTenant no IsExternalSubDomainRoot"
                           << ", pathId: : " << pathId
                           << ", at schemeshard: " << ss->TabletID());
            continue;
        }

        TPath tenantRoot = TPath::Init(pathId, ss);
        Y_ABORT_UNLESS(tenantRoot.Base()->IsExternalSubDomainRoot());

        TSubDomainInfo::TPtr& subDomain = ss->SubDomains.at(pathId);

        if (!ss->SubDomainsLinks.IsActive(pathId)) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "DoUpdateTenant no IsActiveChild"
                           << ", pathId: : " << pathId
                           << ", at schemeshard: " << ss->TabletID());
            continue;
        }

        auto& tenantLink = ss->SubDomainsLinks.GetLink(pathId);
        Y_ABORT_UNLESS(tenantLink.DomainKey == pathId);

        auto message = MakeHolder<TEvSchemeShard::TEvUpdateTenantSchemeShard>(ss->TabletID(), ss->Generation());

        bool hasChanges = false;

        if (tenantLink.TenantRootACL && tenantRoot.Base()->ACL) {
            // KIKIMR-10699: transfer tenants root ACL from GSS to the TSS
            // here GSS sees the ACL from TSS
            // so GSS just removes what already has been transferred from GSS to TSS

            NACLib::TACL tenantRootACL(tenantLink.TenantRootACL);

            // we transform ACL to the TDiffACL
            NACLib::TDiffACL diffACL;
            for (auto ace: tenantRootACL.GetACE()) {
                diffACL.RemoveAccess(ace);
            }

            TString prevACL = tenantRoot.Base()->ACL;
            // and apply that diff to the GSS tenants ACL
            tenantRoot.Base()->ApplyACL(diffACL.SerializeAsString());

            if (prevACL != tenantRoot.Base()->ACL) {
                ++tenantRoot.Base()->ACLVersion;

                NIceDb::TNiceDb db(txc.DB);
                ss->PersistACL(db, tenantRoot.Base());

                PublishToSchemeBoard(InvalidOperationId, pathId);
                {
                    TPath parent = tenantRoot.Parent();
                    ++parent.Base()->DirAlterVersion;
                    ss->PersistPathDirAlterVersion(db, parent.Base());
                    ss->ClearDescribePathCaches(parent.Base());
                    PublishToSchemeBoard(InvalidOperationId, parent.Base()->PathId);
                }
            }
        }

        if (tenantRoot.Base()->ACL) {
            // send ACL until all rights are transferred
            message->SetUpdateTenantRootACL(tenantRoot.Base()->ACL);
            hasChanges = true;
        }

        ui32 actualEffectiveACLVersion = tenantRoot.GetEffectiveACLVersion();
        if (tenantLink.EffectiveACLVersion < actualEffectiveACLVersion) {
            message->SetEffectiveACL(tenantRoot.Base()->Owner, tenantRoot.GetEffectiveACL(), actualEffectiveACLVersion);
            hasChanges = true;
        }

        if (tenantLink.SubdomainVersion < subDomain->GetVersion()) {
            message->SetStoragePools(subDomain->GetStoragePools(), subDomain->GetVersion());
            if (subDomain->GetDeclaredSchemeQuotas()) {
                message->Record.MutableDeclaredSchemeQuotas()->CopyFrom(*subDomain->GetDeclaredSchemeQuotas());
            }
            if (subDomain->GetDatabaseQuotas()) {
                message->Record.MutableDatabaseQuotas()->CopyFrom(*subDomain->GetDatabaseQuotas());
            }
            if (const auto& auditSettings = subDomain->GetAuditSettings()) {
                message->Record.MutableAuditSettings()->CopyFrom(*auditSettings);
            }
            if (const auto& serverlessComputeResourcesMode = subDomain->GetServerlessComputeResourcesMode()) {
                message->Record.SetServerlessComputeResourcesMode(*serverlessComputeResourcesMode);
            }
            hasChanges = true;
        }

        ui32 actualUserAttrsVersion = tenantRoot.Base()->UserAttrs->AlterVersion;
        if (tenantLink.UserAttributesVersion < actualUserAttrsVersion) {
            message->SetUserAttrs(tenantRoot.Base()->UserAttrs->Attrs, actualUserAttrsVersion);
            hasChanges = true;
        }

        if (!tenantLink.TenantHive && subDomain->GetTenantHiveID()) {
            message->SetTenantHive(ui64(subDomain->GetTenantHiveID()));
            hasChanges = true;
        }

        if (tenantLink.TenantHive) {
            if (subDomain->GetAlter()) {
                Y_VERIFY_S(tenantLink.TenantHive == subDomain->GetAlter()->GetTenantHiveID(),
                           "tenant hive is inconsistent"
                               << " on tss: " << tenantLink.TenantHive
                               << " on gss: " << subDomain->GetAlter()->GetTenantHiveID());
            } else {
                Y_VERIFY_S(tenantLink.TenantHive == subDomain->GetTenantHiveID(),
                           "tenant hive is inconsistent"
                               << " on tss: " << tenantLink.TenantHive
                               << " on gss: " << subDomain->GetTenantHiveID());
            }
        }

        if (!tenantLink.TenantSysViewProcessor && subDomain->GetTenantSysViewProcessorID()) {
            message->SetTenantSysViewProcessor(ui64(subDomain->GetTenantSysViewProcessorID()));
            hasChanges = true;
        }

        if (tenantLink.TenantSysViewProcessor) {
            if (subDomain->GetAlter()) {
                Y_VERIFY_S(tenantLink.TenantSysViewProcessor == subDomain->GetAlter()->GetTenantSysViewProcessorID(),
                           "tenant SVP is inconsistent"
                               << " on tss: " << tenantLink.TenantSysViewProcessor
                               << " on gss: " << subDomain->GetAlter()->GetTenantSysViewProcessorID());
            } else {
                Y_VERIFY_S(tenantLink.TenantSysViewProcessor == subDomain->GetTenantSysViewProcessorID(),
                           "tenant SVP is inconsistent"
                               << " on tss: " << tenantLink.TenantSysViewProcessor
                               << " on gss: " << subDomain->GetTenantSysViewProcessorID());
            }
        }

        if (!tenantLink.TenantStatisticsAggregator && subDomain->GetTenantStatisticsAggregatorID()) {
            message->SetTenantStatisticsAggregator(ui64(subDomain->GetTenantStatisticsAggregatorID()));
            hasChanges = true;
        }

        if (tenantLink.TenantStatisticsAggregator) {
            if (subDomain->GetAlter()) {
                Y_VERIFY_S(tenantLink.TenantStatisticsAggregator == subDomain->GetAlter()->GetTenantStatisticsAggregatorID(),
                           "tenant SA is inconsistent"
                               << " on tss: " << tenantLink.TenantStatisticsAggregator
                               << " on gss: " << subDomain->GetAlter()->GetTenantStatisticsAggregatorID());
            } else {
                Y_VERIFY_S(tenantLink.TenantStatisticsAggregator == subDomain->GetTenantStatisticsAggregatorID(),
                           "tenant SA is inconsistent"
                               << " on tss: " << tenantLink.TenantStatisticsAggregator
                               << " on gss: " << subDomain->GetTenantStatisticsAggregatorID());
            }
        }

        if (!tenantLink.TenantGraphShard && subDomain->GetTenantGraphShardID()) {
            message->SetTenantGraphShard(ui64(subDomain->GetTenantGraphShardID()));
            hasChanges = true;
        }

        if (!hasChanges) {
            LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "DoUpdateTenant no hasChanges"
                           << ", pathId: " << pathId
                           << ", tenantLink: " << tenantLink
                           << ", subDomain->GetVersion(): " << subDomain->GetVersion()
                           << ", actualEffectiveACLVersion: " << actualEffectiveACLVersion
                           << ", actualUserAttrsVersion: " << actualUserAttrsVersion
                           << ", tenantHive: " << subDomain->GetTenantHiveID()
                           << ", tenantSysViewProcessor: " << subDomain->GetTenantSysViewProcessorID()
                           << ", at schemeshard: " << ss->TabletID());
            continue;
        }

        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                   "Send TEvUpdateTenantSchemeShard"
                       << ", to actor: " << tenantLink.ActorId
                       << ", msg: " << message->Record.ShortDebugString()
                       << ", at schemeshard: " << ss->TabletID());

        Send(tenantLink.ActorId, message.Release());
    }
}

void TSideEffects::DoPersistPublishPaths(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
    NIceDb::TNiceDb db(txc.DB);

    for (const auto& kv : PublishPaths) {
        const TTxId txId = kv.first;
        if (!ss->Operations.contains(txId)) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Cannot publish paths for unknown operation id#" << txId);
            continue;
        }

        TOperation::TPtr operation = ss->Operations.at(txId);

        const auto& paths = kv.second;
        for (TPathId pathId : paths) {
            Y_ABORT_UNLESS(ss->PathsById.contains(pathId));

            const ui64 version = ss->GetPathVersion(TPath::Init(pathId, ss)).GetGeneralVersion();
            if (operation->AddPublishingPath(pathId, version)) {
                ss->PersistPublishingPath(db, txId, pathId, version);
            }
        }
    }
}


void TSideEffects::DoPublishToSchemeBoard(TSchemeShard* ss, const TActorContext& ctx) {
    for (auto& kv : PublishPaths) {
        ss->PublishToSchemeBoard(kv.first, std::move(kv.second), ctx);
    }

    for (auto& kv : RePublishPaths) {
        ss->PublishToSchemeBoard(kv.first, std::move(kv.second), ctx);
    }
}

void TSideEffects::DoSend(TSchemeShard* ss, const TActorContext& ctx) {
    for (auto& rec: Messages) {
        TActorId actor;
        THolder<::NActors::IEventBase> message;
        ui64 cookie;
        ui32 flags;
        std::tie(actor, message, cookie, flags) = rec;

        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Send "
                        << " to actor: " << actor
                        << " msg type: " << message->Type()
                        << " msg: " << message->ToString().substr(0, 1000)
                        << " at schemeshard: " << ss->TabletID());

        ctx.Send(actor, message.Release(), flags, cookie);
    }
}

void TSideEffects::DoBindMsg(TSchemeShard *ss, const TActorContext &ctx) {
    for (auto& rec: BindedMessages) {
        TOperationId opId;
        TTabletId tablet;
        TPipeMessageId cookie;
        THolder<::NActors::IEventBase> message;
        std::tie(opId, tablet, cookie, message) = rec;

        const ui32 msgType = message->Type();

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Send tablet strongly msg "
                        << " operationId: " << opId
                        << " from tablet: " << ss->TabletID()
                        << " to tablet: " << tablet
                        << " cookie: " << cookie
                        << " msg type: " << msgType);

        Y_ABORT_UNLESS(message->IsSerializable());

        if (!ss->Operations.contains(opId.GetTxId())) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                       "Send tablet strongly msg "
                           << ", operation already done"
                           << ", operationId: " << opId
                           << " from tablet: " << ss->TabletID()
                           << " to tablet: " << tablet
                           << " cookie: " << cookie
                           << " msg type: " << msgType);
            return;
        }

        Y_ABORT_UNLESS(ss->Operations.contains(opId.GetTxId()));
        TOperation::TPtr operation = ss->Operations.at(opId.GetTxId());

        TAllocChunkSerializer serializer;
        const bool success = message->SerializeToArcadiaStream(&serializer);
        Y_ABORT_UNLESS(success);
        TIntrusivePtr<TEventSerializedData> data = serializer.Release(message->CreateSerializationInfo());
        operation->PipeBindedMessages[tablet][cookie] = TOperation::TPreSerializedMessage(msgType, data, opId);

        ss->PipeClientCache->Send(ctx, ui64(tablet), msgType,  data, cookie.second);
    }
}

void TSideEffects::DoBindMsgAcks(TSchemeShard *ss, const TActorContext &ctx) {
    for (auto& ack: BindedMessageAcks) {
        TOperationId opId;
        TTabletId tablet;
        TPipeMessageId cookie;
        std::tie(opId, tablet, cookie) = ack;

        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Ack tablet strongly msg"
                        << " opId: " << opId
                        << " from tablet: " << ss->TabletID()
                        << " to tablet: " << tablet
                        << " cookie: " << cookie);

        if (!ss->Operations.contains(opId.GetTxId())) {
            continue;
        }

        TOperation::TPtr operation = ss->Operations.at(opId.GetTxId());

        if (operation->PipeBindedMessages.contains(tablet)) {
            if (operation->PipeBindedMessages[tablet].contains(cookie)) {
                operation->PipeBindedMessages[tablet].erase(cookie);
            }

            if (operation->PipeBindedMessages[tablet].size() == 0) {
                operation->PipeBindedMessages.erase(tablet);
                //Detach(opId, tablet);
            }
        }
    }
}

void TSideEffects::DoRegisterRelations(TSchemeShard *ss, const TActorContext &ctx) {
    for (auto& opId: RelationsByTabletsFromOperation) {
        TTxState* txState = ss->FindTx(opId);
        if (!txState) {
            continue;
        }

        for (TTxState::TShardOperation& shard : txState->Shards) {
            if (!ss->ShardInfos.contains(shard.Idx)) {
                continue;
            }

            auto tabletId = ss->ShardInfos.at(shard.Idx).TabletID;
            RouteByTablet(opId, tabletId);
        }
    }

    for (auto& rec: RelationsByTabletId) {
        TOperationId opId = InvalidOperationId;
        TTabletId tablet = InvalidTabletId;
        std::tie(opId, tablet) = rec;

        if (auto opPPtr = ss->Operations.FindPtr(opId.GetTxId())) {
           (*opPPtr)->RegisterRelationByTabletId(opId.GetSubTxId(), tablet, ctx);
        }
    }

    for (auto& rec: RelationsByShardIdx) {
        TOperationId opId = InvalidOperationId;
        TShardIdx shardIdx = InvalidShardIdx;
        std::tie(opId, shardIdx) = rec;

        if (auto opPPtr = ss->Operations.FindPtr(opId.GetTxId())) {
           (*opPPtr)->RegisterRelationByShardIdx(opId.GetSubTxId(), shardIdx, ctx);
        }
    }
}

void TSideEffects::DoTriggerDeleteShards(TSchemeShard *ss, const TActorContext &ctx) {
    ss->DoShardsDeletion(ToDeleteShards, ctx);
}

void TSideEffects::DoReleasePathState(TSchemeShard *ss, const TActorContext &) {
    for (auto& rec: ReleasePathStateRecs) {
        TOperationId opId = InvalidOperationId;
        TPathId pathId = InvalidPathId;
        NKikimrSchemeOp::EPathState state = NKikimrSchemeOp::EPathStateNotExist;
        std::tie(opId, pathId, state) = rec;

        if (!ss->Operations.contains(opId.GetTxId())) {
            continue;
        }

        TOperation::TPtr operation = ss->Operations.at(opId.GetTxId());
        if (operation->ReleasePathAtDone.contains(pathId)) {
            Y_ABORT_UNLESS(operation->ReleasePathAtDone.at(pathId) == state);
            continue;
        }

        operation->ReleasePathAtDone[pathId] = state;
    }
}

void TSideEffects::DoPersistDeleteShards(TSchemeShard *ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &) {
    NIceDb::TNiceDb db(txc.DB);
    ss->PersistShardsToDelete(db, ToDeleteShards);
}

void TSideEffects::DoUpdateTempDirsToMakeState(TSchemeShard* ss, const TActorContext &ctx) {
    for (auto& [ownerActorId, tempDirs]: TempDirsToMakeState) {

        auto& TempDirsByOwner = ss->TempDirsState.TempDirsByOwner;
        auto& nodeStates = ss->TempDirsState.NodeStates;

        const auto it = TempDirsByOwner.find(ownerActorId);

        const auto nodeId = ownerActorId.NodeId();

        const auto itNodeStates = nodeStates.find(nodeId);
        if (itNodeStates == nodeStates.end()) {
            auto& nodeState = nodeStates[nodeId];
            nodeState.Owners.insert(ownerActorId);
            nodeState.RetryState.CurrentDelay =
                TDuration::MilliSeconds(ss->BackgroundCleaningRetrySettings.GetStartDelayMs());
        } else {
            itNodeStates->second.Owners.insert(ownerActorId);
        }

        if (it == TempDirsByOwner.end()) {
            ctx.Send(new IEventHandle(ownerActorId, ss->SelfId(),
                new TEvSchemeShard::TEvOwnerActorAck(),
                IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession));

            auto& currentDirsTables = TempDirsByOwner[ownerActorId];

            for (auto& pathId : tempDirs) {
                currentDirsTables.insert(std::move(pathId));
            }
            continue;
        }

        for (auto& pathId : tempDirs) {
            it->second.insert(std::move(pathId));
        }
    }
}

void TSideEffects::DoUpdateTempDirsToRemoveState(TSchemeShard* ss, const TActorContext& ctx) {
    for (auto& [ownerActorId, tempDirs]: TempDirsToRemoveState) {
        auto& TempDirsByOwner = ss->TempDirsState.TempDirsByOwner;
        const auto it = TempDirsByOwner.find(ownerActorId);
        if (it == TempDirsByOwner.end()) {
            continue;
        }

        for (auto& pathId : tempDirs) {
            const auto tempDirIt = it->second.find(pathId);
            if (tempDirIt == it->second.end()) {
                continue;
            }

            it->second.erase(tempDirIt);
            ss->RemoveBackgroundCleaning(pathId);
        }

        if (it->second.empty()) {
            TempDirsByOwner.erase(it);

            auto& nodeStates = ss->TempDirsState.NodeStates;

            const auto nodeId = ownerActorId.NodeId();
            auto itStates = nodeStates.find(nodeId);
            if (itStates != nodeStates.end()) {
                const auto itOwner = itStates->second.Owners.find(ownerActorId);
                if (itOwner != itStates->second.Owners.end()) {
                    itStates->second.Owners.erase(itOwner);
                }
                if (itStates->second.Owners.empty()) {
                    nodeStates.erase(itStates);
                    ctx.Send(new IEventHandle(TActivationContext::InterconnectProxy(nodeId), ss->SelfId(),
                        new TEvents::TEvUnsubscribe, 0));
                }
            }
        }
    }
}

void TSideEffects::ResumeLongOps(TSchemeShard *ss, const TActorContext &ctx) {
    ss->Resume(IndexToProgress, ctx);
}

void TSideEffects::SetupRoutingLongOps(TSchemeShard *ss, const TActorContext &ctx) {
    ss->SetupRouting(IndexToProgress, ctx);
}

void TSideEffects::DoPersistDependencies(TSchemeShard *ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &) {
    NIceDb::TNiceDb db(txc.DB);
    for (auto& item: Dependencies) {
        TTxId parent = InvalidTxId;
        TTxId child = InvalidTxId;
        std::tie(parent, child) = item;
        if (parent != child) {
            if (ss->Operations.contains(parent) && ss->Operations.contains(child)) {
                Y_VERIFY_S(ss->Operations.contains(parent), "parent operation not exist"
                           << ", parent tx " << parent
                           << ", dependent tx " << child);
                ss->Operations.at(parent)->DependentOperations.insert(child);

                Y_ABORT_UNLESS(ss->Operations.contains(child));
                ss->Operations.at(child)->WaitOperations.insert(parent);

                ss->PersistAddTxDependency(db, parent, child);
            }
        }
    }
}

void TSideEffects::DoDoneParts(TSchemeShard *ss, const TActorContext &ctx) {
    for (auto& opId: DoneOperations) {
        TTxId txId = opId.GetTxId();

        if (!ss->Operations.contains(txId)) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Part operation has been done before id#" << opId);
            continue;
        }

        TOperation::TPtr operation = ss->Operations.at(txId);
        operation->DoneParts.insert(opId.GetSubTxId());
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "Part operation is done"
                        << " id#" << opId
                        << " progress is " << operation->DoneParts.size() << "/" << operation->Parts.size());

        if (!operation->IsReadyToDone(ctx)) {
            continue;
        }

        DoneTransactions.insert(opId.GetTxId());
    }
}

void TSideEffects::DoDoneTransactions(TSchemeShard *ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) {
    for (auto& txId: DoneTransactions) {

        if (!ss->Operations.contains(txId)) {
            continue;
        }

        TOperation::TPtr operation = ss->Operations.at(txId);
        if (!operation->IsReadyToDone(ctx)) {
            continue;
        }

        NIceDb::TNiceDb db(txc.DB);

        for (auto& item: operation->ReleasePathAtDone) {
            TPathId pathId = item.first;
            NKikimrSchemeOp::EPathState state = item.second;

            Y_ABORT_UNLESS(ss->PathsById.contains(pathId));
            TPathElement::TPtr path = ss->PathsById.at(pathId);
            path->PathState = state;
        }

        for (auto& dependent: operation->DependentOperations) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Remove dependency"
                            << ", parent tx: " << txId
                            << ", dependent tx: " << dependent);

            ss->PersistRemoveTxDependency(db, txId, dependent);

            if (!ss->Operations.contains(dependent)) {
                continue;
            }

            auto dependentOp = ss->Operations.at(dependent);
            Y_VERIFY_S(dependentOp->WaitOperations.contains(txId),
                       "self consistency check, dependentOp must have parent operation in WaitOperations"
                           << ", dependent " << dependent
                           << ", parent " << txId);

            dependentOp->WaitOperations.erase(txId);

            if (dependentOp->WaitOperations.empty()) {
                ActivateOperation(dependent);
            }
        }

        for (auto item: ss->PipeTracker.FindTablets(ui64(txId))) {
            ui64 pipeCookie = item.first;
            auto tabletId = TTabletId(item.second);
            DetachOperationFromPipe(TOperationId(txId, pipeCookie), tabletId);
        }

        for (ui32 partId = 0; partId < operation->Parts.size(); ++partId) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "Operation and all the parts is done"
                             << ", operation id: " << TOperationId(txId, partId));
            ss->RemoveTx(ctx, db, TOperationId(txId, partId), nullptr);
        }

        if (!operation->IsPublished()) {
            LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "Publication still in progress"
                             << ", tx: " << txId
                             << ", publications: " << operation->Publications.size()
                             << ", subscribers: " << operation->Subscribers.size());

            for (const auto& pub : operation->Publications) {
                LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "Publication details: "
                        << " tx: " << txId
                        << ", " << pub.first
                        << ", " << pub.second);
            }

            ss->Publications[txId] = {
                std::move(operation->Publications),
                std::move(operation->Subscribers)
            };
        }

        ss->Operations.erase(txId);
    }
}

void TSideEffects::DoWaitShardCreated(TSchemeShard* ss, const TActorContext&) {
    for (auto& entry : PendingWaitShardCreated) {
        TShardIdx shardIdx;
        TOperationId opId;
        std::tie(shardIdx, opId) = entry;
        auto it = ss->Operations.find(opId.GetTxId());
        if (it != ss->Operations.end()) {
            it->second->WaitShardCreated(shardIdx, opId.GetSubTxId());
        }
    }
}

void TSideEffects::DoActivateShardCreated(TSchemeShard* ss, const TActorContext&) {
    for (auto& entry : PendingActivateShardCreated) {
        TShardIdx shardIdx;
        TTxId txId;
        std::tie(shardIdx, txId) = entry;
        auto it = ss->Operations.find(txId);
        if (it != ss->Operations.end()) {
            for (TSubTxId partId : it->second->ActivateShardCreated(shardIdx)) {
                ActivateTx(TOperationId(txId, partId));
            }
        }
    }
}

void TSideEffects::DoWaitPublication(TSchemeShard *ss, const TActorContext &/*ctx*/) {
    for (auto& entry : WaitPublications) {
        TOperationId opId;
        TPathId pathId;
        std::tie(opId, pathId) = entry;

        auto it = ss->Operations.find(opId.GetTxId());
        if (it != ss->Operations.end()) {
            const ui64 version = ss->GetPathVersion(TPath::Init(pathId, ss)).GetGeneralVersion();
            it->second->RegisterWaitPublication(opId.GetSubTxId(), pathId, version);
        }
    }
}

void TSideEffects::DoSetBarriers(TSchemeShard *ss, const TActorContext &ctx) {
    for (auto& entry : Barriers) {
        TOperationId opId;
        TString name;
        std::tie(opId, name) = entry;

        auto it = ss->Operations.find(opId.GetTxId());
        if (it != ss->Operations.end()) {
            auto& operation = it->second;
            operation->RegisterBarrier(opId.GetSubTxId(), name);

            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "Set barrier"
                             << ", OperationId: " << opId
                             << ", name: " << name
                             << ", done: " << operation->DoneParts.size()
                             << ", blocked: " << operation->Barriers.at(name).size()
                             << ", parts count: " << operation->Parts.size());
        }
    }
}

void TSideEffects::DoCheckBarriers(TSchemeShard *ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) {
    TSet<TTxId> touchedOperations;

    for (auto& entry : Barriers) {
        TOperationId opId;
        TString name;
        std::tie(opId, name) = entry;

        touchedOperations.insert(opId.GetTxId());
    }

    for (auto& opId : DoneOperations) {
        touchedOperations.insert(opId.GetTxId());
    }

    for (auto& txId : touchedOperations) {
        auto it = ss->Operations.find(txId);

        if (it == ss->Operations.end()) {
            continue;
        }

        auto& operation = it->second;

        if (!operation->HasBarrier()) {
            continue;
        }

        if (!operation->IsDoneBarrier()) {
            continue;
        }

        auto name = operation->Barriers.begin()->first;
        const auto& blockedParts = operation->Barriers.begin()->second;

        LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                     "All parts have reached barrier"
                         << ", tx: " << txId
                         << ", done: " << operation->DoneParts.size()
                         << ", blocked: " << blockedParts.size());

        TMemoryChanges memChanges;
        TStorageChanges dbChanges;
        TOperationContext context{ss, txc, ctx, *this, memChanges, dbChanges};

        THolder<TEvPrivate::TEvCompleteBarrier> msg = MakeHolder<TEvPrivate::TEvCompleteBarrier>(txId, name);
        TEvPrivate::TEvCompleteBarrier::TPtr personalEv = (TEventHandle<TEvPrivate::TEvCompleteBarrier>*) new IEventHandle(
                    context.SS->SelfId(), context.SS->SelfId(), msg.Release());

        for (auto& partId: blockedParts) {
            operation->Parts.at(partId)->HandleReply(personalEv, context);
        }

        operation->DropBarrier(name);
    }
}

}
}
