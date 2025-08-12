#include "schemeshard_build_index_tx_base.h"

#include "schemeshard__operation_side_effects.h"
#include "schemeshard_billing_helpers.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_identificators.h"
#include "schemeshard_impl.h"

#include <ydb/core/metering/metering.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr {
namespace NSchemeShard {

void TSchemeShard::TIndexBuilder::TTxBase::ApplyState(NTabletFlatExecutor::TTransactionContext& txc) {
    for (auto& rec: StateChanges) {
        TIndexBuildId buildId;
        TIndexBuildInfo::EState state;
        std::tie(buildId, state) = rec;

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        Y_VERIFY_S(buildInfoPtr, "IndexBuilds has no " << buildId);
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_I("Change state from " << buildInfo.State << " to " << state);
        if (state == TIndexBuildInfo::EState::Rejected ||
            state == TIndexBuildInfo::EState::Cancelled ||
            state == TIndexBuildInfo::EState::Done) {
            buildInfo.EndTime = TAppData::TimeProvider->Now();
        }
        buildInfo.State = state;

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistBuildIndexState(db, buildInfo);
    }
}

void TSchemeShard::TIndexBuilder::TTxBase::ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
    SideEffects.ApplyOnExecute(Self, txc, ctx);
    ApplyState(txc);
    ApplyBill(txc, ctx);
}

void TSchemeShard::TIndexBuilder::TTxBase::ApplyOnComplete(const TActorContext& ctx) {
    SideEffects.ApplyOnComplete(Self, ctx);
    ApplySchedule(ctx);
}

void TSchemeShard::TIndexBuilder::TTxBase::ApplySchedule(const TActorContext& ctx) {
    for (const auto& rec: ToScheduleBilling) {
        ctx.Schedule(
            std::get<1>(rec),
            new TEvPrivate::TEvIndexBuildingMakeABill(
                ui64(std::get<0>(rec)),
                ctx.Now()));
    }
}

void TSchemeShard::TIndexBuilder::TTxBase::RoundPeriod(TInstant& start, TInstant& end) {
    if (start.Hours() == end.Hours()) {
        return; // that is OK
    }

    TInstant hourEnd = TInstant::Hours(end.Hours());

    if (end - hourEnd >= TDuration::Seconds(1)) {
        start = hourEnd;
        return;
    }

    if (hourEnd - start >= TDuration::Seconds(2)) {
        end = hourEnd - TDuration::Seconds(1);
        return;
    }

    start = hourEnd - TDuration::Seconds(2);
    end = hourEnd - TDuration::Seconds(1);
}

void TSchemeShard::TIndexBuilder::TTxBase::ApplyBill(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx)
{
    for (const auto& rec: ToBill) {
        const auto& buildId = std::get<0>(rec);
        auto startPeriod = std::get<1>(rec);
        auto endPeriod = std::get<2>(rec);

        if (!startPeriod && !endPeriod) {
            startPeriod = endPeriod = ctx.Now();
        }

        RoundPeriod(startPeriod, endPeriod);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        Y_VERIFY_S(buildInfoPtr, "IndexBuilds has no " << buildId);
        auto& buildInfo = *buildInfoPtr->Get();
        auto& processed = buildInfo.Processed;
        auto& billed = buildInfo.Billed;

        auto toBill = processed - billed;
        if (TMeteringStatsHelper::IsZero(toBill)) {
            continue;
        }

        TPath domain = TPath::Init(buildInfo.DomainPathId, Self);
        TPathElement::TPtr pathEl = domain.Base();

        TString cloud_id;
        if (pathEl->UserAttrs->Attrs.contains("cloud_id")) {
            cloud_id = pathEl->UserAttrs->Attrs.at("cloud_id");
        }
        TString folder_id;
        if (pathEl->UserAttrs->Attrs.contains("folder_id")) {
            folder_id = pathEl->UserAttrs->Attrs.at("folder_id");
        }
        TString database_id;
        if (pathEl->UserAttrs->Attrs.contains("database_id")) {
            database_id = pathEl->UserAttrs->Attrs.at("database_id");
        }

        if (!cloud_id || !folder_id || !database_id) {
            LOG_I("ApplyBill: unable to make a bill, neither cloud_id and nor folder_id nor database_id have found in user attributes at the domain"
                  << ", build index operation: " << buildId
                  << ", domain: " << domain.PathString()
                  << ", domainId: " << buildInfo.DomainPathId
                  << ", tableId: " << buildInfo.TablePathId
                  << ", not billed usage: " << toBill);
            continue;
        }

        if (!Self->IsServerlessDomain(domain)) {
            LOG_I("ApplyBill: unable to make a bill, domain is not a serverless db"
                  << ", build index operation: " << buildId
                  << ", domain: " << domain.PathString()
                  << ", domainId: " << buildInfo.DomainPathId
                  << ", IsDomainSchemeShard: " << Self->IsDomainSchemeShard
                  << ", ParentDomainId: " << Self->ParentDomainId
                  << ", ResourcesDomainId: " << domain.DomainInfo()->GetResourcesDomainId()
                  << ", not billed usage: " << toBill);
            continue;
        }

        TString id = TStringBuilder()
            << buildId << "-"
            << buildInfo.TablePathId.OwnerId << "-" << buildInfo.TablePathId.LocalPathId << "-"
            << billed.GetUploadRows() << "-" << billed.GetReadRows() << "-"
            << billed.GetUploadBytes() << "-" << billed.GetReadBytes() << "-"
            << processed.GetUploadRows() << "-" << processed.GetReadRows() << "-"
            << processed.GetUploadBytes() << "-" << processed.GetReadBytes();

        NIceDb::TNiceDb db(txc.DB);

        billed += toBill;
        Self->PersistBuildIndexBilled(db, buildInfo);

        TString requestUnitsExplain;
        ui64 requestUnits = TRUCalculator::Calculate(toBill, requestUnitsExplain);

        const TString billRecord = TBillRecord()
            .Id(id)
            .CloudId(cloud_id)
            .FolderId(folder_id)
            .ResourceId(database_id)
            .SourceWt(ctx.Now())
            .Usage(TBillRecord::RequestUnits(requestUnits, startPeriod, endPeriod))
            .ToString();

        LOG_N("ApplyBill: make a bill, id#" << buildId
            << ", billRecord: " << billRecord
            << ", toBill: " << toBill
            << ", explain: " << requestUnitsExplain
            << ", buildInfo: " << buildInfo);

        auto request = MakeHolder<NMetering::TEvMetering::TEvWriteMeteringJson>(std::move(billRecord));
        // send message at Complete stage
        Send(NMetering::MakeMeteringServiceID(), std::move(request));
    }
}

void TSchemeShard::TIndexBuilder::TTxBase::Send(TActorId dst, THolder<IEventBase> message, ui32 flags, ui64 cookie) {
    SideEffects.Send(dst, message.Release(), cookie, flags);
}

void TSchemeShard::TIndexBuilder::TTxBase::AllocateTxId(TIndexBuildId buildId) {
    LOG_D("AllocateTxId " << buildId);
    Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(buildId));
}

void TSchemeShard::TIndexBuilder::TTxBase::ChangeState(TIndexBuildId id, TIndexBuildInfo::EState state) {
    StateChanges.push_back(TChangeStateRec(id, state));
}

void TSchemeShard::TIndexBuilder::TTxBase::Progress(TIndexBuildId id) {
    SideEffects.ToProgress(id);
}

void TSchemeShard::TIndexBuilder::TTxBase::Fill(NKikimrIndexBuilder::TIndexBuild& index, const TIndexBuildInfo& indexInfo) {
    index.SetId(ui64(indexInfo.Id));
    if (indexInfo.GetIssue()) {
        AddIssue(index.MutableIssues(), indexInfo.GetIssue());
    }
    if (indexInfo.StartTime != TInstant::Zero()) {
        *index.MutableStartTime() = SecondsToProtoTimeStamp(indexInfo.StartTime.Seconds());
    }
    if (indexInfo.EndTime != TInstant::Zero()) {
        *index.MutableEndTime() = SecondsToProtoTimeStamp(indexInfo.EndTime.Seconds());
    }
    if (indexInfo.UserSID) {
        index.SetUserSID(*indexInfo.UserSID);
    }

    for (const auto& item: indexInfo.Shards) {
        const TShardIdx& shardIdx = item.first;
        const TIndexBuildInfo::TShardStatus& status = item.second;

        if (status.Status != NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS) {
            if (status.UploadStatus != Ydb::StatusIds::SUCCESS) {
                if (status.DebugMessage) {
                    AddIssue(index.MutableIssues(), status.ToString(shardIdx));
                }
            }
        }
    }

    switch (indexInfo.State) {
    case TIndexBuildInfo::EState::AlterMainTable:
    case TIndexBuildInfo::EState::Locking:
    case TIndexBuildInfo::EState::GatheringStatistics:
    case TIndexBuildInfo::EState::Initiating:
        index.SetState(Ydb::Table::IndexBuildState::STATE_PREPARING);
        index.SetProgress(0.0);
        break;
    case TIndexBuildInfo::EState::Filling:
    case TIndexBuildInfo::EState::DropBuild:
    case TIndexBuildInfo::EState::CreateBuild:
    case TIndexBuildInfo::EState::LockBuild:
        index.SetState(Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA);
        index.SetProgress(indexInfo.CalcProgressPercent());
        break;
    case TIndexBuildInfo::EState::Applying:
    case TIndexBuildInfo::EState::Unlocking:
        index.SetState(Ydb::Table::IndexBuildState::STATE_APPLYING);
        index.SetProgress(100.0);
        break;
    case TIndexBuildInfo::EState::Done:
        index.SetState(Ydb::Table::IndexBuildState::STATE_DONE);
        index.SetProgress(100.0);
        break;
    case TIndexBuildInfo::EState::Cancellation_DroppingColumns:
    case TIndexBuildInfo::EState::Cancellation_Applying:
    case TIndexBuildInfo::EState::Cancellation_Unlocking:
        index.SetState(Ydb::Table::IndexBuildState::STATE_CANCELLATION);
        index.SetProgress(0.0);
        break;
    case TIndexBuildInfo::EState::Cancelled:
        index.SetState(Ydb::Table::IndexBuildState::STATE_CANCELLED);
        index.SetProgress(0.0);
        break;
    case TIndexBuildInfo::EState::Rejection_DroppingColumns:
    case TIndexBuildInfo::EState::Rejection_Applying:
    case TIndexBuildInfo::EState::Rejection_Unlocking:
        index.SetState(Ydb::Table::IndexBuildState::STATE_REJECTION);
        index.SetProgress(0.0);
        break;
    case TIndexBuildInfo::EState::Rejected:
        index.SetState(Ydb::Table::IndexBuildState::STATE_REJECTED);
        index.SetProgress(0.0);
        break;
    case TIndexBuildInfo::EState::Invalid:
        index.SetState(Ydb::Table::IndexBuildState::STATE_UNSPECIFIED);
        break;
    }

    Fill(*index.MutableSettings(), indexInfo);
}

void TSchemeShard::TIndexBuilder::TTxBase::Fill(NKikimrIndexBuilder::TIndexBuildSettings& settings, const TIndexBuildInfo& info) {
    TPath table = TPath::Init(info.TablePathId, Self);
    settings.set_source_path(table.PathString());

    if (info.IsBuildIndex()) {
        Ydb::Table::TableIndex& index = *settings.mutable_index();
        index.set_name(info.IndexName);

        *index.mutable_index_columns() = {
            info.IndexColumns.begin(),
            info.IndexColumns.end()
        };

        *index.mutable_data_columns() = {
            info.DataColumns.begin(),
            info.DataColumns.end()
        };

        switch (info.IndexType) {
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobal:
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalUnique:
            *index.mutable_global_index() = Ydb::Table::GlobalIndex();
            break;
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync:
            *index.mutable_global_async_index() = Ydb::Table::GlobalAsyncIndex();
            break;
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree:
            *index.mutable_global_vector_kmeans_tree_index() = Ydb::Table::GlobalVectorKMeansTreeIndex();
            break;
        default:
            Y_ABORT("Unreachable");
        }
    } else if (info.IsBuildColumns()) {
        for(const auto& column : info.BuildColumns) {
            auto* columnProto = settings.mutable_column_build_operation()->add_column();
            columnProto->SetColumnName(column.ColumnName);
            columnProto->mutable_default_from_literal()->CopyFrom(column.DefaultFromLiteral);
        }
    }

    settings.MutableScanSettings()->CopyFrom(info.ScanSettings);
    settings.set_max_shards_in_flight(info.MaxInProgressShards);
}

void TSchemeShard::TIndexBuilder::TTxBase::AddIssue(::google::protobuf::RepeatedPtrField<::Ydb::Issue::IssueMessage>* issues,
              const TString& message,
              NYql::TSeverityIds::ESeverityId severity)
{
    auto& issue = *issues->Add();
    issue.set_severity(severity);
    issue.set_message(message);
}

void TSchemeShard::TIndexBuilder::TTxBase::SendNotificationsIfFinished(TIndexBuildInfo& indexInfo) {
    if (!indexInfo.IsFinished()) {
        return;
    }

    LOG_T("TIndexBuildInfo SendNotifications: "
          << ": id# " << indexInfo.Id
          << ", subscribers count# " << indexInfo.Subscribers.size());

    TSet<TActorId> toAnswer;
    toAnswer.swap(indexInfo.Subscribers);
    for (auto& actorId: toAnswer) {
        Send(actorId, MakeHolder<TEvSchemeShard::TEvNotifyTxCompletionResult>(ui64(indexInfo.Id)));
    }
}

void TSchemeShard::TIndexBuilder::TTxBase::EraseBuildInfo(const TIndexBuildInfo& indexBuildInfo) {
    Self->TxIdToIndexBuilds.erase(indexBuildInfo.LockTxId);
    Self->TxIdToIndexBuilds.erase(indexBuildInfo.InitiateTxId);
    Self->TxIdToIndexBuilds.erase(indexBuildInfo.ApplyTxId);
    Self->TxIdToIndexBuilds.erase(indexBuildInfo.UnlockTxId);
    Self->TxIdToIndexBuilds.erase(indexBuildInfo.AlterMainTableTxId);
    Self->TxIdToIndexBuilds.erase(indexBuildInfo.DropColumnsTxId);

    Self->IndexBuildsByUid.erase(indexBuildInfo.Uid);
    Self->IndexBuilds.erase(indexBuildInfo.Id);
}

Ydb::StatusIds::StatusCode TSchemeShard::TIndexBuilder::TTxBase::TranslateStatusCode(NKikimrScheme::EStatus status) {
    switch (status) {
    case NKikimrScheme::StatusSuccess:
    case NKikimrScheme::StatusAccepted:
    case NKikimrScheme::StatusAlreadyExists:
        return Ydb::StatusIds::SUCCESS;

    case NKikimrScheme::StatusPathDoesNotExist:
    case NKikimrScheme::StatusPathIsNotDirectory:
    case NKikimrScheme::StatusSchemeError:
    case NKikimrScheme::StatusNameConflict:
    case NKikimrScheme::StatusInvalidParameter:
    case NKikimrScheme::StatusRedirectDomain:
        return Ydb::StatusIds::BAD_REQUEST;

    case NKikimrScheme::StatusMultipleModifications:
    case NKikimrScheme::StatusQuotaExceeded:
        return Ydb::StatusIds::OVERLOADED;

    case NKikimrScheme::StatusReadOnly:
    case NKikimrScheme::StatusPreconditionFailed:
    case NKikimrScheme::StatusResourceExhausted: //TODO: find better YDB status for it
        return Ydb::StatusIds::PRECONDITION_FAILED;

    case NKikimrScheme::StatusAccessDenied:
        return Ydb::StatusIds::UNAUTHORIZED;
    case NKikimrScheme::StatusNotAvailable:
    case NKikimrScheme::StatusTxIdNotExists:
    case NKikimrScheme::StatusTxIsNotCancellable:
    case NKikimrScheme::StatusReserved18:
    case NKikimrScheme::StatusReserved19:
        Y_ABORT("unreachable");
    }

    return Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
}

void TSchemeShard::TIndexBuilder::TTxBase::Bill(const TIndexBuildInfo& indexBuildInfo,
    TInstant startPeriod, TInstant endPeriod)
{
    ToBill.push_back(TToBill(indexBuildInfo.Id, std::move(startPeriod), std::move(endPeriod)));
}

void TSchemeShard::TIndexBuilder::TTxBase::AskToScheduleBilling(TIndexBuildInfo& indexBuildInfo) {
    if (indexBuildInfo.BillingEventIsScheduled) {
        return;
    }

    if (indexBuildInfo.State != TIndexBuildInfo::EState::Filling) {
        return;
    }

    indexBuildInfo.BillingEventIsScheduled = true;

    ToScheduleBilling.push_back(TBillingEventSchedule(indexBuildInfo.Id, indexBuildInfo.ReBillPeriod));
}

bool TSchemeShard::TIndexBuilder::TTxBase::GotScheduledBilling(TIndexBuildInfo& indexBuildInfo) {
    if (!indexBuildInfo.BillingEventIsScheduled) {
        return false;
    }

    if (indexBuildInfo.State != TIndexBuildInfo::EState::Filling) {
        return false;
    }

    indexBuildInfo.BillingEventIsScheduled = false;

    return true;
}

bool TSchemeShard::TIndexBuilder::TTxBase::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    bool executeResult;

    try {
        executeResult = DoExecute(txc, ctx);
    } catch (const std::exception& exc) {
        if (OnUnhandledExceptionSafe(txc, ctx, exc)) {
            return true;
        }
        throw; // fail process, a really bad thing has happened
    }

    if (!executeResult) {
        return false;
    }

    ApplyOnExecute(txc, ctx);
    return true;
}

bool TSchemeShard::TIndexBuilder::TTxBase::OnUnhandledExceptionSafe(TTransactionContext& txc, const TActorContext& ctx, const std::exception& originalExc) {
    try {
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildId);
        TIndexBuildInfo* buildInfo = buildInfoPtr
            ? buildInfoPtr->Get()
            : nullptr;

        LOG_E("Unhandled exception, id#"
            << (BuildId == InvalidIndexBuildId ? TString("<no id>") : TStringBuilder() << BuildId)
            << " " << TypeName(originalExc) << ": " << originalExc.what() << Endl
            << TBackTrace::FromCurrentException().PrintToString()
            << ", TIndexBuildInfo: " << (buildInfo ? TStringBuilder() << (*buildInfo) : TString("<no build info>")));

        OnUnhandledException(txc, ctx, buildInfo, originalExc);

        return true;
    } catch (const std::exception& handleExc) {
        LOG_E("OnUnhandledException throws unhandled exception " 
            << TypeName(handleExc) << ": " << handleExc.what() << Endl
            << TBackTrace::FromCurrentException().PrintToString());
        return false;
    }
}

void TSchemeShard::TIndexBuilder::TTxBase::Complete(const TActorContext& ctx) {
    DoComplete(ctx);

    ApplyOnComplete(ctx);
}

} // NSchemeShard
} // NKikimr
