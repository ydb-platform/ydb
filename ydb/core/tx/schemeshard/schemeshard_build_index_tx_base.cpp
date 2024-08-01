#include "schemeshard_build_index_tx_base.h"

#include "schemeshard_impl.h"
#include "schemeshard_identificators.h"
#include "schemeshard_billing_helpers.h"
#include "schemeshard_build_index_helpers.h"

#include "schemeshard__operation_side_effects.h"

#include <ydb/core/metering/metering.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr {
namespace NSchemeShard {

void TSchemeShard::TIndexBuilder::TTxBase::ApplyState(NTabletFlatExecutor::TTransactionContext& txc) {
    for (auto& rec: StateChanges) {
        TIndexBuildId buildId;
        TIndexBuildInfo::EState state;
        std::tie(buildId, state) = rec;

        Y_VERIFY_S(Self->IndexBuilds.contains(buildId), "IndexBuilds has no " << buildId);
        auto buildInfo = Self->IndexBuilds.at(buildId);
        LOG_I("Change state from " << buildInfo->State << " to " << state);
        buildInfo->State = state;

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

ui64 TSchemeShard::TIndexBuilder::TTxBase::RequestUnits(const TBillingStats& stats) {
    return TRUCalculator::ReadTable(stats.GetBytes())
         + TRUCalculator::BulkUpsert(stats.GetBytes(), stats.GetRows());
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

        Y_VERIFY_S(Self->IndexBuilds.contains(buildId), "IndexBuilds has no " << buildId);
        auto buildInfo = Self->IndexBuilds.at(buildId);

        TBillingStats toBill = buildInfo->Processed - buildInfo->Billed;
        if (!toBill) {
            continue;
        }

        TPath domain = TPath::Init(buildInfo->DomainPathId, Self);
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
            LOG_N("ApplyBill: unable to make a bill, neither cloud_id and nor folder_id nor database_id have found in user attributes at the domain"
                  << ", build index operation: " << buildId
                  << ", domain: " << domain.PathString()
                  << ", domainId: " << buildInfo->DomainPathId
                  << ", tableId: " << buildInfo->TablePathId
                  << ", not billed usage: " << toBill);
            continue;
        }

        if (!Self->IsServerlessDomain(domain)) {
            LOG_N("ApplyBill: unable to make a bill, domain is not a serverless db"
                  << ", build index operation: " << buildId
                  << ", domain: " << domain.PathString()
                  << ", domainId: " << buildInfo->DomainPathId
                  << ", IsDomainSchemeShard: " << Self->IsDomainSchemeShard
                  << ", ParentDomainId: " << Self->ParentDomainId
                  << ", ResourcesDomainId: " << domain.DomainInfo()->GetResourcesDomainId()
                  << ", not billed usage: " << toBill);
            continue;
        }

        NIceDb::TNiceDb db(txc.DB);

        buildInfo->Billed += toBill;
        Self->PersistBuildIndexBilling(db, buildInfo);

        ui64 requestUnits = RequestUnits(toBill);

        TString id = TStringBuilder()
            << buildId << "-"
            << buildInfo->TablePathId.OwnerId << "-" << buildInfo->TablePathId.LocalPathId << "-"
            << buildInfo->Billed.GetRows() << "-" << buildInfo->Billed.GetBytes() << "-"
            << buildInfo->Processed.GetRows() << "-" << buildInfo->Processed.GetBytes();

        const TString billRecord = TBillRecord()
            .Id(id)
            .CloudId(cloud_id)
            .FolderId(folder_id)
            .ResourceId(database_id)
            .SourceWt(ctx.Now())
            .Usage(TBillRecord::RequestUnits(requestUnits, startPeriod, endPeriod))
            .ToString();

        LOG_D("ApplyBill: made a bill"
              << ", buildInfo: " << *buildInfo
              << ", record: '" << billRecord << "'");

        auto request = MakeHolder<NMetering::TEvMetering::TEvWriteMeteringJson>(std::move(billRecord));
        // send message at Complete stage
        Send(NMetering::MakeMeteringServiceID(), std::move(request));
    }
}

void TSchemeShard::TIndexBuilder::TTxBase::Send(TActorId dst, THolder<IEventBase> message, ui32 flags, ui64 cookie) {
    SideEffects.Send(dst, message.Release(), cookie, flags);
}

void TSchemeShard::TIndexBuilder::TTxBase::ChangeState(TIndexBuildId id, TIndexBuildInfo::EState state) {
    StateChanges.push_back(TChangeStateRec(id, state));
}

void TSchemeShard::TIndexBuilder::TTxBase::Progress(TIndexBuildId id) {
    SideEffects.ToProgress(id);
}

void TSchemeShard::TIndexBuilder::TTxBase::Fill(NKikimrIndexBuilder::TIndexBuild& index, const TIndexBuildInfo::TPtr indexInfo) {
    index.SetId(ui64(indexInfo->Id));
    if (indexInfo->Issue) {
        AddIssue(index.MutableIssues(), indexInfo->Issue);
    }

    for (const auto& item: indexInfo->Shards) {
        const TShardIdx& shardIdx = item.first;
        const TIndexBuildInfo::TShardStatus& status = item.second;

        if (status.Status != NKikimrTxDataShard::EBuildIndexStatus::INPROGRESS) {
            if (status.UploadStatus != Ydb::StatusIds::SUCCESS) {
                if (status.DebugMessage) {
                    AddIssue(index.MutableIssues(), status.ToString(shardIdx));
                }
            }
        }
    }

    switch (indexInfo->State) {
    case TIndexBuildInfo::EState::AlterMainTable:
    case TIndexBuildInfo::EState::Locking:
    case TIndexBuildInfo::EState::GatheringStatistics:
    case TIndexBuildInfo::EState::Initiating:
        index.SetState(Ydb::Table::IndexBuildState::STATE_PREPARING);
        index.SetProgress(0.0);
        break;
    case TIndexBuildInfo::EState::Filling:
        index.SetState(Ydb::Table::IndexBuildState::STATE_TRANSFERING_DATA);
        index.SetProgress(indexInfo->CalcProgressPercent());
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
    case TIndexBuildInfo::EState::Cancellation_Applying:
    case TIndexBuildInfo::EState::Cancellation_Unlocking:
        index.SetState(Ydb::Table::IndexBuildState::STATE_CANCELLATION);
        index.SetProgress(0.0);
        break;
    case TIndexBuildInfo::EState::Cancelled:
        index.SetState(Ydb::Table::IndexBuildState::STATE_CANCELLED);
        index.SetProgress(0.0);
        break;
    case TIndexBuildInfo::EState::Rejection_Applying:
        index.SetState(Ydb::Table::IndexBuildState::STATE_REJECTION);
        index.SetProgress(0.0);
        break;
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

void TSchemeShard::TIndexBuilder::TTxBase::Fill(NKikimrIndexBuilder::TIndexBuildSettings& settings, const TIndexBuildInfo::TPtr info) {
    TPath table = TPath::Init(info->TablePathId, Self);
    settings.set_source_path(table.PathString());

    if (info->IsBuildIndex()) {
        Ydb::Table::TableIndex& index = *settings.mutable_index();
        index.set_name(info->IndexName);

        *index.mutable_index_columns() = {
            info->IndexColumns.begin(),
            info->IndexColumns.end()
        };

        *index.mutable_data_columns() = {
            info->DataColumns.begin(),
            info->DataColumns.end()
        };

        switch (info->IndexType) {
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
    } else if (info->IsBuildColumn()) {
        for(const auto& column : info->BuildColumns) {
            auto* columnProto = settings.mutable_column_build_operation()->add_column();
            columnProto->SetColumnName(column.ColumnName);
            columnProto->mutable_default_from_literal()->CopyFrom(column.DefaultFromLiteral);
        }
    }

    if (info->IsCheckingNotNull()) {
        for(const auto& column : info->CheckingNotNullColumns) {
            auto* columnProto = settings.mutable_column_check_not_null()->add_column();
            columnProto->SetColumnName(column.ColumnName);
        }
    }

    settings.set_max_batch_bytes(info->Limits.MaxBatchBytes);
    settings.set_max_batch_rows(info->Limits.MaxBatchRows);
    settings.set_max_shards_in_flight(info->Limits.MaxShards);
    settings.set_max_retries_upload_batch(info->Limits.MaxRetries);
}

void TSchemeShard::TIndexBuilder::TTxBase::AddIssue(::google::protobuf::RepeatedPtrField<::Ydb::Issue::IssueMessage>* issues,
              const TString& message,
              NYql::TSeverityIds::ESeverityId severity)
{
    auto& issue = *issues->Add();
    issue.set_severity(severity);
    issue.set_message(message);
}

void TSchemeShard::TIndexBuilder::TTxBase::SendNotificationsIfFinished(TIndexBuildInfo::TPtr indexInfo) {
    if (!indexInfo->IsFinished()) {
        return;
    }

    LOG_T("TIndexBuildInfo SendNotifications: "
          << ": id# " << indexInfo->Id
          << ", subscribers count# " << indexInfo->Subscribers.size());

    TSet<TActorId> toAnswer;
    toAnswer.swap(indexInfo->Subscribers);
    for (auto& actorId: toAnswer) {
        Send(actorId, MakeHolder<TEvSchemeShard::TEvNotifyTxCompletionResult>(ui64(indexInfo->Id)));
    }
}

void TSchemeShard::TIndexBuilder::TTxBase::EraseBuildInfo(const TIndexBuildInfo::TPtr indexBuildInfo) {
    Self->IndexBuilds.erase(indexBuildInfo->Id);
    Self->IndexBuildsByUid.erase(indexBuildInfo->Uid);

    Self->TxIdToIndexBuilds.erase(indexBuildInfo->LockTxId);
    Self->TxIdToIndexBuilds.erase(indexBuildInfo->InitiateTxId);
    Self->TxIdToIndexBuilds.erase(indexBuildInfo->ApplyTxId);
    Self->TxIdToIndexBuilds.erase(indexBuildInfo->UnlockTxId);
    Self->TxIdToIndexBuilds.erase(indexBuildInfo->AlterMainTableTxId);
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

void TSchemeShard::TIndexBuilder::TTxBase::Bill(const TIndexBuildInfo::TPtr& indexBuildInfo,
    TInstant startPeriod, TInstant endPeriod)
{
    ToBill.push_back(TToBill(indexBuildInfo->Id, std::move(startPeriod), std::move(endPeriod)));
}

void TSchemeShard::TIndexBuilder::TTxBase::AskToScheduleBilling(const TIndexBuildInfo::TPtr& indexBuildInfo) {
    if (indexBuildInfo->BillingEventIsScheduled) {
        return;
    }

    if (indexBuildInfo->State != TIndexBuildInfo::EState::Filling) {
        return;
    }

    indexBuildInfo->BillingEventIsScheduled = true;

    ToScheduleBilling.push_back(TBillingEventSchedule(indexBuildInfo->Id, indexBuildInfo->ReBillPeriod));
}

bool TSchemeShard::TIndexBuilder::TTxBase::GotScheduledBilling(const TIndexBuildInfo::TPtr& indexBuildInfo) {
    if (!indexBuildInfo->BillingEventIsScheduled) {
        return false;
    }

    if (indexBuildInfo->State != TIndexBuildInfo::EState::Filling) {
        return false;
    }

    indexBuildInfo->BillingEventIsScheduled = false;

    return true;
}

bool TSchemeShard::TIndexBuilder::TTxBase::Execute(TTransactionContext& txc, const TActorContext& ctx) {
    if (!DoExecute(txc, ctx)) {
        return false;
    }

    ApplyOnExecute(txc, ctx);
    return true;
}

void TSchemeShard::TIndexBuilder::TTxBase::Complete(const TActorContext& ctx) {
    DoComplete(ctx);

    ApplyOnComplete(ctx);
}

} // NSchemeShard
} // NKikimr
