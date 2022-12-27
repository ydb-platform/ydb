#pragma once

#include "schemeshard_impl.h"
#include "schemeshard_identificators.h"
#include "schemeshard_billing_helpers.h"
#include "schemeshard_build_index_helpers.h"

#include "schemeshard__operation_part.h" // TSideEffects, make separate file

#include <ydb/core/metering/metering.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr {
namespace NSchemeShard {

class TSchemeShard::TIndexBuilder::TTxBase: public NTabletFlatExecutor::TTransactionBase<TSchemeShard> {
private:
    TSideEffects SideEffects;

    using TChangeStateRec = std::tuple<TIndexBuildId, TIndexBuildInfo::EState>;
    TDeque<TChangeStateRec> StateChanges;
    using TBillingEventSchedule = std::tuple<TIndexBuildId, TDuration>;
    TDeque<TBillingEventSchedule> ToScheduleBilling;
    using TToBill = std::tuple<TIndexBuildId, TInstant, TInstant>;
    TDeque<TToBill> ToBill;

    void ApplyState(NTabletFlatExecutor::TTransactionContext& txc) {
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

    void ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
        SideEffects.ApplyOnExecute(Self, txc, ctx);
        ApplyState(txc);
        ApplyBill(txc, ctx);
    }

    void ApplyOnComplete(const TActorContext& ctx) {
        SideEffects.ApplyOnComplete(Self, ctx);
        ApplySchedule(ctx);
    }

    void ApplySchedule(const TActorContext& ctx) {
        for (const auto& rec: ToScheduleBilling) {
            ctx.Schedule(
                std::get<1>(rec),
                new TEvPrivate::TEvIndexBuildingMakeABill(
                    ui64(std::get<0>(rec)),
                    ctx.Now()));
        }
    }

    ui64 RequestUnits(const TBillingStats& stats) {
        return TRUCalculator::ReadTable(stats.GetBytes())
             + TRUCalculator::BulkUpsert(stats.GetBytes(), stats.GetRows());
    }

    void RoundPeriod(TInstant& start, TInstant& end) {
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

    void ApplyBill(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx)
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

protected:
    void Send(TActorId dst, THolder<IEventBase> message, ui32 flags = 0, ui64 cookie = 0) {
        SideEffects.Send(dst, message.Release(), cookie, flags);
    }

    void ChangeState(TIndexBuildId id, TIndexBuildInfo::EState state) {
        StateChanges.push_back(TChangeStateRec(id, state));
    }

    void Progress(TIndexBuildId id) {
        SideEffects.ToProgress(id);
    }

    void Fill(NKikimrIndexBuilder::TIndexBuild& index, const TIndexBuildInfo::TPtr indexInfo) {
        index.SetId(ui64(indexInfo->Id));
        if (indexInfo->Issue) {
            AddIssue(index.MutableIssues(), indexInfo->Issue);
        }

        for (const auto& item: indexInfo->Shards) {
            const TShardIdx& shardIdx = item.first;
            const TIndexBuildInfo::TShardStatus& status = item.second;

            if (status.Status != NKikimrTxDataShard::TEvBuildIndexProgressResponse::INPROGRESS) {
                if (status.UploadStatus != Ydb::StatusIds::SUCCESS) {
                    if (status.DebugMessage) {
                        AddIssue(index.MutableIssues(), status.ToString(shardIdx));
                    }
                }
            }
        }

        switch (indexInfo->State) {
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

    void Fill(NKikimrIndexBuilder::TIndexBuildSettings& settings, const TIndexBuildInfo::TPtr indexInfo) {
        TPath table = TPath::Init(indexInfo->TablePathId, Self);
        settings.set_source_path(table.PathString());

        Ydb::Table::TableIndex& index = *settings.mutable_index();
        index.set_name(indexInfo->IndexName);

        *index.mutable_index_columns() = {
            indexInfo->IndexColumns.begin(),
            indexInfo->IndexColumns.end()
        };

        *index.mutable_data_columns() = {
            indexInfo->DataColumns.begin(),
            indexInfo->DataColumns.end()
        };

        switch (indexInfo->IndexType) {
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobal:
            *index.mutable_global_index() = Ydb::Table::GlobalIndex();
            break;
        case NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync:
            *index.mutable_global_async_index() = Ydb::Table::GlobalAsyncIndex();
            break;
        case NKikimrSchemeOp::EIndexType::EIndexTypeInvalid:
            Y_FAIL("Unreachable");
        };

        settings.set_max_batch_bytes(indexInfo->Limits.MaxBatchBytes);
        settings.set_max_batch_rows(indexInfo->Limits.MaxBatchRows);
        settings.set_max_shards_in_flight(indexInfo->Limits.MaxShards);
        settings.set_max_retries_upload_batch(indexInfo->Limits.MaxRetries);
    }

    void AddIssue(::google::protobuf::RepeatedPtrField< ::Ydb::Issue::IssueMessage>* issues,
                  const TString& message,
                  NYql::TSeverityIds::ESeverityId severity = NYql::TSeverityIds::S_ERROR)
    {
        auto& issue = *issues->Add();
        issue.set_severity(severity);
        issue.set_message(message);
    }

    void SendNotificationsIfFinished(TIndexBuildInfo::TPtr indexInfo) {
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

    void EraseBuildInfo(const TIndexBuildInfo::TPtr indexBuildInfo) {
        Self->IndexBuilds.erase(indexBuildInfo->Id);
        Self->IndexBuildsByUid.erase(indexBuildInfo->Uid);

        Self->TxIdToIndexBuilds.erase(indexBuildInfo->LockTxId);
        Self->TxIdToIndexBuilds.erase(indexBuildInfo->InitiateTxId);
        Self->TxIdToIndexBuilds.erase(indexBuildInfo->ApplyTxId);
        Self->TxIdToIndexBuilds.erase(indexBuildInfo->UnlockTxId);
    }

    Ydb::StatusIds::StatusCode TranslateStatusCode(NKikimrScheme::EStatus status) {
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
            Y_FAIL("unreachable");
        }

        return Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    }

    void Bill(const TIndexBuildInfo::TPtr& indexBuildInfo, TInstant startPeriod = TInstant::Zero(), TInstant endPeriod = TInstant::Zero()) {

        ToBill.push_back(TToBill(indexBuildInfo->Id, std::move(startPeriod), std::move(endPeriod)));
    }

    void AskToScheduleBilling(const TIndexBuildInfo::TPtr& indexBuildInfo) {
        if (indexBuildInfo->BillingEventIsScheduled) {
            return;
        }

        if (indexBuildInfo->State != TIndexBuildInfo::EState::Filling) {
            return;
        }

        indexBuildInfo->BillingEventIsScheduled = true;

        ToScheduleBilling.push_back(TBillingEventSchedule(indexBuildInfo->Id, indexBuildInfo->ReBillPeriod));
    }

    bool GotScheduledBilling(const TIndexBuildInfo::TPtr& indexBuildInfo) {
        if (!indexBuildInfo->BillingEventIsScheduled) {
            return false;
        }

        if (indexBuildInfo->State != TIndexBuildInfo::EState::Filling) {
            return false;
        }

        indexBuildInfo->BillingEventIsScheduled = false;

        return true;
    }

public:
    explicit TTxBase(TSelf* self)
        : TBase(self)
    { }

    virtual ~TTxBase() = default;

    virtual bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) = 0;
    virtual void DoComplete(const TActorContext& ctx) = 0;

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (!DoExecute(txc, ctx)) {
            return false;
        }

        ApplyOnExecute(txc, ctx);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        DoComplete(ctx);

        ApplyOnComplete(ctx);
    }

};

} // NSchemeShard
} // NKikimr
