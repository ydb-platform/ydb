#include "schemeshard_export.h"
#include "schemeshard_export_flow_proposals.h"
#include "schemeshard_impl.h"

#include <util/generic/xrange.h>
#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_import.pb.h>

namespace NKikimr {
namespace NSchemeShard {

namespace {

    void AddIssue(NKikimrExport::TExport& exprt, const TString& errorMessage) {
        auto& issue = *exprt.AddIssues();
        issue.set_severity(NYql::TSeverityIds::S_ERROR);
        issue.set_message(errorMessage);
    }

    void FillIssues(NKikimrExport::TExport& exprt, const TExportInfo::TPtr exportInfo) {
        if (exportInfo->Issue) {
            AddIssue(exprt, exportInfo->Issue);
        }

        for (const auto& item : exportInfo->Items) {
            if (item.Issue) {
                AddIssue(exprt, item.Issue);
            }
        }
    }

    NProtoBuf::Timestamp SecondsToProtoTimeStamp(ui64 sec) {
        NProtoBuf::Timestamp timestamp;
        timestamp.set_seconds((i64)(sec));
        timestamp.set_nanos(0);
        return timestamp;
    }

    void FillItemProgress(TSchemeShard* ss, const TExportInfo::TPtr exportInfo, ui32 itemIdx,
            Ydb::Export::ExportItemProgress& itemProgress) {

        Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
        const auto& item = exportInfo->Items.at(itemIdx);

        const auto opId = TOperationId(item.WaitTxId, FirstSubTxId);
        if (item.WaitTxId != InvalidTxId && ss->TxInFlight.contains(opId)) {
            const auto& txState = ss->TxInFlight.at(opId);
            if (txState.TxType != TTxState::TxBackup) {
                return;
            }

            itemProgress.set_parts_total(txState.Shards.size());
            itemProgress.set_parts_completed(txState.Shards.size() - txState.ShardsInProgress.size());
            *itemProgress.mutable_start_time() = SecondsToProtoTimeStamp(txState.StartTime.Seconds());
        } else {
            const auto path = TPath::Resolve(ExportItemPathName(ss, exportInfo, itemIdx), ss);
            if (!path.IsResolved() || !ss->Tables.contains(path.Base()->PathId)) {
                return;
            }

            auto table = ss->Tables.at(path.Base()->PathId);
            auto it = table->BackupHistory.end();
            if (item.WaitTxId != InvalidTxId) {
                it = table->BackupHistory.find(item.WaitTxId);
            } else if (table->BackupHistory.size() == 1) {
                it = table->BackupHistory.begin();
            }

            if (it == table->BackupHistory.end()) {
                return;
            }

            const auto& backupResult = it->second;
            itemProgress.set_parts_total(backupResult.TotalShardCount);
            itemProgress.set_parts_completed(backupResult.TotalShardCount);
            *itemProgress.mutable_start_time() = SecondsToProtoTimeStamp(backupResult.StartDateTime);
            *itemProgress.mutable_end_time() = SecondsToProtoTimeStamp(backupResult.CompletionDateTime);
        }
    }

} // anonymous

void TSchemeShard::FromXxportInfo(NKikimrExport::TExport& exprt, const TExportInfo::TPtr exportInfo) {
    exprt.SetId(exportInfo->Id);
    exprt.SetStatus(Ydb::StatusIds::SUCCESS);
    
    if (exportInfo->StartTime != TInstant::Zero()) {
        *exprt.MutableStartTime() = SecondsToProtoTimeStamp(exportInfo->StartTime.Seconds());
    }
    if (exportInfo->EndTime != TInstant::Zero()) {
        *exprt.MutableEndTime() = SecondsToProtoTimeStamp(exportInfo->EndTime.Seconds());
    }

    if (exportInfo->UserSID) {
        exprt.SetUserSID(*exportInfo->UserSID);
    }

    switch (exportInfo->State) {
    case TExportInfo::EState::CreateExportDir:
    case TExportInfo::EState::CopyTables:
        exprt.SetProgress(Ydb::Export::ExportProgress::PROGRESS_PREPARING);
        break;

    case TExportInfo::EState::Transferring:
    case TExportInfo::EState::Done:
        for (ui32 itemIdx : xrange(exportInfo->Items.size())) {
            FillItemProgress(this, exportInfo, itemIdx, *exprt.AddItemsProgress());
        }
        exprt.SetProgress(exportInfo->IsDone()
            ? Ydb::Export::ExportProgress::PROGRESS_DONE
            : Ydb::Export::ExportProgress::PROGRESS_TRANSFER_DATA);
        break;

    case TExportInfo::EState::Dropping:
        exprt.SetProgress(Ydb::Export::ExportProgress::PROGRESS_DONE);
        break;

    case TExportInfo::EState::Cancellation:
        FillIssues(exprt, exportInfo);
        exprt.SetProgress(Ydb::Export::ExportProgress::PROGRESS_CANCELLATION);
        break;

    case TExportInfo::EState::Cancelled:
        exprt.SetStatus(Ydb::StatusIds::CANCELLED);
        FillIssues(exprt, exportInfo);
        exprt.SetProgress(Ydb::Export::ExportProgress::PROGRESS_CANCELLED);
        break;

    default:
        exprt.SetStatus(Ydb::StatusIds::UNDETERMINED);
        exprt.SetProgress(Ydb::Export::ExportProgress::PROGRESS_UNSPECIFIED);
        break;
    }

    switch (exportInfo->Kind) {
    case TExportInfo::EKind::YT:
        Y_ABORT_UNLESS(exprt.MutableExportToYtSettings()->ParseFromString(exportInfo->Settings));
        exprt.MutableExportToYtSettings()->clear_token();
        break;

    case TExportInfo::EKind::S3:
        Y_ABORT_UNLESS(exprt.MutableExportToS3Settings()->ParseFromString(exportInfo->Settings));
        exprt.MutableExportToS3Settings()->clear_access_key();
        exprt.MutableExportToS3Settings()->clear_secret_key();
        break;
    }
}

void TSchemeShard::PersistCreateExport(NIceDb::TNiceDb& db, const TExportInfo::TPtr exportInfo) {
    db.Table<Schema::Exports>().Key(exportInfo->Id).Update(
        NIceDb::TUpdate<Schema::Exports::Uid>(exportInfo->Uid),
        NIceDb::TUpdate<Schema::Exports::Kind>(static_cast<ui8>(exportInfo->Kind)),
        NIceDb::TUpdate<Schema::Exports::Settings>(exportInfo->Settings),
        NIceDb::TUpdate<Schema::Exports::DomainPathOwnerId>(exportInfo->DomainPathId.OwnerId),
        NIceDb::TUpdate<Schema::Exports::DomainPathId>(exportInfo->DomainPathId.LocalPathId),
        NIceDb::TUpdate<Schema::Exports::Items>(exportInfo->Items.size())
    );

    if (exportInfo->UserSID) {
        db.Table<Schema::Exports>().Key(exportInfo->Id).Update(
            NIceDb::TUpdate<Schema::Exports::UserSID>(*exportInfo->UserSID)
        );
    }

    for (ui32 itemIdx : xrange(exportInfo->Items.size())) {
        const auto& item = exportInfo->Items.at(itemIdx);

        db.Table<Schema::ExportItems>().Key(exportInfo->Id, itemIdx).Update(
            NIceDb::TUpdate<Schema::ExportItems::SourcePathName>(item.SourcePathName),
            NIceDb::TUpdate<Schema::ExportItems::SourceOwnerPathId>(item.SourcePathId.OwnerId),
            NIceDb::TUpdate<Schema::ExportItems::SourcePathId>(item.SourcePathId.LocalPathId),
            NIceDb::TUpdate<Schema::ExportItems::State>(static_cast<ui8>(item.State))
        );
    }
}

void TSchemeShard::PersistRemoveExport(NIceDb::TNiceDb& db, const TExportInfo::TPtr exportInfo) {
    for (ui32 itemIdx : xrange(exportInfo->Items.size())) {
        db.Table<Schema::ExportItems>().Key(exportInfo->Id, itemIdx).Delete();
    }

    db.Table<Schema::Exports>().Key(exportInfo->Id).Delete();
}

void TSchemeShard::PersistExportPathId(NIceDb::TNiceDb& db, const TExportInfo::TPtr exportInfo) {
    db.Table<Schema::Exports>().Key(exportInfo->Id).Update(
        NIceDb::TUpdate<Schema::Exports::ExportOwnerPathId>(exportInfo->ExportPathId.OwnerId),
        NIceDb::TUpdate<Schema::Exports::ExportPathId>(exportInfo->ExportPathId.LocalPathId)
    );
}

void TSchemeShard::PersistExportState(NIceDb::TNiceDb& db, const TExportInfo::TPtr exportInfo) {
    db.Table<Schema::Exports>().Key(exportInfo->Id).Update(
        NIceDb::TUpdate<Schema::Exports::State>(static_cast<ui8>(exportInfo->State)),
        NIceDb::TUpdate<Schema::Exports::WaitTxId>(exportInfo->WaitTxId),
        NIceDb::TUpdate<Schema::Exports::Issue>(exportInfo->Issue),
        NIceDb::TUpdate<Schema::Exports::StartTime>(exportInfo->StartTime.Seconds()),
        NIceDb::TUpdate<Schema::Exports::EndTime>(exportInfo->EndTime.Seconds())
    );
}

void TSchemeShard::PersistExportItemState(NIceDb::TNiceDb& db, const TExportInfo::TPtr exportInfo, ui32 itemIdx) {
    Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
    const auto& item = exportInfo->Items.at(itemIdx);

    db.Table<Schema::ExportItems>().Key(exportInfo->Id, itemIdx).Update(
        NIceDb::TUpdate<Schema::ExportItems::State>(static_cast<ui8>(item.State)),
        NIceDb::TUpdate<Schema::ExportItems::BackupTxId>(item.WaitTxId),
        NIceDb::TUpdate<Schema::ExportItems::Issue>(item.Issue)
    );
}

void TSchemeShard::Handle(TEvExport::TEvCreateExportRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCreateExport(ev), ctx);
}

void TSchemeShard::Handle(TEvExport::TEvGetExportRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxGetExport(ev), ctx);
}

void TSchemeShard::Handle(TEvExport::TEvCancelExportRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCancelExport(ev), ctx);
}

void TSchemeShard::Handle(TEvExport::TEvForgetExportRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxForgetExport(ev), ctx);
}

void TSchemeShard::Handle(TEvExport::TEvListExportsRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxListExports(ev), ctx);
}

void TSchemeShard::ResumeExports(const TVector<ui64>& exportIds, const TActorContext& ctx) {
    for (const ui64 id : exportIds) {
        Execute(CreateTxProgressExport(id), ctx);
    }
}

} // NSchemeShard
} // NKikimr
