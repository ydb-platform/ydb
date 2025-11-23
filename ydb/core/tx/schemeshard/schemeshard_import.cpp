#include "schemeshard_import.h"

#include "schemeshard_impl.h"
#include "schemeshard_import_getters.h"
#include "schemeshard_import_helpers.h"

#include <util/generic/xrange.h>

namespace NKikimr {
namespace NSchemeShard {

namespace {

    void FillIssues(NKikimrImport::TImport& import, const TImportInfo& importInfo) {
        if (importInfo.Issue) {
            AddIssue(import, importInfo.Issue);
        }

        for (const auto& item : importInfo.Items) {
            if (item.Issue) {
                AddIssue(import, item.Issue);
            }
        }
    }

    TImportInfo::EState GetMinState(const TImportInfo& importInfo) {
        TImportInfo::EState state = TImportInfo::EState::Invalid;

        for (const auto& item : importInfo.Items) {
            if (state == TImportInfo::EState::Invalid) {
                state = item.State;
            }

            state = Min(state, item.State);
        }

        return state;
    }
    ui32 GetTablePartitions(const Ydb::Table::CreateTableRequest& table) {
        switch (table.partitions_case()) {
            case Ydb::Table::CreateTableRequest::PartitionsCase::kUniformPartitions:
                return table.uniform_partitions();
            case Ydb::Table::CreateTableRequest::PartitionsCase::kPartitionAtKeys:
                return table.partition_at_keys().split_points_size() + 1;
            default:
                // Set min number of partitions as default
                return table.partitioning_settings().min_partitions_count();
        }
    }

    TString MakeIndexBuildUid(const TImportInfo& importInfo, ui32 itemIdx, i32 indexIdx) {
        return TStringBuilder() << importInfo.Id << "-" << itemIdx << "-" << indexIdx;
    }

    void AddTransferringItemProgress(TSchemeShard* ss, const TImportInfo& importInfo, ui32 itemIdx,
        Ydb::Import::ImportItemProgress& itemProgress) {

        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        const auto opId = TOperationId(item.WaitTxId, FirstSubTxId);
        if (item.WaitTxId != InvalidTxId && ss->TxInFlight.contains(opId)) {
            const auto& txState = ss->TxInFlight.at(opId);
            if (txState.TxType != TTxState::TxRestore) {
                return;
            }

            itemProgress.set_parts_total(itemProgress.parts_total() + txState.Shards.size());
            itemProgress.set_parts_completed(itemProgress.parts_completed() + txState.Shards.size() - txState.ShardsInProgress.size());
            *itemProgress.mutable_start_time() = SecondsToProtoTimeStamp(txState.StartTime.Seconds());
        } else {
            if (!ss->Tables.contains(item.DstPathId)) {
                return;
            }

            auto table = ss->Tables.at(item.DstPathId);
            auto it = table->RestoreHistory.end();
            if (item.WaitTxId != InvalidTxId && table->RestoreHistory.contains(item.WaitTxId)) {
                it = table->RestoreHistory.find(item.WaitTxId);
            } else if (table->RestoreHistory.size() >= 1) {
                // As restore operations always create new table, case table->RestoreHistory.size() > 1 is unexpected
                // To return at least something in this case, we'll just pick one of items from RestoreHistory
                it = table->RestoreHistory.begin();
            }

            if (it == table->RestoreHistory.end()) {
                return;
            }

            const auto& restoreResult = it->second;
            itemProgress.set_parts_total(itemProgress.parts_total() + restoreResult.TotalShardCount);
            itemProgress.set_parts_completed(itemProgress.parts_completed() + restoreResult.TotalShardCount);
            *itemProgress.mutable_start_time() = SecondsToProtoTimeStamp(restoreResult.StartDateTime);
            // Update for case no index builds are required
            *itemProgress.mutable_end_time() = SecondsToProtoTimeStamp(restoreResult.CompletionDateTime);
        }
    }

    void AddBuildIndexesItemProgress(TSchemeShard* ss, const TImportInfo& importInfo, ui32 itemIdx, 
        i32 indexIdx, Ydb::Import::ImportItemProgress& itemProgress) {

        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        Y_ABORT_UNLESS(indexIdx < item.Table->indexes_size());

        const auto buildUid = MakeIndexBuildUid(importInfo, itemIdx, indexIdx);
        if (ss->IndexBuildsByUid.contains(buildUid)) {
            const auto& indexBuild = ss->IndexBuildsByUid.FindPtr(buildUid)->Get();

            itemProgress.set_parts_total(itemProgress.parts_total() + indexBuild->Shards.size());
            itemProgress.set_parts_completed(itemProgress.parts_completed() + indexBuild->Shards.size() - indexBuild->InProgressShards.size());
            *itemProgress.mutable_end_time() = SecondsToProtoTimeStamp(indexBuild->EndTime.Seconds());

        } else {
            // As it is better and easier not to bind to schemeshard state,
            // We'll use info from CreateTableRequest instead
            // At this point item.Table is guaranteed not to be empty
            const auto partsTotal = GetTablePartitions(*item.Table);
            const auto partsCompleted = indexIdx >= item.NextIndexIdx ? 0 : partsTotal;

            itemProgress.set_parts_total(itemProgress.parts_total() + partsTotal);
            itemProgress.set_parts_completed(itemProgress.parts_completed() + partsCompleted);
        }
    }

    void FillItemProgress(TSchemeShard* ss, const TImportInfo& importInfo, ui32 itemIdx,
        Ydb::Import::ImportItemProgress& itemProgress) {

        itemProgress.set_parts_total(0);
        itemProgress.set_parts_completed(0);

        AddTransferringItemProgress(ss, importInfo, itemIdx, itemProgress);

        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);
        if (!item.Table) {
            return;
        }

        for (i32 indexIdx: xrange(item.Table->indexes_size())) {
            AddBuildIndexesItemProgress(ss, importInfo, itemIdx, indexIdx, itemProgress);
        }
    }

} // anonymous

void TSchemeShard::FromXxportInfo(NKikimrImport::TImport& import, const TImportInfo& importInfo) {
    import.SetId(importInfo.Id);
    import.SetStatus(Ydb::StatusIds::SUCCESS);

    if (importInfo.StartTime != TInstant::Zero()) {
        *import.MutableStartTime() = SecondsToProtoTimeStamp(importInfo.StartTime.Seconds());
    }
    if (importInfo.EndTime != TInstant::Zero()) {
        *import.MutableEndTime() = SecondsToProtoTimeStamp(importInfo.EndTime.Seconds());
    }

    if (importInfo.UserSID) {
        import.SetUserSID(*importInfo.UserSID);
    }

    switch (importInfo.State) {
    case TImportInfo::EState::DownloadExportMetadata:
        import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_PREPARING);
        break;
    case TImportInfo::EState::Waiting:
        switch (GetMinState(importInfo)) {
        case TImportInfo::EState::GetScheme:
        case TImportInfo::EState::CreateSchemeObject:
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_PREPARING);
            break;
        case TImportInfo::EState::Transferring:
            for (ui32 itemIdx : xrange(importInfo.Items.size())) {
                FillItemProgress(this, importInfo, itemIdx, *import.AddItemsProgress());
            }
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_TRANSFER_DATA);
            break;
        case TImportInfo::EState::BuildIndexes:
            for (ui32 itemIdx : xrange(importInfo.Items.size())) {
                FillItemProgress(this, importInfo, itemIdx, *import.AddItemsProgress());
            }
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_BUILD_INDEXES);
            break;
        case TImportInfo::EState::CreateChangefeed:
            for (ui32 itemIdx : xrange(importInfo.Items.size())) {
                FillItemProgress(this, importInfo, itemIdx, *import.AddItemsProgress());
            }
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_CREATE_CHANGEFEEDS);
            break;
        case TImportInfo::EState::Done:
            for (ui32 itemIdx : xrange(importInfo.Items.size())) {
                FillItemProgress(this, importInfo, itemIdx, *import.AddItemsProgress());
            }
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_DONE);
            break;
        default:
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_UNSPECIFIED);
            break;
        }
        break;

    case TImportInfo::EState::Done:
        for (ui32 itemIdx : xrange(importInfo.Items.size())) {
            FillItemProgress(this, importInfo, itemIdx, *import.AddItemsProgress());
        }
        import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_DONE);
        break;

    case TImportInfo::EState::Cancellation:
        FillIssues(import, importInfo);
        import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_CANCELLATION);
        break;

    case TImportInfo::EState::Cancelled:
        import.SetStatus(Ydb::StatusIds::CANCELLED);
        FillIssues(import, importInfo);
        import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_CANCELLED);
        break;

    default:
        import.SetStatus(Ydb::StatusIds::UNDETERMINED);
        import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_UNSPECIFIED);
        break;
    }

    switch (importInfo.Kind) {
    case TImportInfo::EKind::S3:
        import.MutableImportFromS3Settings()->CopyFrom(importInfo.Settings);
        import.MutableImportFromS3Settings()->clear_access_key();
        import.MutableImportFromS3Settings()->clear_secret_key();
        break;
    }
}

void TSchemeShard::PersistCreateImport(NIceDb::TNiceDb& db, const TImportInfo& importInfo) {
    db.Table<Schema::Imports>().Key(importInfo.Id).Update(
        NIceDb::TUpdate<Schema::Imports::Uid>(importInfo.Uid),
        NIceDb::TUpdate<Schema::Imports::Kind>(static_cast<ui8>(importInfo.Kind)),
        NIceDb::TUpdate<Schema::Imports::Settings>(importInfo.Settings.SerializeAsString()),
        NIceDb::TUpdate<Schema::Imports::DomainPathOwnerId>(importInfo.DomainPathId.OwnerId),
        NIceDb::TUpdate<Schema::Imports::DomainPathLocalId>(importInfo.DomainPathId.LocalPathId),
        NIceDb::TUpdate<Schema::Imports::Items>(importInfo.Items.size()),
        NIceDb::TUpdate<Schema::Imports::PeerName>(importInfo.PeerName),
        NIceDb::TUpdate<Schema::Imports::SanitizedToken>(importInfo.SanitizedToken)
    );

    if (importInfo.UserSID) {
        db.Table<Schema::Imports>().Key(importInfo.Id).Update(
            NIceDb::TUpdate<Schema::Imports::UserSID>(*importInfo.UserSID)
        );
    }

    for (ui32 itemIdx : xrange(importInfo.Items.size())) {
        const auto& item = importInfo.Items.at(itemIdx);

        db.Table<Schema::ImportItems>().Key(importInfo.Id, itemIdx).Update(
            NIceDb::TUpdate<Schema::ImportItems::DstPathName>(item.DstPathName),
            NIceDb::TUpdate<Schema::ImportItems::State>(static_cast<ui8>(item.State)),
            NIceDb::TUpdate<Schema::ImportItems::SrcPrefix>(item.SrcPrefix),
            NIceDb::TUpdate<Schema::ImportItems::SrcPath>(item.SrcPath)
        );
    }
}

void TSchemeShard::PersistSchemaMappingImportFields(NIceDb::TNiceDb& db, const TImportInfo& importInfo) {
    // There can be new items, so do at least the same as for creation
    for (ui32 itemIdx : xrange(importInfo.Items.size())) {
        const auto& item = importInfo.Items.at(itemIdx);

        db.Table<Schema::ImportItems>().Key(importInfo.Id, itemIdx).Update(
            NIceDb::TUpdate<Schema::ImportItems::DstPathName>(item.DstPathName),
            NIceDb::TUpdate<Schema::ImportItems::State>(static_cast<ui8>(item.State)),
            NIceDb::TUpdate<Schema::ImportItems::SrcPrefix>(item.SrcPrefix),
            NIceDb::TUpdate<Schema::ImportItems::SrcPath>(item.SrcPath)
        );
        if (item.ExportItemIV) {
            db.Table<Schema::ImportItems>().Key(importInfo.Id, itemIdx).Update(
                NIceDb::TUpdate<Schema::ImportItems::EncryptionIV>(item.ExportItemIV->GetBinaryString())
            );
        }
    }
}

void TSchemeShard::PersistRemoveImport(NIceDb::TNiceDb& db, const TImportInfo& importInfo) {
    for (ui32 itemIdx : xrange(importInfo.Items.size())) {
        db.Table<Schema::ImportItems>().Key(importInfo.Id, itemIdx).Delete();
    }

    db.Table<Schema::Imports>().Key(importInfo.Id).Delete();
}

void TSchemeShard::PersistImportState(NIceDb::TNiceDb& db, const TImportInfo& importInfo) {
    db.Table<Schema::Imports>().Key(importInfo.Id).Update(
        NIceDb::TUpdate<Schema::Imports::State>(static_cast<ui8>(importInfo.State)),
        NIceDb::TUpdate<Schema::Imports::Issue>(importInfo.Issue),
        NIceDb::TUpdate<Schema::Imports::StartTime>(importInfo.StartTime.Seconds()),
        NIceDb::TUpdate<Schema::Imports::EndTime>(importInfo.EndTime.Seconds()),
        NIceDb::TUpdate<Schema::Imports::Items>(importInfo.Items.size())
    );
}

void TSchemeShard::PersistImportItemState(NIceDb::TNiceDb& db, const TImportInfo& importInfo, ui32 itemIdx) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);

    db.Table<Schema::ImportItems>().Key(importInfo.Id, itemIdx).Update(
        NIceDb::TUpdate<Schema::ImportItems::State>(static_cast<ui8>(item.State)),
        NIceDb::TUpdate<Schema::ImportItems::WaitTxId>(item.WaitTxId),
        NIceDb::TUpdate<Schema::ImportItems::NextIndexIdx>(item.NextIndexIdx),
        NIceDb::TUpdate<Schema::ImportItems::NextChangefeedIdx>(item.NextChangefeedIdx),
        NIceDb::TUpdate<Schema::ImportItems::Issue>(item.Issue)
    );
}

void TSchemeShard::PersistImportItemScheme(NIceDb::TNiceDb& db, const TImportInfo& importInfo, ui32 itemIdx) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);

    auto record = db.Table<Schema::ImportItems>().Key(importInfo.Id, itemIdx);

    if (item.Table) {
        record.Update(
            NIceDb::TUpdate<Schema::ImportItems::Scheme>(item.Table->SerializeAsString())
        );
    }

    if (item.Topic) {
        record.Update(
            NIceDb::TUpdate<Schema::ImportItems::Topic>(item.Topic->SerializeAsString())
        );
    }
    if (!item.CreationQuery.empty()) {
        record.Update(
            NIceDb::TUpdate<Schema::ImportItems::CreationQuery>(item.CreationQuery)
        );
    }
    if (item.Permissions.Defined()) {
        record.Update(
            NIceDb::TUpdate<Schema::ImportItems::Permissions>(item.Permissions->SerializeAsString())
        );
    }
    db.Table<Schema::ImportItems>().Key(importInfo.Id, itemIdx).Update(
        NIceDb::TUpdate<Schema::ImportItems::Metadata>(item.Metadata.Serialize())
    );

    db.Table<Schema::ImportItems>().Key(importInfo.Id, itemIdx).Update(
        NIceDb::TUpdate<Schema::ImportItems::Changefeeds>(item.Changefeeds)
    );
}

void TSchemeShard::PersistImportItemPreparedCreationQuery(NIceDb::TNiceDb& db, const TImportInfo& importInfo, ui32 itemIdx) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items[itemIdx];

    if (item.PreparedCreationQuery) {
        db.Table<Schema::ImportItems>().Key(importInfo.Id, itemIdx).Update(
            NIceDb::TUpdate<Schema::ImportItems::PreparedCreationQuery>(item.PreparedCreationQuery->SerializeAsString())
        );
    }
}

void TSchemeShard::PersistImportItemDstPathId(NIceDb::TNiceDb& db, const TImportInfo& importInfo, ui32 itemIdx) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);

    db.Table<Schema::ImportItems>().Key(importInfo.Id, itemIdx).Update(
        NIceDb::TUpdate<Schema::ImportItems::DstPathOwnerId>(item.DstPathId.OwnerId),
        NIceDb::TUpdate<Schema::ImportItems::DstPathLocalId>(item.DstPathId.LocalPathId)
    );
}

void TSchemeShard::Handle(TEvImport::TEvCreateImportRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCreateImport(ev), ctx);
}

void TSchemeShard::Handle(TEvImport::TEvGetImportRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxGetImport(ev), ctx);
}

void TSchemeShard::Handle(TEvImport::TEvCancelImportRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCancelImport(ev), ctx);
}

void TSchemeShard::Handle(TEvImport::TEvForgetImportRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxForgetImport(ev), ctx);
}

void TSchemeShard::Handle(TEvImport::TEvListImportsRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxListImports(ev), ctx);
}

void TSchemeShard::Handle(TEvImport::TEvListObjectsInS3ExportRequest::TPtr& ev, const TActorContext&) {
    Register(CreateListObjectsInS3ExportGetter(std::move(ev)));
}

void TSchemeShard::Handle(TEvPrivate::TEvImportSchemeReady::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxProgressImport(ev), ctx);
}

void TSchemeShard::Handle(TEvPrivate::TEvImportSchemaMappingReady::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxProgressImport(ev), ctx);
}

void TSchemeShard::Handle(TEvPrivate::TEvImportSchemeQueryResult::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxProgressImport(ev), ctx);
}

void TSchemeShard::ResumeImports(const TVector<ui64>& ids, const TActorContext& ctx) {
    for (const ui64 id : ids) {
        Execute(CreateTxProgressImport(id), ctx);
    }
}

void TSchemeShard::WaitForTableProfiles(ui64 importId, ui32 itemIdx) {
    LOG_N("Wait for table profiles"
        << ": id# " << importId
        << ", itemIdx# " << itemIdx);
    TableProfilesWaiters.insert(std::make_pair(importId, itemIdx));
}

void TSchemeShard::LoadTableProfiles(const NKikimrConfig::TTableProfilesConfig* config, const TActorContext& ctx) {
    if (config) {
        LOG_N("Load table profiles");
        TableProfiles.Load(*config);
    } else {
        LOG_W("Table profiles were not loaded");
    }

    TableProfilesLoaded = true;
    auto waiters = std::move(TableProfilesWaiters);
    for (const auto& [importId, itemIdx] : waiters) {
        Execute(CreateTxProgressImport(importId, itemIdx), ctx);
    }
}

} // NSchemeShard
} // NKikimr
