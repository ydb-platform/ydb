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
            // TODO(ilnaz): fill items progress
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_TRANSFER_DATA);
            break;
        case TImportInfo::EState::BuildIndexes:
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_BUILD_INDEXES);
            break;
        case TImportInfo::EState::CreateChangefeed:
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_CREATE_CHANGEFEEDS);
            break;
        case TImportInfo::EState::Done:
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_DONE);
            break;
        default:
            import.SetProgress(Ydb::Import::ImportProgress::PROGRESS_UNSPECIFIED);
            break;
        }
        break;

    case TImportInfo::EState::Done:
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
        NIceDb::TUpdate<Schema::Imports::Items>(importInfo.Items.size())
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
