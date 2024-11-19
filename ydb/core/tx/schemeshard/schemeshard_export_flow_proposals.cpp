#include "schemeshard_export_flow_proposals.h"
#include "schemeshard_path_describer.h"

#include <ydb/core/ydb_convert/compression.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NSchemeShard {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MkDirPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo::TPtr exportInfo
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());
    auto& record = propose->Record;

    if (exportInfo->UserSID) {
        record.SetOwner(*exportInfo->UserSID);
    }

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
    modifyScheme.SetInternal(true);

    const TPath domainPath = TPath::Init(exportInfo->DomainPathId, ss);
    modifyScheme.SetWorkingDir(domainPath.PathString());

    auto& mkDir = *modifyScheme.MutableMkDir();
    mkDir.SetName(Sprintf("export-%" PRIu64, exportInfo->Id));

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CopyTablesPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo::TPtr exportInfo
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());
    auto& record = propose->Record;

    if (exportInfo->UserSID) {
        record.SetOwner(*exportInfo->UserSID);
    }

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables);
    modifyScheme.SetInternal(true);

    auto& copyTables = *modifyScheme.MutableCreateConsistentCopyTables()->MutableCopyTableDescriptions();
    copyTables.Reserve(exportInfo->Items.size());

    const TPath exportPath = TPath::Init(exportInfo->ExportPathId, ss);
    const TString& exportPathName = exportPath.PathString();

    for (ui32 itemIdx : xrange(exportInfo->Items.size())) {
        const auto& item = exportInfo->Items.at(itemIdx);

        auto& desc = *copyTables.Add();
        desc.SetSrcPath(item.SourcePathName);
        desc.SetDstPath(ExportItemPathName(exportPathName, itemIdx));
        desc.SetOmitIndexes(true);
        desc.SetOmitFollowers(true);
        desc.SetIsBackup(true);
    }

    return propose;
}

static NKikimrSchemeOp::TPathDescription GetTableDescription(TSchemeShard* ss, const TPathId& pathId) {
    NKikimrSchemeOp::TDescribeOptions opts;
    opts.SetReturnPartitioningInfo(false);
    opts.SetReturnPartitionConfig(true);
    opts.SetReturnBoundaries(true);
    opts.SetReturnIndexTableBoundaries(true);

    auto desc = DescribePath(ss, TlsActivationContext->AsActorContext(), pathId, opts);
    auto record = desc->GetRecord();

    return record.GetPathDescription();
}

void FillSetValForSequences(TSchemeShard* ss, NKikimrSchemeOp::TTableDescription& description,
        const TPathId& exportItemPathId) {
    NKikimrSchemeOp::TDescribeOptions opts;
    opts.SetReturnSetVal(true);

    auto pathDescription = DescribePath(ss, TlsActivationContext->AsActorContext(), exportItemPathId, opts);
    auto tableDescription = pathDescription->GetRecord().GetPathDescription().GetTable();

    THashMap<TString, NKikimrSchemeOp::TSequenceDescription::TSetVal> setValForSequences;

    for (const auto& sequenceDescription : tableDescription.GetSequences()) {
        if (sequenceDescription.HasSetVal()) {
            setValForSequences[sequenceDescription.GetName()] = sequenceDescription.GetSetVal();
        }
    }

    for (auto& sequenceDescription : *description.MutableSequences()) {
        auto it = setValForSequences.find(sequenceDescription.GetName());
        if (it != setValForSequences.end()) {
            *sequenceDescription.MutableSetVal() = it->second;
        }
    }
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> BackupPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo::TPtr exportInfo,
    ui32 itemIdx
) {
    Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());

    auto& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpBackup);
    modifyScheme.SetInternal(true);

    const TPath exportPath = TPath::Init(exportInfo->ExportPathId, ss);
    const TString& exportPathName = exportPath.PathString();
    modifyScheme.SetWorkingDir(exportPathName);

    auto& task = *modifyScheme.MutableBackup();
    task.SetTableName(ToString(itemIdx));
    task.SetNeedToBill(!exportInfo->UserSID || !ss->SystemBackupSIDs.contains(*exportInfo->UserSID));

    const TPath sourcePath = TPath::Init(exportInfo->Items[itemIdx].SourcePathId, ss);
    const TPath exportItemPath = exportPath.Child(ToString(itemIdx));
    if (sourcePath.IsResolved() && exportItemPath.IsResolved()) {
        auto sourceDescription = GetTableDescription(ss, sourcePath.Base()->PathId);
        if (sourceDescription.HasTable()) {
            FillSetValForSequences(
                ss, *sourceDescription.MutableTable(), exportItemPath.Base()->PathId);
        }
        task.MutableTable()->CopyFrom(sourceDescription);
    }

    task.SetSnapshotStep(exportInfo->SnapshotStep);
    task.SetSnapshotTxId(exportInfo->SnapshotTxId);

    switch (exportInfo->Kind) {
    case TExportInfo::EKind::YT:
        {
            Ydb::Export::ExportToYtSettings exportSettings;
            Y_ABORT_UNLESS(exportSettings.ParseFromString(exportInfo->Settings));

            task.SetNumberOfRetries(exportSettings.number_of_retries());
            auto& backupSettings = *task.MutableYTSettings();
            backupSettings.SetHost(exportSettings.host());
            backupSettings.SetPort(exportSettings.port());
            backupSettings.SetToken(exportSettings.token());
            backupSettings.SetTablePattern(exportSettings.items(itemIdx).destination_path());
            backupSettings.SetUseTypeV3(exportSettings.use_type_v3());
        }
        break;

    case TExportInfo::EKind::S3:
        {
            Ydb::Export::ExportToS3Settings exportSettings;
            Y_ABORT_UNLESS(exportSettings.ParseFromString(exportInfo->Settings));

            task.SetNumberOfRetries(exportSettings.number_of_retries());
            auto& backupSettings = *task.MutableS3Settings();
            backupSettings.SetEndpoint(exportSettings.endpoint());
            backupSettings.SetBucket(exportSettings.bucket());
            backupSettings.SetAccessKey(exportSettings.access_key());
            backupSettings.SetSecretKey(exportSettings.secret_key());
            backupSettings.SetObjectKeyPattern(exportSettings.items(itemIdx).destination_prefix());
            backupSettings.SetStorageClass(exportSettings.storage_class());
            backupSettings.SetUseVirtualAddressing(!exportSettings.disable_virtual_addressing());

            switch (exportSettings.scheme()) {
            case Ydb::Export::ExportToS3Settings::HTTP:
                backupSettings.SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
                break;
            case Ydb::Export::ExportToS3Settings::HTTPS:
                backupSettings.SetScheme(NKikimrSchemeOp::TS3Settings::HTTPS);
                break;
            default:
                Y_ABORT("Unknown scheme");
            }

            if (const auto region = exportSettings.region()) {
                backupSettings.SetRegion(region);
            }

            if (const auto compression = exportSettings.compression()) {
                Y_ABORT_UNLESS(FillCompression(*task.MutableCompression(), compression));
            }
        }
        break;
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo::TPtr exportInfo,
    ui32 itemIdx
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());

    auto& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
    modifyScheme.SetInternal(true);

    const TPath exportPath = TPath::Init(exportInfo->ExportPathId, ss);
    modifyScheme.SetWorkingDir(exportPath.PathString());

    auto& drop = *modifyScheme.MutableDrop();
    drop.SetName(ToString(itemIdx));

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo::TPtr exportInfo
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());

    auto& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpRmDir);
    modifyScheme.SetInternal(true);

    const TPath domainPath = TPath::Init(exportInfo->DomainPathId, ss);
    modifyScheme.SetWorkingDir(domainPath.PathString());

    auto& drop = *modifyScheme.MutableDrop();
    drop.SetName(Sprintf("export-%" PRIu64, exportInfo->Id));

    return propose;
}

THolder<TEvSchemeShard::TEvCancelTx> CancelPropose(
    const TExportInfo::TPtr exportInfo,
    TTxId backupTxId
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvCancelTx>();

    auto& record = propose->Record;
    record.SetTxId(exportInfo->Id);
    record.SetTargetTxId(ui64(backupTxId));

    return propose;
}

TString ExportItemPathName(TSchemeShard* ss, const TExportInfo::TPtr exportInfo, ui32 itemIdx) {
    const TPath exportPath = TPath::Init(exportInfo->ExportPathId, ss);
    return ExportItemPathName(exportPath.PathString(), itemIdx);
}

TString ExportItemPathName(const TString& exportPathName, ui32 itemIdx) {
    return TStringBuilder() << exportPathName << "/" << itemIdx;
}

} // NSchemeShard
} // NKikimr
