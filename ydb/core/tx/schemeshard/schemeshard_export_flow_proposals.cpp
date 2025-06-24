#include "schemeshard_export_flow_proposals.h"

#include "schemeshard_path_describer.h"

#include <ydb/public/api/protos/ydb_export.pb.h>

#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/ydb_convert/compression.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NSchemeShard {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MkDirPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());
    auto& record = propose->Record;

    if (exportInfo.UserSID) {
        record.SetOwner(*exportInfo.UserSID);
    }

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
    modifyScheme.SetInternal(true);

    const TPath domainPath = TPath::Init(exportInfo.DomainPathId, ss);
    modifyScheme.SetWorkingDir(domainPath.PathString());

    auto& mkDir = *modifyScheme.MutableMkDir();
    mkDir.SetName(Sprintf("export-%" PRIu64, exportInfo.Id));

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CopyTablesPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());
    auto& record = propose->Record;

    if (exportInfo.UserSID) {
        record.SetOwner(*exportInfo.UserSID);
    }

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables);
    modifyScheme.SetInternal(true);

    auto& copyTables = *modifyScheme.MutableCreateConsistentCopyTables()->MutableCopyTableDescriptions();
    copyTables.Reserve(exportInfo.Items.size());

    const TPath exportPath = TPath::Init(exportInfo.ExportPathId, ss);
    const TString& exportPathName = exportPath.PathString();

    for (ui32 itemIdx : xrange(exportInfo.Items.size())) {
        const auto& item = exportInfo.Items.at(itemIdx);
        if (item.SourcePathType != NKikimrSchemeOp::EPathTypeTable) {
            continue;
        }

        auto& desc = *copyTables.Add();
        desc.SetSrcPath(item.SourcePathName);
        desc.SetDstPath(ExportItemPathName(exportPathName, itemIdx));
        desc.SetOmitIndexes(true);
        desc.SetOmitFollowers(true);
        desc.SetIsBackup(true);
    }

    return propose;
}

static void SetTableDescriptionOptions(NKikimrSchemeOp::TDescribeOptions& opts) {
    opts.SetReturnPartitioningInfo(false);
    opts.SetReturnPartitionConfig(true);
    opts.SetReturnBoundaries(true);
    opts.SetReturnIndexTableBoundaries(true);
}

static void SetChangefeedDescriptionOptions(NKikimrSchemeOp::TDescribeOptions& opts) {
    SetTableDescriptionOptions(opts);
    opts.SetShowPrivateTable(true);
}

static void SetTopicDescriptionOptions(NKikimrSchemeOp::TDescribeOptions& opts) {
    SetTableDescriptionOptions(opts);
    opts.SetShowPrivateTable(true);
}

static NKikimrSchemeOp::TPathDescription GetDescription(TSchemeShard* ss, const TPathId& pathId, NKikimrSchemeOp::TDescribeOptions& opts) {
    auto desc = DescribePath(ss, TlsActivationContext->AsActorContext(), pathId, opts);
    auto record = desc->GetRecord();

    return record.GetPathDescription();
}

static NKikimrSchemeOp::TPathDescription GetTableDescription(TSchemeShard* ss, const TPathId& pathId) {
    NKikimrSchemeOp::TDescribeOptions opts;
    SetTableDescriptionOptions(opts);
    return GetDescription(ss, pathId, opts);
}

static NKikimrSchemeOp::TPathDescription GetChangefeedDescription(TSchemeShard* ss, const TPathId& pathId) {
    NKikimrSchemeOp::TDescribeOptions opts;
    SetChangefeedDescriptionOptions(opts);
    return GetDescription(ss, pathId, opts);
}

static NKikimrSchemeOp::TPathDescription GetTopicDescription(TSchemeShard* ss, const TPathId& pathId) {
    NKikimrSchemeOp::TDescribeOptions opts;
    SetTopicDescriptionOptions(opts);
    return GetDescription(ss, pathId, opts);
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

void FillPartitioning(TSchemeShard* ss, NKikimrSchemeOp::TTableDescription& desc, const TPathId& exportItemPathId) {
    NKikimrSchemeOp::TDescribeOptions opts;
    opts.SetReturnPartitionConfig(true);
    opts.SetReturnBoundaries(true);

    auto copiedPath = DescribePath(ss, TlsActivationContext->AsActorContext(), exportItemPathId, opts);
    const auto& copiedTable = copiedPath->GetRecord().GetPathDescription().GetTable();

    *desc.MutableSplitBoundary() = copiedTable.GetSplitBoundary();
    *desc.MutablePartitionConfig()->MutablePartitioningPolicy() = copiedTable.GetPartitionConfig().GetPartitioningPolicy();
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> BackupPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo,
    ui32 itemIdx
) {
    Y_ABORT_UNLESS(itemIdx < exportInfo.Items.size());

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());

    auto& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpBackup);
    modifyScheme.SetInternal(true);

    const TPath exportPath = TPath::Init(exportInfo.ExportPathId, ss);
    const TString& exportPathName = exportPath.PathString();
    modifyScheme.SetWorkingDir(exportPathName);

    auto& task = *modifyScheme.MutableBackup();
    task.SetTableName(ToString(itemIdx));
    task.SetNeedToBill(!exportInfo.UserSID || !ss->SystemBackupSIDs.contains(*exportInfo.UserSID));

    const TPath sourcePath = TPath::Init(exportInfo.Items[itemIdx].SourcePathId, ss);
    const TPath exportItemPath = exportPath.Child(ToString(itemIdx));
    if (sourcePath.IsResolved() && exportItemPath.IsResolved()) {
        auto sourceDescription = GetTableDescription(ss, sourcePath.Base()->PathId);
        if (sourceDescription.HasTable()) {
            FillSetValForSequences(
                ss, *sourceDescription.MutableTable(), exportItemPath.Base()->PathId);
            FillPartitioning(ss, *sourceDescription.MutableTable(), exportItemPath.Base()->PathId);
            for (const auto& cdcStream : sourceDescription.GetTable().GetCdcStreams()) {
                auto cdcPathDesc =  GetChangefeedDescription(ss, TPathId::FromProto(cdcStream.GetPathId()));
                for (const auto& child : cdcPathDesc.GetChildren()) {
                    if (child.GetPathType() == NKikimrSchemeOp::EPathTypePersQueueGroup) {
                        *task.AddChangefeedUnderlyingTopics() = GetTopicDescription(ss, TPathId(child.GetSchemeshardId(), child.GetPathId()));
                    }
                }
            }
        }
        task.MutableTable()->CopyFrom(sourceDescription);
    }

    task.SetSnapshotStep(exportInfo.SnapshotStep);
    task.SetSnapshotTxId(exportInfo.SnapshotTxId);

    switch (exportInfo.Kind) {
    case TExportInfo::EKind::YT:
        {
            Ydb::Export::ExportToYtSettings exportSettings;
            Y_ABORT_UNLESS(exportSettings.ParseFromString(exportInfo.Settings));

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
            Y_ABORT_UNLESS(exportSettings.ParseFromString(exportInfo.Settings));

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

            task.SetEnableChecksums(exportInfo.EnableChecksums);
            task.SetEnablePermissions(exportInfo.EnablePermissions);

            if (exportSettings.has_encryption_settings()) {
                auto& encryptionSettings = *task.MutableEncryptionSettings();
                encryptionSettings.SetEncryptionAlgorithm(exportInfo.ExportMetadata.GetEncryptionAlgorithm());
                Y_ABORT_UNLESS(itemIdx < exportInfo.ExportMetadata.SchemaMappingSize());
                encryptionSettings.SetIV(exportInfo.ExportMetadata.GetSchemaMapping(itemIdx).GetIV());
                *encryptionSettings.MutableSymmetricKey() = exportSettings.encryption_settings().symmetric_key();
            }
        }
        break;
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo,
    ui32 itemIdx
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());

    auto& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
    modifyScheme.SetInternal(true);

    const TPath exportPath = TPath::Init(exportInfo.ExportPathId, ss);
    modifyScheme.SetWorkingDir(exportPath.PathString());

    auto& drop = *modifyScheme.MutableDrop();
    drop.SetName(ToString(itemIdx));

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), ss->TabletID());

    auto& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpRmDir);
    modifyScheme.SetInternal(true);

    const TPath domainPath = TPath::Init(exportInfo.DomainPathId, ss);
    modifyScheme.SetWorkingDir(domainPath.PathString());

    auto& drop = *modifyScheme.MutableDrop();
    drop.SetName(Sprintf("export-%" PRIu64, exportInfo.Id));

    return propose;
}

THolder<TEvSchemeShard::TEvCancelTx> CancelPropose(
    const TExportInfo& exportInfo,
    TTxId backupTxId
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvCancelTx>();

    auto& record = propose->Record;
    record.SetTxId(exportInfo.Id);
    record.SetTargetTxId(ui64(backupTxId));

    return propose;
}

TString ExportItemPathName(TSchemeShard* ss, const TExportInfo& exportInfo, ui32 itemIdx) {
    const TPath exportPath = TPath::Init(exportInfo.ExportPathId, ss);
    return ExportItemPathName(exportPath.PathString(), itemIdx);
}

TString ExportItemPathName(const TString& exportPathName, ui32 itemIdx) {
    return TStringBuilder() << exportPathName << "/" << itemIdx;
}

void PrepareDropping(
        TSchemeShard* ss,
        TExportInfo& exportInfo,
        NIceDb::TNiceDb& db,
        TExportInfo::EState droppingState,
        std::function<void(ui64)> func)
{
    Y_ABORT_UNLESS(IsIn({TExportInfo::EState::AutoDropping, TExportInfo::EState::Dropping}, droppingState));

    exportInfo.WaitTxId = InvalidTxId;
    exportInfo.State = droppingState;
    ss->PersistExportState(db, exportInfo);

    for (ui32 itemIdx : xrange(exportInfo.Items.size())) {
        auto& item = exportInfo.Items.at(itemIdx);

        item.WaitTxId = InvalidTxId;
        item.State = TExportInfo::EState::Dropped;
        const TPath itemPath = TPath::Resolve(ExportItemPathName(ss, exportInfo, itemIdx), ss);
        if (itemPath.IsResolved() && !itemPath.IsDeleted()) {
            item.State = TExportInfo::EState::Dropping;
            if (exportInfo.State == TExportInfo::EState::AutoDropping) {
                func(itemIdx);
            }
        }

        ss->PersistExportItemState(db, exportInfo, itemIdx);
    }
}

void PrepareDropping(TSchemeShard* ss, TExportInfo& exportInfo, NIceDb::TNiceDb& db) {
    PrepareDropping(ss, exportInfo, db, TExportInfo::EState::Dropping, [](ui64){});
}

} // NSchemeShard
} // NKikimr
