#include "schemeshard_export_flow_proposals.h"

#include "schemeshard_path_describer.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/backup/common/fields_wrappers.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/ydb_convert/compression.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NSchemeShard {

THolder<TEvSchemeShard::TEvModifySchemeTransaction> MkDirPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo
) {
    auto propose = MakeModifySchemeTransaction(ss, txId, exportInfo);
    auto& record = propose->Record;

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
    auto propose = MakeModifySchemeTransaction(ss, txId, exportInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables);
    modifyScheme.SetInternal(true);

    auto& copyTables = *modifyScheme.MutableCreateConsistentCopyTables()->MutableCopyTableDescriptions();
    copyTables.Reserve(exportInfo.Items.size());

    for (ui32 itemIdx : xrange(exportInfo.Items.size())) {
        const auto& item = exportInfo.Items.at(itemIdx);
        if (item.SourcePathType != NKikimrSchemeOp::EPathTypeTable) {
            continue;
        }

        auto& desc = *copyTables.Add();
        desc.SetSrcPath(item.SourcePathName);
        desc.SetDstPath(ExportItemPathName(ss, exportInfo, itemIdx));
        desc.SetOmitIndexes(!exportInfo.IncludeIndexData);
        desc.SetOmitFollowers(true);
        desc.SetIsBackup(true);
    }

    return propose;
}

static NKikimrSchemeOp::TPathDescription GetDescription(TSchemeShard* ss, const TPathId& pathId) {
    NKikimrSchemeOp::TDescribeOptions opts;
    opts.SetReturnPartitioningInfo(false);
    opts.SetReturnPartitionConfig(true);
    opts.SetReturnBoundaries(true);
    opts.SetReturnIndexTableBoundaries(true);
    opts.SetShowPrivateTable(true);

    auto desc = DescribePath(ss, TlsActivationContext->AsActorContext(), pathId, opts);
    auto record = desc->GetRecord();

    return record.GetPathDescription();
}

static TString ComputeIndexItemSuffix(
    TSchemeShard* ss,
    const TExportInfo& exportInfo,
    const TExportInfo::TItem& item,
    bool encrypted)
{
    if (!encrypted) {
        return item.SourcePathName;
    }

    static constexpr int INVALID_IDX = 999;
    int idx = INVALID_IDX;
    bool found = false;

    Y_ABORT_UNLESS(item.ParentIdx < exportInfo.Items.size());
    const auto& parentItem = exportInfo.Items[item.ParentIdx];

    auto parentPath = TPath::Init(parentItem.SourcePathId, ss);
    TStringBuf indexName;
    TStringBuf implTableName;
    if (parentPath.IsResolved() && TStringBuf(item.SourcePathName).TrySplit('/', indexName, implTableName)) {
        const auto parentDescription = GetDescription(ss, parentPath.Base()->PathId);
        idx = parentDescription.GetTable().CdcStreamsSize() + 1;

        for (const auto& index : parentDescription.GetTable().GetTableIndexes()) {
            const TVector<TString> indexColumns(index.GetKeyColumnNames().begin(), index.GetKeyColumnNames().end());

            const auto implTables = NTableIndex::GetImplTables(index.GetType(), indexColumns);
            if (index.GetName() != indexName) {
                idx += implTables.size();
                continue;
            }

            for (const auto& implTable : implTables) {
                if (implTable != implTableName) {
                    ++idx;
                    continue;
                }

                found = true;
                break;
            }

            if (found) {
                break;
            }
        }
    }

    std::stringstream ss2;
    ss2 << std::setfill('0') << std::setw(3) << std::right << (found ? idx : INVALID_IDX);
    return ss2.str();
}

template <typename TSettings>
TString ComputeIndexItemPath(
    TSchemeShard* ss,
    const TExportInfo::TItem& item,
    ui32 itemIdx, const TExportInfo& exportInfo,
    const TSettings& exportSettings)
{
    TString itemPath;
    if (item.ParentIdx == Max<ui32>()) {
        Y_ABORT_UNLESS(itemIdx < (ui32)exportSettings.items().size());
        itemPath = NBackup::NFieldsWrappers::GetItemDestination(exportSettings.items(itemIdx));
    } else {
        Y_ABORT_UNLESS(item.ParentIdx < (ui32)exportSettings.items().size());
        itemPath = NBackup::NFieldsWrappers::GetItemDestination(exportSettings.items(item.ParentIdx));
        if (!itemPath.empty() && itemPath.back() != '/') {
            itemPath += '/';
        }
        itemPath += ComputeIndexItemSuffix(ss, exportInfo, item,
            exportSettings.has_encryption_settings());
    }
    return itemPath;
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
    auto copiedPath = GetDescription(ss, exportItemPathId);
    const auto& copiedTable = copiedPath.GetTable();

    *desc.MutableSplitBoundary() = copiedTable.GetSplitBoundary();
    *desc.MutablePartitionConfig()->MutablePartitioningPolicy() = copiedTable.GetPartitionConfig().GetPartitioningPolicy();
}

void FillTableDescription(TSchemeShard* ss, NKikimrSchemeOp::TBackupTask& task, const TPath& sourcePath, const TPath& exportItemPath) {
    if (!sourcePath.IsResolved() || (!sourcePath->IsColumnTable() && !exportItemPath.IsResolved())) {
        return;
    }

    auto sourceDescription = GetDescription(ss, sourcePath.Base()->PathId);
    if (sourceDescription.HasTable()) {
        FillSetValForSequences(
            ss, *sourceDescription.MutableTable(), exportItemPath.Base()->PathId);
        FillPartitioning(ss, *sourceDescription.MutableTable(), exportItemPath.Base()->PathId);
        for (const auto& cdcStream : sourceDescription.GetTable().GetCdcStreams()) {
            auto cdcPathDesc =  GetDescription(ss, TPathId::FromProto(cdcStream.GetPathId()));
            for (const auto& child : cdcPathDesc.GetChildren()) {
                if (child.GetPathType() == NKikimrSchemeOp::EPathTypePersQueueGroup) {
                    *task.AddChangefeedUnderlyingTopics() =
                        GetDescription(ss, TPathId(child.GetSchemeshardId(), child.GetPathId()));
                }
            }
        }
    }

    task.MutableTable()->CopyFrom(sourceDescription);
}

template <typename TSettings>
void FillEncryptionSettings(
    NKikimrSchemeOp::TBackupTask& task,
    const TSettings& exportSettings,
    const TExportInfo& exportInfo,
    ui32 itemIdx)
{
    if (exportSettings.has_encryption_settings()) {
        auto& encryptionSettings = *task.MutableEncryptionSettings();
        encryptionSettings.SetEncryptionAlgorithm(exportInfo.ExportMetadata.GetEncryptionAlgorithm());
        Y_ABORT_UNLESS(itemIdx < exportInfo.ExportMetadata.SchemaMappingSize());
        encryptionSettings.SetIV(exportInfo.ExportMetadata.GetSchemaMapping(itemIdx).GetIV());
        *encryptionSettings.MutableSymmetricKey() = exportSettings.encryption_settings().symmetric_key();
    }
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> BackupPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo,
    ui32 itemIdx
) {
    Y_ABORT_UNLESS(itemIdx < exportInfo.Items.size());
    const auto& item = exportInfo.Items[itemIdx];

    auto propose = MakeModifySchemeTransaction(ss, txId, exportInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpBackup);
    modifyScheme.SetInternal(true);

    const TPath exportPath = TPath::Init(exportInfo.ExportPathId, ss);
    auto& task = *modifyScheme.MutableBackup();

    if (item.ParentIdx == Max<ui32>()) {
        const TPath sourcePath = TPath::Init(item.SourcePathId, ss);
        TString exportPathName;
        TString tableName;
        if (sourcePath.IsResolved() && sourcePath->IsColumnTable()) {
            exportPathName = sourcePath.Parent().PathString();
            tableName = sourcePath->Name;
        } else {
            exportPathName = exportPath.PathString();
            tableName = ToString(itemIdx);
        }
        modifyScheme.SetWorkingDir(exportPathName);
        task.SetTableName(tableName);

        FillTableDescription(ss, task, sourcePath, exportPath.Child(ToString(itemIdx)));
    } else {
        auto parentPath = exportPath.Child(ToString(item.ParentIdx));

        auto childParts = SplitPath(item.SourcePathName);
        Y_ABORT_UNLESS(!childParts.empty());

        auto childName = std::move(childParts.back());
        childParts.pop_back();

        for (const auto& part : childParts) {
            parentPath.Dive(part);
        }

        modifyScheme.SetWorkingDir(parentPath.PathString());
        task.SetTableName(childName);

        FillTableDescription(ss, task, TPath::Init(item.SourcePathId, ss), parentPath.Child(childName));
    }

    task.SetNeedToBill(!exportInfo.UserSID || !ss->SystemBackupSIDs.contains(*exportInfo.UserSID));
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
            backupSettings.SetUseTypeV3(exportSettings.use_type_v3());

            Y_ABORT_UNLESS(itemIdx < (ui32)exportSettings.items().size());
            backupSettings.SetTablePattern(exportSettings.items(itemIdx).destination_path());
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
            backupSettings.SetStorageClass(exportSettings.storage_class());
            backupSettings.SetUseVirtualAddressing(!exportSettings.disable_virtual_addressing());



            backupSettings.SetObjectKeyPattern(ComputeIndexItemPath(ss, item, itemIdx, exportInfo, exportSettings));

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

            FillEncryptionSettings(task, exportSettings, exportInfo, itemIdx);
        }
        break;
    case TExportInfo::EKind::FS:
        {
            Ydb::Export::ExportToFsSettings exportSettings;
            Y_ABORT_UNLESS(exportSettings.ParseFromString(exportInfo.Settings));

            task.SetNumberOfRetries(exportSettings.number_of_retries());
            auto& backupSettings = *task.MutableFSSettings();
            backupSettings.SetBasePath(exportSettings.base_path());
            backupSettings.SetPath(ComputeIndexItemPath(ss, item, itemIdx, exportInfo, exportSettings));

            if (const auto compression = exportSettings.compression()) {
                Y_ABORT_UNLESS(FillCompression(*task.MutableCompression(), compression));
            }

            task.SetEnableChecksums(exportInfo.EnableChecksums);
            task.SetEnablePermissions(exportInfo.EnablePermissions);

            FillEncryptionSettings(task, exportSettings, exportInfo, itemIdx);
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
    auto propose = MakeModifySchemeTransaction(ss, txId, exportInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
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
    auto propose = MakeModifySchemeTransaction(ss, txId, exportInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
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
    Y_ABORT_UNLESS(itemIdx < exportInfo.Items.size());
    const auto& item = exportInfo.Items[itemIdx];

    const TPath exportPath = TPath::Init(exportInfo.ExportPathId, ss);
    if (item.ParentIdx == Max<ui32>()) {
        return TStringBuilder() << exportPath.PathString() << "/" << itemIdx;
    } else {
        return TStringBuilder() << exportPath.PathString() << "/" << item.ParentIdx << "/" << item.SourcePathName;
    }
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
        if (item.SourcePathType == NKikimrSchemeOp::EPathTypeTable && itemPath.IsResolved() && !itemPath.IsDeleted()) {
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
