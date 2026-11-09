#include "schemeshard_export_flow_proposals.h"

#include "schemeshard_path_describer.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/backup/common/index_metadata.h>
#include <ydb/core/backup/common/fields_wrappers.h>
#include <ydb/core/protos/data_format_settings.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/ydb_convert/compression.h>
#include <ydb/core/ydb_convert/table_description.h>
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
        if (!IsPathTypeTable(item)) {
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

static TConclusion<TVector<NBackup::TIndexMetadata>> GetTableIndexesExportMetadata(const NKikimrSchemeOp::TTableDescription& table, bool encrypted) {
    TString error;
    if (!ValidateIndexImplTableDescriptions(table, error)) {
        return TConclusionStatus::Fail(error);
    }

    // Must match the list that DataShard writes into the table metadata.json
    // (see TS3Export::CreateUploader and GenRowTableScheme): DataShard numbers
    // anonymized index prefixes after the exported changefeeds, lists only the
    // indexes it can convert and lists nothing if it fails to build the scheme.
    Ydb::Table::CreateTableRequest scheme;
    NKikimrMiniKQL::TType mkqlKeyType;
    try {
        FillColumnDescription(scheme, mkqlKeyType, table);
        FillTableBoundary(scheme, table, mkqlKeyType);
        FillIndexDescription(scheme, table);
    } catch (const yexception&) {
        return TVector<NBackup::TIndexMetadata>{};
    }

    const auto& featureFlags = AppData()->FeatureFlags;
    const ui32 changefeedsCount = featureFlags.GetEnableChangefeedsExport() ? table.CdcStreamsSize() : 0;
    return NBackup::MakeIndexesMetadata(scheme.indexes(), changefeedsCount + 1, encrypted,
        featureFlags.GetEnableCompactFulltextIndex());
}

static void FillChangefeedTopics(TSchemeShard* ss, const NKikimrSchemeOp::TTableDescription& table,
    NKikimrSchemeOp::TBackupTask& task)
{
    for (const auto& cdcStream : table.GetCdcStreams()) {
        const auto cdcPathDesc = GetDescription(ss, TPathId::FromProto(cdcStream.GetPathId()));
        for (const auto& child : cdcPathDesc.GetChildren()) {
            if (child.GetPathType() == NKikimrSchemeOp::EPathTypePersQueueGroup) {
                *task.AddChangefeedUnderlyingTopics() =
                    GetDescription(ss, TPathId(child.GetSchemeshardId(), child.GetPathId()));
            }
        }
    }
}

TConclusion<TVector<NBackup::TIndexMetadata>> GetTableIndexesExportMetadata(TSchemeShard* ss, const TPathId& tablePathId,
    bool encrypted, NKikimrSchemeOp::TBackupTask* snapshot)
{
    const auto description = GetDescription(ss, tablePathId);
    const auto& table = description.GetTable();
    auto indexes = GetTableIndexesExportMetadata(table, encrypted);
    if (indexes && snapshot) {
        snapshot->Clear();
        auto& savedTable = *snapshot->MutableTable()->MutableTable();
        *savedTable.MutableTableIndexes() = table.GetTableIndexes();
        if (AppData()->FeatureFlags.GetEnableChangefeedsExport()) {
            *savedTable.MutableCdcStreams() = table.GetCdcStreams();
            FillChangefeedTopics(ss, table, *snapshot);
        }
    }
    return indexes;
}

static TConclusion<TString> ComputeIndexItemSuffix(
    TSchemeShard* ss,
    const TExportInfo& exportInfo,
    const TExportInfo::TItem& item,
    ui32 itemIdx,
    bool encrypted)
{
    if (!encrypted) {
        return item.SourcePathName;
    }

    Y_ABORT_UNLESS(item.ParentIdx < exportInfo.Items.size());
    const TString unlistedPrefix = TStringBuilder() << "unlisted_" << itemIdx;

    if (item.ParentIdx < exportInfo.ExportMetadata.SchemaMappingSize()) {
        const auto& parentMapping = exportInfo.ExportMetadata.GetSchemaMapping(item.ParentIdx);
        if (parentMapping.HasIndexExportPrefixes()) {
            // Fixed by FillExportMetadata, DataShard lists the same prefixes in the table metadata
            for (const auto& index : parentMapping.GetIndexExportPrefixes().GetIndexes()) {
                if (index.GetImplTablePrefix() == item.SourcePathName) {
                    return index.GetExportPrefix();
                }
            }
            // Never referenced by the table metadata
            return unlistedPrefix;
        }
    }

    // Exports created by older versions: compute the prefix from the current scheme
    const auto& parentItem = exportInfo.Items[item.ParentIdx];
    auto parentPath = TPath::Init(parentItem.SourcePathId, ss);
    if (parentPath.IsResolved()) {
        const auto indexes = GetTableIndexesExportMetadata(ss, parentPath.Base()->PathId, encrypted);
        if (!indexes) {
            return indexes.GetError();
        }
        for (const auto& index : indexes.GetResult()) {
            if (index.ImplTablePrefix == item.SourcePathName) {
                return index.ExportPrefix;
            }
        }
    }

    // Upload to a unique prefix that is never referenced by the table metadata
    return unlistedPrefix;
}

template <typename TSettings>
TConclusion<TString> ComputeIndexItemPath(
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
        auto suffix = ComputeIndexItemSuffix(ss, exportInfo, item, itemIdx,
            exportSettings.has_encryption_settings());
        if (!suffix) {
            return suffix.GetError();
        }
        itemPath += suffix.GetResult();
    }
    return itemPath;
}

TConclusion<TString> ExportItemDestinationPrefix(TSchemeShard* ss, const TExportInfo& exportInfo, ui32 itemIdx) {
    Ydb::Export::ExportToS3Settings settings;
    Y_ABORT_UNLESS(settings.ParseFromString(exportInfo.Settings));
    return ComputeIndexItemPath(ss, exportInfo.Items.at(itemIdx), itemIdx, exportInfo, settings);
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
        FillChangefeedTopics(ss, sourceDescription.GetTable(), task);
    }

    task.MutableTable()->CopyFrom(sourceDescription);
}

TString GetExportItemIV(const TExportInfo& exportInfo, ui32 itemIdx) {
    const auto& item = exportInfo.Items.at(itemIdx);
    if (item.ParentIdx == Max<ui32>()) {
        Y_ABORT_UNLESS(itemIdx < exportInfo.ExportMetadata.SchemaMappingSize());
        return exportInfo.ExportMetadata.GetSchemaMapping(itemIdx).GetIV();
    }

    // Index impl tables are not independent objects in SchemaMapping: their IVs
    // are stored in the parent's mapping item by FillExportMetadata. Use exactly
    // the uploaded IV so that the data can always be decrypted on import.
    Y_ABORT_UNLESS(item.ParentIdx < exportInfo.ExportMetadata.SchemaMappingSize());
    const auto& indexIVs = exportInfo.ExportMetadata.GetSchemaMapping(item.ParentIdx).GetIndexIVs().GetIVs();
    if (const auto it = indexIVs.find(item.SourcePathName); it != indexIVs.end()) {
        return it->second;
    }

    // Exports created by older versions have no index IVs in the metadata
    // (such versions crash on encrypted index items). Let them finish after
    // upgrade instead of crashing again: the IV is still unique, but import
    // of this index data will be rejected because the schema mapping lacks it.
    return MakeExportIndexItemIV(
        NBackup::TEncryptionIV::FromBinaryString(exportInfo.ExportMetadata.GetIV()), itemIdx).GetBinaryString();
}

NBackup::TEncryptionIV MakeExportIndexItemIV(const NBackup::TEncryptionIV& exportIV, ui32 itemIdx) {
    // Index impl-table items are appended after the user items, so their item
    // numbers continue the numbering of SchemaMapping items
    return NBackup::TEncryptionIV::Combine(exportIV, NBackup::EBackupFileType::Metadata, itemIdx + 1, 0);
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
        encryptionSettings.SetIV(GetExportItemIV(exportInfo, itemIdx));
        *encryptionSettings.MutableSymmetricKey() = exportSettings.encryption_settings().symmetric_key();

        const bool isTableItem = exportInfo.Items.at(itemIdx).ParentIdx == Max<ui32>();
        if (isTableItem && itemIdx < exportInfo.ExportMetadata.SchemaMappingSize()) {
            const auto& mapping = exportInfo.ExportMetadata.GetSchemaMapping(itemIdx);
            if (mapping.HasIndexExportPrefixes()) {
                *task.MutableIndexExportPrefixes() = mapping.GetIndexExportPrefixes();
            }
            if (mapping.HasIndexExportSnapshot()) {
                // Older receivers derive prefixes from these existing fields.
                // Keep their inputs consistent with the frozen index prefixes,
                // even if changefeeds or indexes were added in the meantime.
                const auto& snapshot = mapping.GetIndexExportSnapshot();
                auto& table = *task.MutableTable()->MutableTable();
                table.MutableTableIndexes()->CopyFrom(snapshot.GetTable().GetTable().GetTableIndexes());
                table.MutableCdcStreams()->CopyFrom(snapshot.GetTable().GetTable().GetCdcStreams());
                task.MutableChangefeedUnderlyingTopics()->CopyFrom(snapshot.GetChangefeedUnderlyingTopics());
            }
        }
    }
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> BackupPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TExportInfo& exportInfo,
    ui32 itemIdx,
    TString& error
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
        modifyScheme.SetWorkingDir(exportPath.PathString());
        task.SetTableName(ToString(itemIdx));

        FillTableDescription(ss, task, TPath::Init(item.SourcePathId, ss), exportPath.Child(ToString(itemIdx)));
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

    // S3/FS convert the full scheme on DataShard. YT only exports the columns
    // and does not need implementation table descriptions.
    if (exportInfo.Kind != TExportInfo::EKind::YT
        && !ValidateIndexImplTableDescriptions(task.GetTable().GetTable(), error)) {
        return nullptr;
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

            auto prefix = ComputeIndexItemPath(ss, item, itemIdx, exportInfo, exportSettings);
            if (!prefix) {
                error = prefix.GetErrorMessage();
                return nullptr;
            }
            backupSettings.SetObjectKeyPattern(prefix.GetResult());

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

            switch (exportSettings.format_case()) {
            case Ydb::Export::ExportToS3Settings::kParquet:
                {
                    auto parquet = backupSettings.MutableExportDataSettings()->MutableParquet();
                    if (exportSettings.parquet().row_group_size()) {
                        parquet->SetRowGroupSize(exportSettings.parquet().row_group_size());
                    }
                    break;
                }
            case Ydb::Export::ExportToS3Settings::kYdbDump:
            case Ydb::Export::ExportToS3Settings::FORMAT_NOT_SET:
                // YdbDump format - empty message
                backupSettings.MutableExportDataSettings()->MutableYdbDump();
                break;
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
            auto prefix = ComputeIndexItemPath(ss, item, itemIdx, exportInfo, exportSettings);
            if (!prefix) {
                error = prefix.GetErrorMessage();
                return nullptr;
            }
            backupSettings.SetPath(prefix.GetResult());
            
            // TODO: Parquet format support for FS will be added after public API approval

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
        if (IsPathTypeTable(item) && itemPath.IsResolved() && !itemPath.IsDeleted()) {
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
