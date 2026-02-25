#include "schemeshard_import_flow_proposals.h"
#include "schemeshard_import_helpers.h"

#include "schemeshard_path_describer.h"
#include "schemeshard_xxport__helpers.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/services/lib/actors/pq_schema_actor.h>

namespace NKikimr {
namespace NSchemeShard {

static bool FillDefaultValues(
    const NKikimr::NSchemeShard::TImportInfo::TItem& item,
    ::NKikimrSchemeOp::TIndexedTableCreationConfig& indexedTable,
    TString& error)
{
    for (const auto& column : item.Table->columns()) {
        switch (column.default_value_case()) {
            case Ydb::Table::ColumnMeta::kFromSequence: {
                const auto& fromSequence = column.from_sequence();
                Ydb::StatusIds::StatusCode status;
                auto* seqDesc = indexedTable.MutableSequenceDescription()->Add();
                if (!FillSequenceDescription(*seqDesc, fromSequence, status, error)) {
                    return false;
                }

                break;
            }
            case Ydb::Table::ColumnMeta::kFromLiteral:
                break;
            default:
                break;
        }
    }
    return true;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);
    Y_ABORT_UNLESS(item.Table);

    auto propose = MakeModifySchemeTransaction(ss, txId, importInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    const bool isColumnTable = item.Table->store_type() == Ydb::Table::STORE_TYPE_COLUMN;
    modifyScheme.SetOperationType(isColumnTable ? NKikimrSchemeOp::ESchemeOpCreateColumnTable : NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
    modifyScheme.SetInternal(true);

    const TPath domainPath = TPath::Init(importInfo.DomainPathId, ss);

    std::pair<TString, TString> wdAndPath;
    if (!TrySplitPathByDb(item.DstPathName, domainPath.PathString(), wdAndPath, error)) {
        return nullptr;
    }

    modifyScheme.SetWorkingDir(wdAndPath.first);

    if (isColumnTable) {
        auto& tableDesc = *modifyScheme.MutableCreateColumnTable();
        tableDesc.SetName(wdAndPath.second);
        tableDesc.SetIsRestore(true);

        Y_ABORT_UNLESS(ss->TableProfilesLoaded);
        Ydb::StatusIds::StatusCode status;
        if (!FillColumnTableDescription(modifyScheme, *item.Table, status, error)) {
            return nullptr;
        }
    } else {
        auto& indexedTable = *modifyScheme.MutableCreateIndexedTable();
        auto& tableDesc = *indexedTable.MutableTableDescription();
        tableDesc.SetName(wdAndPath.second);
        tableDesc.SetIsRestore(true);

        Y_ABORT_UNLESS(ss->TableProfilesLoaded);
        Ydb::StatusIds::StatusCode status;
        if (!FillTableDescription(modifyScheme, *item.Table, ss->TableProfiles, status, error, true)) {
            return nullptr;
        }

        if (!NeedToBuildIndexes(importInfo, itemIdx) && !FillIndexDescription(indexedTable, *item.Table, status, error)) {
            return nullptr;
        }

        if (!FillDefaultValues(item, indexedTable, error)) {
            return nullptr;
        }
    }

    if (importInfo.UserSID) {
        record.SetOwner(*importInfo.UserSID);
    }
    FillOwner(record, item.Permissions);

    if (!FillACL(modifyScheme, item.Permissions, error)) {
        return nullptr;
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTablePropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx
) {
    TString unused;
    return CreateTablePropose(ss, txId, importInfo, itemIdx, unused);
}

template <typename TPath>
static auto GetDescription(TSchemeShard* ss, const TPath& path) {
    NKikimrSchemeOp::TDescribeOptions opts;
    opts.SetShowPrivateTable(true);
    return DescribePath(ss, TlsActivationContext->AsActorContext(), path, opts);
}

static NKikimrSchemeOp::TTableDescription GetTableDescription(TSchemeShard* ss, const TPathId& pathId) {
    auto desc = GetDescription(ss, pathId);
    auto record = desc->GetRecord();

    Y_ABORT_UNLESS(record.HasPathDescription());
    const auto& pathDesc = record.GetPathDescription();
    Y_ABORT_UNLESS(pathDesc.HasTable() || pathDesc.HasColumnTableDescription());

    if (pathDesc.HasColumnTableDescription()) {
        NKikimrSchemeOp::TTableDescription result;
        const auto& columnTable = pathDesc.GetColumnTableDescription();
        THashMap<TString, ui32> columnIds;
        for (const auto& column : columnTable.GetSchema().GetColumns()) {
            auto& dstColumn = *result.add_columns();
            dstColumn.set_name(column.GetName());
            dstColumn.set_type(column.GetType());
            dstColumn.set_typeid_(column.GetTypeId());
            dstColumn.set_id(column.GetId());
            columnIds[column.GetName()] = column.GetId();
        }

        result.MutableKeyColumnNames()->CopyFrom(columnTable.GetSchema().GetKeyColumnNames());
        for (const auto& keyColumnName : columnTable.GetSchema().GetKeyColumnNames()) {
            auto it = columnIds.find(keyColumnName);
            Y_ABORT_UNLESS(it != columnIds.end());
            result.AddKeyColumnIds(it->second);
        }

        result.MutablePartitionConfig()->MutablePartitioningPolicy()->SetMinPartitionsCount(columnTable.GetColumnShardCount());
        return result;
    }

    return record.GetPathDescription().GetTable();
}

static NKikimrSchemeOp::TTableDescription RebuildTableDescription(
    const NKikimrSchemeOp::TTableDescription& src,
    const Ydb::Table::CreateTableRequest& scheme
) {
    NKikimrSchemeOp::TTableDescription tableDesc;
    tableDesc.MutableKeyColumnNames()->CopyFrom(src.GetKeyColumnNames());
    tableDesc.MutableKeyColumnIds()->CopyFrom(src.GetKeyColumnIds());

    THashMap<TString, ui32> columnNameToIdx;
    for (ui32 i = 0; i < src.ColumnsSize(); ++i) {
        Y_ABORT_UNLESS(columnNameToIdx.emplace(src.GetColumns(i).GetName(), i).second);
    }

    for (const auto& column : scheme.columns()) {
        auto it = columnNameToIdx.find(column.name());
        Y_ABORT_UNLESS(it != columnNameToIdx.end());

        Y_ABORT_UNLESS(it->second < src.ColumnsSize());
        tableDesc.MutableColumns()->Add()->CopyFrom(src.GetColumns(it->second));
    }

    return tableDesc;
}

template <typename TSettings>
void FillRestoreEncryptionSettings(
    NKikimrSchemeOp::TRestoreTask& task,
    const TSettings& settings,
    const TImportInfo::TItem& item)
{
    if (settings.has_encryption_settings()) {
        auto& taskEncryptionSettings = *task.MutableEncryptionSettings();
        *taskEncryptionSettings.MutableSymmetricKey() = settings.encryption_settings().symmetric_key();
        if (item.ExportItemIV) {
            taskEncryptionSettings.SetIV(item.ExportItemIV->GetBinaryString());
        }
    }
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> RestoreTableDataPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx
) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);
    Y_ABORT_UNLESS(item.Table);

    auto propose = MakeModifySchemeTransaction(ss, txId, importInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpRestore);
    modifyScheme.SetInternal(true);

    const TPath dstPath = TPath::Init(item.DstPathId, ss);
    Y_ABORT_UNLESS(dstPath.IsResolved());

    modifyScheme.SetWorkingDir(dstPath.Parent().PathString());

    auto& task = *modifyScheme.MutableRestore();
    task.SetTableName(dstPath.LeafName());
    *task.MutableTableDescription() = RebuildTableDescription(GetTableDescription(ss, item.DstPathId), *item.Table);

    switch (importInfo.Kind) {
    case TImportInfo::EKind::S3:
        {
            auto settings = importInfo.GetS3Settings();
            FillRestoreEncryptionSettings(task, settings, item);
            task.SetNumberOfRetries(settings.number_of_retries());
            auto& restoreSettings = *task.MutableS3Settings();
            restoreSettings.SetEndpoint(settings.endpoint());
            restoreSettings.SetBucket(settings.bucket());
            restoreSettings.SetAccessKey(settings.access_key());
            restoreSettings.SetSecretKey(settings.secret_key());
            restoreSettings.SetObjectKeyPattern(importInfo.GetItemSrcPrefix(itemIdx));
            restoreSettings.SetUseVirtualAddressing(!settings.disable_virtual_addressing());

            switch (settings.scheme()) {
            case Ydb::Import::ImportFromS3Settings::HTTP:
                restoreSettings.SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
                break;
            case Ydb::Import::ImportFromS3Settings::HTTPS:
                restoreSettings.SetScheme(NKikimrSchemeOp::TS3Settings::HTTPS);
                break;
            default:
                Y_ABORT("Unknown scheme");
            }

            if (const auto region = settings.region()) {
                restoreSettings.SetRegion(region);
            }
        }
        break;

    case TImportInfo::EKind::FS:
        {
            auto settings = importInfo.GetFsSettings();
            FillRestoreEncryptionSettings(task, settings, item);
            task.SetNumberOfRetries(settings.number_of_retries());
            auto& restoreSettings = *task.MutableFSSettings();
            restoreSettings.SetBasePath(settings.base_path());
            restoreSettings.SetPath(importInfo.GetItemSrcPrefix(itemIdx));
        }
        break;
    }

    const auto* metadata = &item.Metadata;
    if (item.ParentIdx != Max<ui32>()) {
        Y_ABORT_UNLESS(item.ParentIdx < importInfo.Items.size());
        metadata = &importInfo.Items[item.ParentIdx].Metadata;
    }

    if (!metadata->HasVersion() || metadata->GetVersion() > 0) {
        task.SetValidateChecksums(!importInfo.GetSkipChecksumValidation());
    }

    return propose;
}

THolder<TEvSchemeShard::TEvCancelTx> CancelRestoreTableDataPropose(
    const TImportInfo& importInfo,
    TTxId restoreTxId
) {
    auto propose = MakeHolder<TEvSchemeShard::TEvCancelTx>();

    auto& record = propose->Record;
    record.SetTxId(importInfo.Id);
    record.SetTargetTxId(ui64(restoreTxId));

    return propose;
}

THolder<TEvIndexBuilder::TEvCreateRequest> BuildIndexPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    const TString& uid
) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);
    Y_ABORT_UNLESS(item.Table);

    NKikimrIndexBuilder::TIndexBuildSettings settings;

    const TPath dstPath = TPath::Init(item.DstPathId, ss);
    settings.set_source_path(dstPath.PathString());
    if (ss->MaxRestoreBuildIndexShardsInFlight) {
        settings.set_max_shards_in_flight(ss->MaxRestoreBuildIndexShardsInFlight);
    }

    Y_ABORT_UNLESS(item.NextIndexIdx < item.Table->indexes_size());
    settings.mutable_index()->CopyFrom(item.Table->indexes(item.NextIndexIdx));

    if (settings.mutable_index()->type_case() == Ydb::Table::TableIndex::TypeCase::TYPE_NOT_SET) {
        settings.mutable_index()->mutable_global_index();
    }

    const TPath domainPath = TPath::Init(importInfo.DomainPathId, ss);
    auto propose = MakeHolder<TEvIndexBuilder::TEvCreateRequest>(ui64(txId), domainPath.PathString(), std::move(settings));
    auto& request = propose->Record;
    (*request.MutableOperationParams()->mutable_labels())["uid"] = uid;
    request.SetInternal(true);

    return propose;
}

THolder<TEvIndexBuilder::TEvCancelRequest> CancelIndexBuildPropose(
    TSchemeShard* ss,
    const TImportInfo& importInfo,
    TTxId indexBuildId
) {
    const TPath domainPath = TPath::Init(importInfo.DomainPathId, ss);
    return MakeHolder<TEvIndexBuilder::TEvCancelRequest>(ui64(indexBuildId), domainPath.PathString(), ui64(indexBuildId));
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateChangefeedPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    const TImportInfo::TItem& item,
    TString& error
) {
    Y_ABORT_UNLESS(item.NextChangefeedIdx < item.Changefeeds.GetChangefeeds().size());

    const auto& importChangefeedTopic = item.Changefeeds.GetChangefeeds()[item.NextChangefeedIdx];
    const auto& changefeed = importChangefeedTopic.GetChangefeed();
    const auto& topic = importChangefeedTopic.GetTopic();

    auto propose = MakeModifySchemeTransaction(ss, txId, importInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateCdcStream);
    auto& cdcStream = *modifyScheme.MutableCreateCdcStream();

    const TPath dstPath = TPath::Init(item.DstPathId, ss);
    modifyScheme.SetWorkingDir(dstPath.Parent().PathString());
    cdcStream.SetTableName(dstPath.LeafName());

    auto& cdcStreamDescription = *cdcStream.MutableStreamDescription();
    Ydb::StatusIds::StatusCode status;
    if (!FillChangefeedDescription(cdcStreamDescription, changefeed, status, error)) {
        return nullptr;
    }

    if (topic.has_retention_period()) {
        cdcStream.SetRetentionPeriodSeconds(topic.retention_period().seconds());
    }

    auto tableDesc = GetTableDescription(ss, dstPath->PathId);
    Y_ABORT_UNLESS(!tableDesc.GetKeyColumnIds().empty());
    const auto& keyId = tableDesc.GetKeyColumnIds()[0];
    bool isPartitioningAvailable = false;

    // Explicit specification of the number of partitions when creating CDC
    // is possible only if the first component of the primary key
    // of the source table is Uint32 or Uint64
    for (const auto& column : tableDesc.GetColumns()) {
        if (column.GetId() == keyId) {
            isPartitioningAvailable = column.GetType() == "Uint32" || column.GetType() == "Uint64";
            break;
        }
    }

    if (topic.has_partitioning_settings()) {
        if (isPartitioningAvailable) {
            i64 minActivePartitions =
                topic.partitioning_settings().min_active_partitions();
            if (minActivePartitions < 0) {
                error = "minActivePartitions must be >= 0";
                return nullptr;
            } else if (minActivePartitions == 0) {
                minActivePartitions = 1;
            }
            cdcStream.SetTopicPartitions(minActivePartitions);
        }

        if (topic.partitioning_settings().has_auto_partitioning_settings()) {
            auto& partitioningSettings = topic.partitioning_settings().auto_partitioning_settings();
            cdcStream.SetTopicAutoPartitioning(partitioningSettings.strategy() != ::Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED);

            i64 maxActivePartitions =
                topic.partitioning_settings().max_active_partitions();
            if (maxActivePartitions < 0) {
                error = "maxActivePartitions must be >= 0";
                return nullptr;
            } else if (maxActivePartitions == 0) {
                maxActivePartitions = 50;
            }
            cdcStream.SetMaxPartitionCount(maxActivePartitions);
        }
    }
    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateConsumersPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    TImportInfo::TItem& item
) {
    Y_ABORT_UNLESS(item.NextChangefeedIdx < item.Changefeeds.GetChangefeeds().size());

    const auto& importChangefeedTopic = item.Changefeeds.GetChangefeeds()[item.NextChangefeedIdx];
    const auto& topic = importChangefeedTopic.GetTopic();

    auto propose = MakeModifySchemeTransaction(ss, txId, importInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
    auto& pqGroup = *modifyScheme.MutableAlterPersQueueGroup();

    const TPath dstPath = TPath::Init(item.DstPathId, ss);
    const TString changefeedPath = dstPath.PathString() + "/" + importChangefeedTopic.GetChangefeed().name();
    modifyScheme.SetWorkingDir(changefeedPath);
    modifyScheme.SetInternal(true);

    pqGroup.SetName("streamImpl");

    auto describeSchemeResult = GetDescription(ss, changefeedPath + "/streamImpl");

    const auto& response = describeSchemeResult->GetRecord().GetPathDescription();
    item.StreamImplPathId = {response.GetSelf().GetSchemeshardId(), response.GetSelf().GetPathId()};
    pqGroup.CopyFrom(response.GetPersQueueGroup());

    pqGroup.ClearTotalGroupCount();
    pqGroup.MutablePQTabletConfig()->ClearPartitionKeySchema();

    auto* tabletConfig = pqGroup.MutablePQTabletConfig();
    const auto& pqConfig = AppData()->PQConfig;

    for (const auto& consumer : topic.consumers()) {
        auto& addedConsumer = *tabletConfig->AddConsumers();
        auto consumerName = NPersQueue::ConvertNewConsumerName(consumer.name(), pqConfig);
        addedConsumer.SetName(consumerName);
        if (consumer.important()) {
            addedConsumer.SetImportant(true);
        }
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTopicPropose(
    TSchemeShard* ss,
    TTxId txId,
    const TImportInfo& importInfo,
    ui32 itemIdx,
    TString& error
) {
    Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
    const auto& item = importInfo.Items.at(itemIdx);
    Y_ABORT_UNLESS(item.Topic);

    auto propose = MakeModifySchemeTransaction(ss, txId, importInfo);
    auto& record = propose->Record;

    auto& modifyScheme = *record.AddTransaction();

    const TPath domainPath = TPath::Init(importInfo.DomainPathId, ss);
    std::pair<TString, TString> wdAndPath;
    if (!TrySplitPathByDb(item.DstPathName, domainPath.PathString(), wdAndPath, error)) {
        return nullptr;
    }

    modifyScheme.SetWorkingDir(wdAndPath.first);

    auto codes =
        NGRpcProxy::V1::FillProposeRequestImpl(wdAndPath.second, *item.Topic, modifyScheme, AppData(), error, wdAndPath.first);

    if (codes.YdbCode != Ydb::StatusIds::SUCCESS) {
        return nullptr;
    }

    return propose;
}

} // NSchemeShard
} // NKikimr
