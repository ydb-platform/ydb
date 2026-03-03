#include "schemeshard__backup_collection_common.h"
#include "schemeshard__operation_alter_cdc_stream.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard__operation_rotate_cdc_stream.h"
#include "schemeshard_impl.h"
#include "schemeshard_utils.h"  // for TransactionTemplate

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard {

void DoAlterPqPart(const TOperationId& opId, const TPath& tablePath, const TPath& topicPath, TTopicInfo::TPtr topic, TVector<ISubOperation::TPtr>& result)
{
    auto outTx = TransactionTemplate(topicPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
    // outTx.SetFailOnExist(!acceptExisted);

    outTx.SetAllowAccessToPrivatePaths(true);

    auto& desc = *outTx.MutableAlterPersQueueGroup();
    desc.SetPathId(topicPath.Base()->PathId.LocalPathId);

    NKikimrPQ::TPQTabletConfig tabletConfig;
    if (!topic->TabletConfig.empty()) {
        bool parseOk = ParseFromStringNoSizeLimit(tabletConfig, topic->TabletConfig);
        Y_ABORT_UNLESS(parseOk, "Previously serialized pq tablet config cannot be parsed");
    }

    auto& pqConfig = *desc.MutablePQTabletConfig();
    pqConfig.CopyFrom(tabletConfig);
    pqConfig.ClearPartitionKeySchema();
    auto& ib = *pqConfig.MutableOffloadConfig()->MutableIncrementalBackup();
    ib.SetDstPath(tablePath.PathString());
    ib.SetTxId((ui64)opId.GetTxId());

    result.push_back(CreateAlterPQ(NextPartId(opId, result), outTx));
}

void DoCreateIncrBackupTable(const TOperationId& opId, const TPath& dst, NKikimrSchemeOp::TTableDescription tableDesc, TVector<ISubOperation::TPtr>& result) {
    auto outTx = TransactionTemplate(dst.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
    // outTx.SetFailOnExist(!acceptExisted);

    outTx.SetAllowAccessToPrivatePaths(true);

    auto& desc = *outTx.MutableCreateTable();
    desc.CopyFrom(tableDesc);
    desc.SetName(dst.LeafName());
    desc.SetSystemColumnNamesAllowed(true);

    auto& attrsDesc = *outTx.MutableAlterUserAttributes();
    attrsDesc.SetPathName(dst.LeafName());
    auto& attr = *attrsDesc.AddUserAttributes();
    attr.SetKey(TString(ATTR_INCREMENTAL_BACKUP));
    attr.SetValue("{}");

    auto& replicationConfig = *desc.MutableReplicationConfig();
    replicationConfig.SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
    replicationConfig.SetConsistencyLevel(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_LEVEL_ROW);

    // Set incremental backup config so DataShard can distinguish between async replica and incremental backup
    auto& incrementalBackupConfig = *desc.MutableIncrementalBackupConfig();
    incrementalBackupConfig.SetMode(NKikimrSchemeOp::TTableIncrementalBackupConfig::RESTORE_MODE_INCREMENTAL_BACKUP);
    incrementalBackupConfig.SetConsistency(NKikimrSchemeOp::TTableIncrementalBackupConfig::CONSISTENCY_WEAK);
    
    for (auto& column : *desc.MutableColumns()) {
        column.SetNotNull(false);
    }
    
    auto* changeMetadataCol = desc.AddColumns();
    changeMetadataCol->SetName("__ydb_incrBackupImpl_changeMetadata");
    changeMetadataCol->SetType("String");
    changeMetadataCol->SetNotNull(false);
    // TODO: cleanup all sequences

    result.push_back(CreateNewTable(NextPartId(opId, result), outTx));
}

bool CreateAlterContinuousBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context, TVector<ISubOperation::TPtr>& result, TPathId& outStream) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpAlterContinuousBackup);

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& cbOp = tx.GetAlterContinuousBackup();
    const auto& tableName = cbOp.GetTableName();
    const auto tablePath = workingDirPath.Child(tableName, TPath::TSplitChildTag{});

    TMaybeFail<TString> lastStreamName;
    static_assert(
        std::is_same_v<
            TMap<TString, TPathId>,
            std::decay_t<decltype(tablePath.Base()->GetChildren())>> == true,
        "Assume path children list is lexicographically sorted");

    for (const auto& [childName, childPathId] : tablePath.Base()->GetChildren()) {
        if (childName.EndsWith("_continuousBackupImpl")) {
            TPath childPath = tablePath.Child(childName);
            if (!childPath.IsDeleted() && childPath.IsCdcStream()) {
                if (context.SS->CdcStreams.contains(childPathId)) {
                    const auto& streamInfo = context.SS->CdcStreams.at(childPathId);
                    if (streamInfo->Format == NKikimrSchemeOp::ECdcStreamFormatProto) {
                        lastStreamName = childName;
                    }
                }
            }
        }
    }

    if (lastStreamName.Empty()) {
        result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter,
            TStringBuilder() << "Last continuous backup stream is not found")};
        return false;
    }

    const auto checksResult = NCdc::DoAlterStreamPathChecks(opId, workingDirPath, tableName, *lastStreamName);
    if (std::holds_alternative<ISubOperation::TPtr>(checksResult)) {
        result = {std::get<ISubOperation::TPtr>(checksResult)};
        return false;
    }

    const auto [_, streamPath] = std::get<NCdc::TStreamPaths>(checksResult);
    TTableInfo::TPtr table = context.SS->Tables.at(tablePath.Base()->PathId);

    const auto topicPath = streamPath.Child("streamImpl");
    TTopicInfo::TPtr topic = context.SS->Topics.at(topicPath.Base()->PathId);

    const auto backupTablePath = workingDirPath.Child(cbOp.GetTakeIncrementalBackup().GetDstPath(), TPath::TSplitChildTag{});

    const NScheme::TTypeRegistry* typeRegistry = AppData(context.Ctx)->TypeRegistry;

    NKikimrSchemeOp::TTableDescription schema;
    context.SS->DescribeTable(*table, typeRegistry, true, &schema);

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        result = {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
        return false;
    }

    if (!context.SS->CheckLocks(tablePath.Base()->PathId, tx, errStr)) {
        result = {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
        return false;
    }

    switch (cbOp.GetActionCase()) {
    case NKikimrSchemeOp::TAlterContinuousBackup::kStop:
    case NKikimrSchemeOp::TAlterContinuousBackup::kTakeIncrementalBackup:
        break;
    default:
        result = {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, TStringBuilder()
            << "Unknown action: " << static_cast<ui32>(cbOp.GetActionCase()))};

        return false;
    }

    if (cbOp.GetActionCase() == NKikimrSchemeOp::TAlterContinuousBackup::kTakeIncrementalBackup) {
        TString newStreamName;
        if (cbOp.GetTakeIncrementalBackup().HasDstStreamPath()) {
            newStreamName = cbOp.GetTakeIncrementalBackup().GetDstStreamPath();
        } else {
            newStreamName = NBackup::ToX509String(TlsActivationContext->AsActorContext().Now()) + "_continuousBackupImpl";
        }

        const auto cdcChecksResult = NCdc::DoNewStreamPathChecks(context, opId, workingDirPath, tableName, newStreamName, false);
        if (std::holds_alternative<ISubOperation::TPtr>(cdcChecksResult)) {
            result = {std::get<ISubOperation::TPtr>(cdcChecksResult)};
            return false;
        }

        const auto [_, newStreamPath] = std::get<NCdc::TStreamPaths>(cdcChecksResult);

        NKikimrSchemeOp::TRotateCdcStream rotateCdcStreamOp;
        rotateCdcStreamOp.SetTableName(tableName);
        rotateCdcStreamOp.SetOldStreamName(*lastStreamName);

        NKikimrSchemeOp::TCreateCdcStream createCdcStreamOp;
        createCdcStreamOp.SetTableName(tableName);
        auto& streamDescription = *createCdcStreamOp.MutableStreamDescription();
        streamDescription.SetName(newStreamName);
        streamDescription.SetMode(NKikimrSchemeOp::ECdcStreamModeUpdate);
        streamDescription.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);

        rotateCdcStreamOp.MutableNewStream()->CopyFrom(createCdcStreamOp);

        NCdc::DoRotateStream(result, rotateCdcStreamOp, opId, workingDirPath, tablePath);
        DoCreateIncrBackupTable(opId, backupTablePath, schema, result);
        DoAlterPqPart(opId, backupTablePath, topicPath, topic, result);

        TVector<TString> boundaries;
        const auto& partitions = table->GetPartitions();
        boundaries.reserve(partitions.size() - 1);

        for (ui32 i = 0; i < partitions.size(); ++i) {
            const auto& partition = partitions.at(i);
            if (i != partitions.size() - 1) {
                boundaries.push_back(partition.EndOfRange);
            }
        }

        NCdc::DoCreatePqPart(result, createCdcStreamOp, opId, newStreamPath, newStreamName, table, boundaries, false);
    } else if (cbOp.GetActionCase() == NKikimrSchemeOp::TAlterContinuousBackup::kStop) {
        NKikimrSchemeOp::TAlterCdcStream alterCdcStreamOp;
        alterCdcStreamOp.SetTableName(tableName);
        alterCdcStreamOp.SetStreamName(*lastStreamName);
        alterCdcStreamOp.MutableDisable();
        NCdc::DoAlterStream(result, alterCdcStreamOp, opId, workingDirPath, tablePath);
    }

    outStream = streamPath->PathId;
    return true;
}

bool CreateAlterContinuousBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context, TVector<ISubOperation::TPtr>& result) {
    TPathId unused;
    return CreateAlterContinuousBackup(opId, tx, context, result, unused);
}

TVector<ISubOperation::TPtr> CreateAlterContinuousBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    TVector<ISubOperation::TPtr> result;

    CreateAlterContinuousBackup(opId, tx, context, result);

    return result;
}

} // namespace NKikimr::NSchemeShard
