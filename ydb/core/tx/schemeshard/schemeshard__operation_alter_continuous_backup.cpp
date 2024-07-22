#include "schemeshard__operation_alter_cdc_stream.h"
#include "schemeshard__operation_common.h"
#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/schemeshard/backup/constants.h>

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

    result.push_back(CreateAlterPQ(NextPartId(opId, result), outTx));
}

void DoCreateIncBackupTable(const TOperationId& opId, const TPath& dst, NKikimrSchemeOp::TTableDescription tableDesc, TVector<ISubOperation::TPtr>& result) {
    auto outTx = TransactionTemplate(dst.Parent().PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
    // outTx.SetFailOnExist(!acceptExisted);

    outTx.SetAllowAccessToPrivatePaths(true);

    auto& desc = *outTx.MutableCreateTable();
    desc.CopyFrom(tableDesc);
    desc.SetName(dst.LeafName());

    auto& replicationConfig = *desc.MutableReplicationConfig();
    replicationConfig.SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
    replicationConfig.SetConsistency(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_WEAK);

    // TODO: remove NotNull from all columns for correct deletion writing
    // TODO: cleanup all sequences

    auto* col = desc.AddColumns();
    col->SetName("__incrBackupImpl_deleted");
    col->SetType("Bool");

    result.push_back(CreateNewTable(NextPartId(opId, result), outTx));
}

TVector<ISubOperation::TPtr> CreateAlterContinuousBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpAlterContinuousBackup);

    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& cbOp = tx.GetAlterContinuousBackup();
    const auto& tableName = cbOp.GetTableName();

    const auto checksResult = NCdc::DoAlterStreamPathChecks(opId, workingDirPath, tableName, NBackup::CB_CDC_STREAM_NAME);
    if (std::holds_alternative<ISubOperation::TPtr>(checksResult)) {
        return {std::get<ISubOperation::TPtr>(checksResult)};
    }

    const auto [tablePath, streamPath] = std::get<NCdc::TStreamPaths>(checksResult);
    TTableInfo::TPtr table = context.SS->Tables.at(tablePath.Base()->PathId);

    const auto topicPath = streamPath.Child("streamImpl");
    TTopicInfo::TPtr topic = context.SS->Topics.at(topicPath.Base()->PathId);

    const auto backupTablePath = tablePath.Child("incBackupImpl");

    const NScheme::TTypeRegistry* typeRegistry = AppData(context.Ctx)->TypeRegistry;

    NKikimrSchemeOp::TTableDescription schema;
    context.SS->DescribeTable(table, typeRegistry, true, &schema);
    schema.MutablePartitionConfig()->CopyFrom(table->TableDescription.GetPartitionConfig());

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (!context.SS->CheckLocks(tablePath.Base()->PathId, tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
    }

    NKikimrSchemeOp::TAlterCdcStream alterCdcStreamOp;
    alterCdcStreamOp.SetTableName(tableName);
    alterCdcStreamOp.SetStreamName(NBackup::CB_CDC_STREAM_NAME);

    switch (cbOp.GetActionCase()) {
    case NKikimrSchemeOp::TAlterContinuousBackup::kStop:
    case NKikimrSchemeOp::TAlterContinuousBackup::kTakeIncrementalBackup:
        alterCdcStreamOp.MutableDisable();
        break;
    default:
        return {CreateReject(opId, NKikimrScheme::StatusInvalidParameter, TStringBuilder()
            << "Unknown action: " << static_cast<ui32>(cbOp.GetActionCase()))};
    }

    TVector<ISubOperation::TPtr> result;

    NCdc::DoAlterStream(result, alterCdcStreamOp, opId, workingDirPath, tablePath);

    if (cbOp.GetActionCase() == NKikimrSchemeOp::TAlterContinuousBackup::kTakeIncrementalBackup) {
        DoCreateIncBackupTable(opId, backupTablePath, schema, result);
        DoAlterPqPart(opId, backupTablePath, topicPath, topic, result);
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
