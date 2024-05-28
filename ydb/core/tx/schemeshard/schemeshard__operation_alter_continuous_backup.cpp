#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include "schemeshard__operation_alter_cdc_stream.h"

#include <ydb/core/tx/schemeshard/backup/constants.h>

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard {

void DoAlterPqPart(const TOperationId& opId, const TPath& topicPath, TTopicInfo::TPtr topic, TVector<ISubOperation::TPtr>& result)
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
    pqConfig.MutableOffloadConfig();

    result.push_back(CreateAlterPQ(NextPartId(opId, result), outTx));
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
    const auto topicPath = streamPath.Child("streamImpl");
    TTopicInfo::TPtr topic = context.SS->Topics.at(topicPath.Base()->PathId);

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

    NCdc::DoAlterStream(alterCdcStreamOp, opId, workingDirPath, tablePath, result);

    if (cbOp.GetActionCase() == NKikimrSchemeOp::TAlterContinuousBackup::kTakeIncrementalBackup) {
        DoAlterPqPart(opId, topicPath, topic, result);
    }

    return result;
}

} // namespace NKikimr::NSchemeShard
