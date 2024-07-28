#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include "schemeshard__operation_create_cdc_stream.h"

namespace {

const char* IB_RESTORE_CDC_STREAM_NAME = "__ib_restore_stream";

}

#define LOG_D(stream) LOG_DEBUG_S (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_I(stream) LOG_INFO_S  (context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(context.Ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << context.SS->TabletID() << "] " << stream)

namespace NKikimr::NSchemeShard {

void DoCreateLock(const TOperationId opId, const TPath& workingDirPath, const TPath& tablePath, bool allowIndexImplLock,
    TVector<ISubOperation::TPtr>& result)
{
    auto outTx = TransactionTemplate(workingDirPath.PathString(),
        NKikimrSchemeOp::EOperationType::ESchemeOpCreateLock);
    outTx.SetFailOnExist(false);
    outTx.SetInternal(true);
    auto cfg = outTx.MutableLockConfig();
    cfg->SetName(tablePath.LeafName());

    result.push_back(CreateLock(NextPartId(opId, result), outTx));
}

void DoCreatePqPart(
    const TOperationId& opId,
    const TPath& streamPath,
    const TString& streamName,
    const TIntrusivePtr<TTableInfo> table,
    const TPathId dstPathId,
    const NKikimrSchemeOp::TCreateCdcStream& op,
    const TVector<TString>& boundaries,
    const bool acceptExisted,
    TVector<ISubOperation::TPtr>& result)
{
    auto outTx = TransactionTemplate(streamPath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpCreatePersQueueGroup);
    outTx.SetFailOnExist(!acceptExisted);

    auto& desc = *outTx.MutableCreatePersQueueGroup();
    desc.SetName("streamImpl");
    desc.SetTotalGroupCount(op.HasTopicPartitions() ? op.GetTopicPartitions() : table->GetPartitions().size());
    desc.SetPartitionPerTablet(2);

    auto& pqConfig = *desc.MutablePQTabletConfig();
    pqConfig.SetTopicName(streamName);
    pqConfig.SetTopicPath(streamPath.Child("streamImpl").PathString());
    pqConfig.SetMeteringMode(NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS);

    auto& partitionConfig = *pqConfig.MutablePartitionConfig();
    partitionConfig.SetLifetimeSeconds(op.GetRetentionPeriodSeconds());
    partitionConfig.SetWriteSpeedInBytesPerSecond(1_MB); // TODO: configurable write speed
    partitionConfig.SetBurstSize(1_MB); // TODO: configurable burst
    partitionConfig.SetMaxCountInPartition(Max<i32>());

    for (const auto& tag : table->KeyColumnIds) {
        Y_ABORT_UNLESS(table->Columns.contains(tag));
        const auto& column = table->Columns.at(tag);

        auto& keyComponent = *pqConfig.AddPartitionKeySchema();
        keyComponent.SetName(column.Name);
        auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.PType, column.PTypeMod);
        keyComponent.SetTypeId(columnType.TypeId);
        if (columnType.TypeInfo) {
            *keyComponent.MutableTypeInfo() = *columnType.TypeInfo;
        }
    }

    for (const auto& serialized : boundaries) {
        TSerializedCellVec endKey(serialized);
        Y_ABORT_UNLESS(endKey.GetCells().size() <= table->KeyColumnIds.size());

        TString errStr;
        auto& boundary = *desc.AddPartitionBoundaries();
        for (ui32 ki = 0; ki < endKey.GetCells().size(); ++ki) {
            const auto& cell = endKey.GetCells()[ki];
            const auto tag = table->KeyColumnIds.at(ki);
            Y_ABORT_UNLESS(table->Columns.contains(tag));
            const auto typeId = table->Columns.at(tag).PType;
            const bool ok = NMiniKQL::CellToValue(typeId, cell, *boundary.AddTuple(), errStr);
            Y_ABORT_UNLESS(ok, "Failed to build key tuple at position %" PRIu32 " error: %s", ki, errStr.data());
        }
    }

    auto& ir = *pqConfig.MutableOffloadConfig()->MutableIncrementalRestore();
    auto* pathId = ir.MutableDstPathId();
    PathIdFromPathId(dstPathId, pathId);

    result.push_back(CreateNewPQ(NextPartId(opId, result), outTx));
}

void DoCreateAlterTable(
    const TOperationId& opId,
    const TPath& dstTablePath,
    TVector<ISubOperation::TPtr>& result)
{
    auto outTx = TransactionTemplate(dstTablePath.PathString(), NKikimrSchemeOp::EOperationType::ESchemeOpAlterTable);
    auto& desc = *outTx.MutableAlterTable();

    PathIdFromPathId(dstTablePath.Base()->PathId, desc.MutablePathId());

    auto& replicationConfig = *desc.MutableReplicationConfig();
    replicationConfig.SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_RESTORE_INCREMENTAL_BACKUP);
    replicationConfig.SetConsistency(NKikimrSchemeOp::TTableReplicationConfig::CONSISTENCY_WEAK);

    result.push_back(CreateAlterTable(NextPartId(opId, result), outTx));
}


TVector<ISubOperation::TPtr> CreateRestoreIncrementalBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpRestoreIncrementalBackup);

    LOG_N("CreateRestoreIncrementalBackup"
        << ": opId# " << opId
        << ", tx# " << tx.ShortDebugString());

    const auto acceptExisted = !tx.GetFailOnExist();
    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& restoreOp = tx.GetRestoreIncrementalBackup();
    const auto& srcTableName = restoreOp.GetSrcTableName();
    const auto& dstTableName = restoreOp.GetDstTableName();
    const auto dstTablePath = workingDirPath.Child(dstTableName);

    const auto checksResult = NCdc::DoNewStreamPathChecks(opId, workingDirPath, srcTableName, IB_RESTORE_CDC_STREAM_NAME, acceptExisted, true);
    if (std::holds_alternative<ISubOperation::TPtr>(checksResult)) {
        return {std::get<ISubOperation::TPtr>(checksResult)};
    }

    const auto [srcTablePath, streamPath] = std::get<NCdc::TStreamPaths>(checksResult);

    Y_ABORT_UNLESS(context.SS->Tables.contains(srcTablePath.Base()->PathId));
    auto srcTable = context.SS->Tables.at(srcTablePath.Base()->PathId);

    TString errStr;
    if (!context.SS->CheckApplyIf(tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusPreconditionFailed, errStr)};
    }

    if (!context.SS->CheckLocks(srcTablePath.Base()->PathId, tx, errStr)) {
        return {CreateReject(opId, NKikimrScheme::StatusMultipleModifications, errStr)};
    }

    // check dst locks
    // lock dst
    // start replication read on dst

    TVector<TString> boundaries;
    const auto& partitions = srcTable->GetPartitions();
    boundaries.reserve(partitions.size() - 1);

    for (ui32 i = 0; i < partitions.size(); ++i) {
        const auto& partition = partitions.at(i);
        if (i != partitions.size() - 1) {
            boundaries.push_back(partition.EndOfRange);
        }
    }

    NKikimrSchemeOp::TCreateCdcStream createCdcStreamOp;
    createCdcStreamOp.SetTableName(srcTableName);
    auto& streamDescription = *createCdcStreamOp.MutableStreamDescription();
    streamDescription.SetName(IB_RESTORE_CDC_STREAM_NAME);
    streamDescription.SetMode(NKikimrSchemeOp::ECdcStreamModeRestoreIncrBackup);
    streamDescription.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);
    streamDescription.SetState(NKikimrSchemeOp::ECdcStreamStateScan);

    TVector<ISubOperation::TPtr> result;

    DoCreateLock(opId, workingDirPath, srcTablePath, false, result);

    DoCreateAlterTable(opId, dstTablePath, result);

    NCdc::DoCreateStream(
        result,
        createCdcStreamOp,
        opId,
        workingDirPath,
        srcTablePath,
        acceptExisted,
        true);
    DoCreatePqPart(
        opId,
        streamPath,
        IB_RESTORE_CDC_STREAM_NAME,
        srcTable,
        dstTablePath.Base()->PathId,
        createCdcStreamOp,
        boundaries,
        acceptExisted,
        result);

    return result;
}

} // namespace NKikimr::NSchemeShard
