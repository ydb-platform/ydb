#include "schemeshard__operation_part.h"
#include "schemeshard__operation_common.h"
#include "schemeshard_impl.h"

#include "schemeshard__operation_create_cdc_stream.h"

#include <ydb/core/tx/schemeshard/backup/constants.h>

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateNewContinuousBackup(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_ABORT_UNLESS(tx.GetOperationType() == NKikimrSchemeOp::EOperationType::ESchemeOpCreateContinuousBackup);

    const auto acceptExisted = !tx.GetFailOnExist();
    const auto workingDirPath = TPath::Resolve(tx.GetWorkingDir(), context.SS);
    const auto& cbOp = tx.GetCreateContinuousBackup();
    const auto& tableName = cbOp.GetTableName();
    const auto tablePath = workingDirPath.Child(tableName);

    NKikimrSchemeOp::TCreateCdcStream createCdcStreamOp;
    createCdcStreamOp.SetTableName(tableName);
    auto& streamDescription = *createCdcStreamOp.MutableStreamDescription();
    streamDescription.SetName(NBackup::CB_CDC_STREAM_NAME);
    streamDescription.SetMode(NKikimrSchemeOp::ECdcStreamModeUpdate);
    streamDescription.SetFormat(NKikimrSchemeOp::ECdcStreamFormatProto);

    auto table = context.SS->Tables.at(tablePath.Base()->PathId);

    const auto streamPath = tablePath.Child(NBackup::CB_CDC_STREAM_NAME);

    TVector<TString> boundaries;
    const auto& partitions = table->GetPartitions();
    boundaries.reserve(partitions.size() - 1);

    for (ui32 i = 0; i < partitions.size(); ++i) {
        const auto& partition = partitions.at(i);
        if (i != partitions.size() - 1) {
            boundaries.push_back(partition.EndOfRange);
        }
    }

    TVector<ISubOperation::TPtr> result;

    NCdc::DoCreateStream(createCdcStreamOp, opId, workingDirPath, tablePath, acceptExisted, false, result);
    NCdc::DoCreatePqPart(opId, streamPath, NBackup::CB_CDC_STREAM_NAME, table, createCdcStreamOp, boundaries, acceptExisted, result);

    return result;
}

} // namespace NKikimr::NSchemeShard
