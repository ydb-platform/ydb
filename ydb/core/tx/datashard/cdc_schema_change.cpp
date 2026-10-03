#include "cdc_schema_change.h"

#include <ydb/library/aclib/user_context.h>

namespace NKikimr::NDataShard {

void PersistCdcSchemaChange(TDataShard& dataShard, NTabletFlatExecutor::TTransactionContext& txc,
    TOperation::TPtr op, const TPathId& tableId, const TUserTable& table)
{
    if (table.GetSchemaChangesCdcStreams().empty()) {
        return;
    }

    NIceDb::TNiceDb db(txc.DB);
    for (const auto& streamPathId : table.GetSchemaChangesCdcStreams()) {
        auto recordPtr = TChangeRecordBuilder(TChangeRecord::EKind::CdcSchemaChange)
            .WithOrder(dataShard.AllocateChangeRecordOrder(db))
            .WithGroup(0)
            .WithStep(op->GetStep())
            .WithTxId(op->GetTxId())
            .WithPathId(streamPathId)
            .WithTableId(tableId)
            .WithSchemaVersion(table.GetTableSchemaVersion())
            .WithUserCtx(NACLib::TUserContextBuilder().WithUserSID(BUILTIN_ACL_CDC_WITHOUT_USER_SID).Build())
            .Build();

        const auto& record = *recordPtr;
        dataShard.PersistChangeRecord(db, record);
        op->ChangeRecords().push_back(IDataShardChangeCollector::TChange{
            .Order = record.GetOrder(),
            .Group = record.GetGroup(),
            .Step = record.GetStep(),
            .TxId = record.GetTxId(),
            .PathId = record.GetPathId(),
            .BodySize = record.GetBody().size(),
            .TableId = record.GetTableId(),
            .SchemaVersion = record.GetSchemaVersion(),
        });
    }
}

}
