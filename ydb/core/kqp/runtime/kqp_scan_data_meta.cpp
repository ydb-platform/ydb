#include "kqp_compute.h"
#include "kqp_scan_data_meta.h"
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>

namespace NKikimr::NMiniKQL {

TScanDataMeta::TScanDataMeta(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta) {
    const auto& tableMeta = meta.GetTable();
    TableId = TTableId(tableMeta.GetTableId().GetOwnerId(), tableMeta.GetTableId().GetTableId(),
        tableMeta.GetSysViewInfo(), tableMeta.GetSchemaVersion());
    TablePath = meta.GetTable().GetTablePath();
}

TScanDataMeta::TScanDataMeta(const TTableId& tableId, const TString& tablePath)
    : TableId(tableId)
    , TablePath(tablePath)
{
}


TScanDataColumnsMeta::TScanDataColumnsMeta(const TSmallVec<TKqpComputeContextBase::TColumn>& columns,
    const TSmallVec<TKqpComputeContextBase::TColumn>& systemColumns,
    const TSmallVec<TKqpComputeContextBase::TColumn>& resultColumns)
    : Columns(columns)
    , SystemColumns(systemColumns)
    , ResultColumns(resultColumns)
    , TotalColumnsCount(resultColumns.size() + systemColumns.size())
{

}

TScanDataColumnsMeta::TScanDataColumnsMeta(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta) {
    Columns.reserve(meta.GetColumns().size());
    for (const auto& column : meta.GetColumns()) {
        TKqpComputeContextBase::TColumn c;
        c.Tag = column.GetId();
        auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetType(),
            column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
        c.Type = typeInfoMod.TypeInfo;
        c.TypeMod = typeInfoMod.TypeMod;

        if (!IsSystemColumn(c.Tag)) {
            Columns.emplace_back(std::move(c));
        } else {
            SystemColumns.emplace_back(std::move(c));
        }
    }

    if (meta.GetResultColumns().empty() && !meta.HasOlapProgram()) {
        // Currently we define ResultColumns just for Olap tables in TKqpQueryCompiler
        ResultColumns = Columns;
    } else {
        ResultColumns.reserve(meta.GetResultColumns().size());
        for (const auto& resColumn : meta.GetResultColumns()) {
            TKqpComputeContextBase::TColumn c;
            c.Tag = resColumn.GetId();
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(resColumn.GetType(),
                resColumn.HasTypeInfo() ? &resColumn.GetTypeInfo() : nullptr);
            c.Type = typeInfoMod.TypeInfo;
            c.TypeMod = typeInfoMod.TypeMod;

            if (!IsSystemColumn(c.Tag)) {
                ResultColumns.emplace_back(std::move(c));
            }
        }
    }

    TotalColumnsCount = ResultColumns.size() + SystemColumns.size();
}

}
