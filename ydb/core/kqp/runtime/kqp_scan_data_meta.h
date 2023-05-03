#pragma once
#include "kqp_compute.h"
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <ydb/core/tablet_flat/flat_table_column.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimrTxDataShard {
    class TKqpTransaction_TScanTaskMeta;
}

namespace NKikimr::NMiniKQL {

class TScanDataMeta {
public:
    TScanDataMeta(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta);
    TScanDataMeta(const TTableId& tableId, const TString& tablePath);
    TTableId TableId;
    TString TablePath;
};

class TScanDataColumnsMeta {
protected:
    TSmallVec<TKqpComputeContextBase::TColumn> Columns;
    TSmallVec<TKqpComputeContextBase::TColumn> SystemColumns;
    TSmallVec<TKqpComputeContextBase::TColumn> ResultColumns;
    ui32 TotalColumnsCount;
public:
    TScanDataColumnsMeta(const TSmallVec<TKqpComputeContextBase::TColumn>& columns, const TSmallVec<TKqpComputeContextBase::TColumn>& systemColumns,
        const TSmallVec<TKqpComputeContextBase::TColumn>& resultColumns);

    const TSmallVec<TKqpComputeContextBase::TColumn>& GetColumns() const {
        return Columns;
    }

    TScanDataColumnsMeta(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta);
};

class TScanDataMetaFull: public TScanDataColumnsMeta, public TScanDataMeta {
public:
    TScanDataMetaFull(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta)
        : TScanDataColumnsMeta(meta)
        , TScanDataMeta(meta)
    {

    }
};

}
