#pragma once

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/scheme_types/scheme_raw_type_value.h>
#include <ydb/core/tablet_flat/flat_database.h>

namespace NKikimr::NDataShard {

class TChangeRecordBodySerializer {
    using TDataChange = NKikimrChangeExchange::TDataChange;
    using TSerializedCells = TDataChange::TSerializedCells;

    static void SerializeCells(TSerializedCells& out,
        TArrayRef<const TRawTypeValue> in, TArrayRef<const NTable::TTag> tags);
    static void SerializeCells(TSerializedCells& out,
        TArrayRef<const NTable::TUpdateOp> in);
    static void SerializeCells(TSerializedCells& out,
        const NTable::TRowState& state, TArrayRef<const NTable::TTag> tags);

public:
    static void Serialize(TDataChange& out, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags, TArrayRef<const NTable::TUpdateOp> updates);
    static void Serialize(TDataChange& out, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags,
        const NTable::TRowState* oldState, const NTable::TRowState* newState, TArrayRef<const NTable::TTag> valueTags);

}; // TChangeRecordBodySerializer

}
