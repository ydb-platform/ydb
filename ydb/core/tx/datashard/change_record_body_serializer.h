#pragma once

#include <ydb/core/protos/change_exchange.pb.h>
#include <ydb/core/scheme_types/scheme_raw_type_value.h>
#include <ydb/core/tablet_flat/flat_database.h>

#include <util/generic/function_ref.h>

namespace NKikimr::NDataShard {

class TChangeRecordBodySerializer {
    using TDataChange = NKikimrChangeExchange::TDataChange;
    using TSerializedCells = TDataChange::TSerializedCells;

    static void SerializeCells(TSerializedCells& out,
        TArrayRef<const TRawTypeValue> in, TArrayRef<const NTable::TTag> tags);
    static void SerializeCells(TDataChange& out, NTable::ERowOp rop,
        TArrayRef<const NTable::TUpdateOp> updates, TFunctionRef<bool(NTable::TTag)> includeColumn);
    static void SerializeCells(TSerializedCells& out,
        const NTable::TRowState& state, TArrayRef<const NTable::TTag> tags);

public:
    // Serializes raw updates, selecting columns with includeColumn without copying the update array.
    // The overload without a predicate includes all updates.
    static void SerializeUpdates(TDataChange& out, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags, TArrayRef<const NTable::TUpdateOp> updates);
    static void SerializeUpdates(TDataChange& out, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags,
        TArrayRef<const NTable::TUpdateOp> updates, TFunctionRef<bool(NTable::TTag)> includeColumn);

    // Serializes prepared images whose cells correspond to valueTags in order.
    // Callers must apply column selection before reading or building the images.
    static void SerializeImages(TDataChange& out, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TTag> keyTags,
        const NTable::TRowState* oldState, const NTable::TRowState* newState, TArrayRef<const NTable::TTag> valueTags);

}; // TChangeRecordBodySerializer

}
