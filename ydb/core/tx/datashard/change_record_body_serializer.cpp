#include "change_record_body_serializer.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NDataShard {

using namespace NTable;

void TChangeRecordBodySerializer::SerializeCells(TDataChange& out, ERowOp rop,
        TArrayRef<const TUpdateOp> updates, TFunctionRef<bool(TTag)> includeColumn)
{
    TSerializedCells* values;
    switch (rop) {
        case ERowOp::Upsert:
            values = out.MutableUpsert();
            break;
        case ERowOp::Erase:
            out.MutableErase();
            return;
        case ERowOp::Reset:
            values = out.MutableReset();
            break;
        default:
            Y_ENSURE(false, "Unsupported row op: " << static_cast<ui8>(rop));
    }

    if (updates.empty()) {
        return;
    }

    TVector<TCell> cells(Reserve(updates.size()));
    for (const auto& update : updates) {
        if (!includeColumn(update.Tag)) {
            continue;
        }

        Y_ENSURE(update.Op == ECellOp::Set, "Unexpected cell op: " << update.Op.Raw());
        values->AddTags(update.Tag);
        cells.emplace_back(update.AsCell());
    }

    if (!cells.empty()) {
        values->SetData(TSerializedCellVec::Serialize(cells));
    }
}

void TChangeRecordBodySerializer::SerializeCells(TSerializedCells& out,
        TArrayRef<const TRawTypeValue> in, TArrayRef<const TTag> tags)
{
    Y_ENSURE(in.size() == tags.size(), "Count doesn't match"
        << ": in# " << in.size()
        << ", tags# " << tags.size());

    TVector<TCell> cells(Reserve(in.size()));
    for (size_t i = 0; i < in.size(); ++i) {
        out.AddTags(tags.at(i));
        cells.emplace_back(in.at(i).AsRef());
    }

    out.SetData(TSerializedCellVec::Serialize(cells));
}

void TChangeRecordBodySerializer::SerializeCells(TSerializedCells& out,
        const TRowState& state, TArrayRef<const TTag> tags)
{
    Y_ENSURE(state.Size() == tags.size(), "Count doesn't match"
        << ": state# " << state.Size()
        << ", tags# " << tags.size());

    TVector<TCell> cells(Reserve(state.Size()));
    for (TPos pos = 0; pos < state.Size(); ++pos) {
        out.AddTags(tags.at(pos));
        cells.emplace_back(state.Get(pos));
    }

    out.SetData(TSerializedCellVec::Serialize(cells));
}

void TChangeRecordBodySerializer::SerializeUpdates(TDataChange& out, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags, TArrayRef<const TUpdateOp> updates)
{
    SerializeCells(*out.MutableKey(), key, keyTags);
    SerializeCells(out, rop, updates, [](TTag) { return true; });
}

void TChangeRecordBodySerializer::SerializeUpdates(TDataChange& out, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags,
        TArrayRef<const TUpdateOp> updates, TFunctionRef<bool(TTag)> includeColumn)
{
    SerializeCells(*out.MutableKey(), key, keyTags);
    SerializeCells(out, rop, updates, includeColumn);
}

void TChangeRecordBodySerializer::SerializeImages(TDataChange& out, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags,
        const TRowState* oldState, const TRowState* newState, TArrayRef<const TTag> valueTags)
{
    SerializeUpdates(out, rop, key, keyTags, {});

    if (oldState) {
        SerializeCells(*out.MutableOldImage(), *oldState, valueTags);
    }

    if (newState) {
        SerializeCells(*out.MutableNewImage(), *newState, valueTags);
    }
}

}
