#include "change_record_body_serializer.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NDataShard {

using namespace NTable;

void TChangeRecordBodySerializer::SerializeCells(TSerializedCells& out,
        TArrayRef<const TRawTypeValue> in, TArrayRef<const TTag> tags)
{
    Y_VERIFY_S(in.size() == tags.size(), "Count doesn't match"
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
        TArrayRef<const TUpdateOp> in)
{
    if (!in) {
        return;
    }

    TVector<TCell> cells(Reserve(in.size()));
    for (const auto& op : in) {
        Y_VERIFY_S(op.Op == ECellOp::Set, "Unexpected cell op: " << op.Op.Raw());

        out.AddTags(op.Tag);
        cells.emplace_back(op.AsCell());
    }

    out.SetData(TSerializedCellVec::Serialize(cells));
}

void TChangeRecordBodySerializer::SerializeCells(TSerializedCells& out,
        const TRowState& state, TArrayRef<const TTag> tags)
{
    Y_VERIFY_S(state.Size() == tags.size(), "Count doesn't match"
        << ": state# " << state.Size()
        << ", tags# " << tags.size());

    TVector<TCell> cells(Reserve(state.Size()));
    for (TPos pos = 0; pos < state.Size(); ++pos) {
        out.AddTags(tags.at(pos));
        cells.emplace_back(state.Get(pos));
    }

    out.SetData(TSerializedCellVec::Serialize(cells));
}

void TChangeRecordBodySerializer::Serialize(TDataChange& out, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags, TArrayRef<const TUpdateOp> updates)
{
    SerializeCells(*out.MutableKey(), key, keyTags);

    switch (rop) {
    case ERowOp::Upsert:
        SerializeCells(*out.MutableUpsert(), updates);
        break;
    case ERowOp::Erase:
        out.MutableErase();
        break;
    case ERowOp::Reset:
        SerializeCells(*out.MutableReset(), updates);
        break;
    default:
        Y_FAIL_S("Unsupported row op: " << static_cast<ui8>(rop));
    }
}

void TChangeRecordBodySerializer::Serialize(TDataChange& out, ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const TTag> keyTags,
        const TRowState* oldState, const TRowState* newState, TArrayRef<const TTag> valueTags)
{
    Serialize(out, rop, key, keyTags, {});

    if (oldState) {
        SerializeCells(*out.MutableOldImage(), *oldState, valueTags);
    }

    if (newState) {
        SerializeCells(*out.MutableNewImage(), *newState, valueTags);
    }
}

}
