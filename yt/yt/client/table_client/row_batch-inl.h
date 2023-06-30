#ifndef ROW_BATCH_INL_H_
#error "Direct inclusion of this file is not allowed, include row_batch.h"
// For the sake of sane code completion.
#include "row_batch.h"
#endif
#undef ROW_BATCH_INL_H_

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TRange<T> IUnversionedColumnarRowBatch::TColumn::GetTypedValues() const
{
    YT_VERIFY(Values);
    YT_VERIFY(Values->BitWidth == sizeof(T) * 8);
    return TRange<T>(
        reinterpret_cast<const T*>(Values->Data.Begin()),
        reinterpret_cast<const T*>(Values->Data.End()));
}


template <class T>
TRange<T> IUnversionedColumnarRowBatch::TColumn::GetRelevantTypedValues() const
{
    YT_VERIFY(Values);
    YT_VERIFY(Values->BitWidth == sizeof(T) * 8);
    YT_VERIFY(!Rle);
    return TRange<T>(
        reinterpret_cast<const T*>(Values->Data.Begin()) + StartIndex,
        reinterpret_cast<const T*>(Values->Data.Begin()) + StartIndex + ValueCount);
}

inline TRef IUnversionedColumnarRowBatch::TColumn::GetBitmapValues() const
{
    YT_VERIFY(Values);
    YT_VERIFY(Values->BitWidth == 1);
    YT_VERIFY(Values->BaseValue == 0);
    YT_VERIFY(!Values->ZigZagEncoded);
    YT_VERIFY(!Rle);
    YT_VERIFY(!Dictionary);
    return Values->Data;
}

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
typename TRowBatchTrait<TRow>::IRowBatchPtr CreateBatchFromRows(
    TSharedRange<TRow> rows)
{
    if constexpr (std::is_same_v<TRow, TVersionedRow>) {
        return CreateBatchFromVersionedRows(rows);
    } else {
        return CreateBatchFromUnversionedRows(rows);
    }
}

template <class TRow>
typename TRowBatchTrait<TRow>::IRowBatchPtr CreateEmptyRowBatch()
{
    if constexpr (std::is_same_v<TRow, TVersionedRow>) {
        return CreateEmptyVersionedRowBatch();
    } else {
        return CreateEmptyUnversionedRowBatch();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
