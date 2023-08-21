#pragma once

#include "unversioned_row.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Reorders values in original rows, putting key columns at the front.
//! Omitted key columns are filled in with null values.
//! All non-key columns are positioned after key ones, preserving order.
class TSchemalessRowReorderer
    : public TNonCopyable
{
public:
    TSchemalessRowReorderer(
        TNameTablePtr nameTable,
        TRowBufferPtr rowBuffer,
        bool captureValues,
        const TKeyColumns& keyColumns);

    TMutableUnversionedRow ReorderRow(TUnversionedRow row);

    //! Preserves only key columns, non-key column are ignored.
    TMutableUnversionedRow ReorderKey(TUnversionedRow row);

private:
    const TKeyColumns KeyColumns_;
    const TRowBufferPtr RowBuffer_;
    const bool CaptureValues_;
    const TNameTablePtr NameTable_;

    std::vector<int> IdMapping_;
    std::vector<TUnversionedValue> EmptyKey_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
