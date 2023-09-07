#pragma once

#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/common.h>

namespace NYT::NColumnConverters {

////////////////////////////////////////////////////////////////////////////////

void FillColumnarNullBitmap(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRef bitmap);

void FillColumnarDictionary(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* primaryColumn,
    NTableClient::IUnversionedColumnarRowBatch::TColumn* dictionaryColumn,
    NTableClient::IUnversionedColumnarRowBatch::TDictionaryId dictionaryId,
    NTableClient::TLogicalTypePtr type,
    i64 startIndex,
    i64 valueCount,
    TRef ids);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnversionedStringSegmentType,
    ((DictionaryDense) (0))
    ((DirectDense)     (1))
);

struct TConverterTag
{};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
