#include "helpers.h"

#include <yt/yt/client/table_client/columnar.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/bitmap.h>

namespace NYT::NColumnConverters {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void FillColumnarNullBitmap(
    IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRef bitmap)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    column->NullBitmap = IUnversionedColumnarRowBatch::TBitmap{};
    auto& nullBitmap = *column->NullBitmap;
    nullBitmap.Data = bitmap;
}

void FillColumnarDictionary(
    IUnversionedColumnarRowBatch::TColumn* primaryColumn,
    IUnversionedColumnarRowBatch::TColumn* dictionaryColumn,
    IUnversionedColumnarRowBatch::TDictionaryId dictionaryId,
    TLogicalTypePtr type,
    i64 startIndex,
    i64 valueCount,
    TRef ids)
{
    primaryColumn->StartIndex = startIndex;
    primaryColumn->ValueCount = valueCount;

    dictionaryColumn->Type = type && type->GetMetatype() == ELogicalMetatype::Optional
        ? type->AsOptionalTypeRef().GetElement()
        : type;

    primaryColumn->Values = IUnversionedColumnarRowBatch::TValueBuffer{};
    auto& primaryValues = *primaryColumn->Values;
    primaryValues.BitWidth = 32;
    primaryValues.Data = ids;

    primaryColumn->Dictionary = IUnversionedColumnarRowBatch::TDictionaryEncoding{};
    auto& dictionary = *primaryColumn->Dictionary;
    dictionary.DictionaryId = dictionaryId;
    dictionary.ZeroMeansNull = true;
    dictionary.ValueColumn = dictionaryColumn;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
