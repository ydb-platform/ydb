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
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRef bitmap)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    auto& nullBitmap = column->NullBitmap.emplace();
    nullBitmap.Data = bitmap;
}


void FillColumnarDictionary(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* primaryColumn,
    NTableClient::IUnversionedColumnarRowBatch::TColumn* dictionaryColumn,
    NTableClient::IUnversionedColumnarRowBatch::TDictionaryId dictionaryId,
    NTableClient::TLogicalTypePtr type,
    i64 startIndex,
    i64 valueCount,
    TRef ids)
{
    primaryColumn->StartIndex = startIndex;
    primaryColumn->ValueCount = valueCount;

    dictionaryColumn->Type = type && type->GetMetatype() == ELogicalMetatype::Optional
        ? type->AsOptionalTypeRef().GetElement()
        : type;

    auto& primaryValues = primaryColumn->Values.emplace();
    primaryValues.BitWidth = 32;
    primaryValues.Data = ids;

    auto& dictionary = primaryColumn->Dictionary.emplace();
    dictionary.DictionaryId = dictionaryId;
    dictionary.ZeroMeansNull = true;
    dictionary.ValueColumn = dictionaryColumn;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
