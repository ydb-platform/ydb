#pragma once

#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>

#include <util/generic/string.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns::NTesting {

TString PrintBinaryJsons(const std::shared_ptr<arrow::ChunkedArray>& array);

std::shared_ptr<TTrivialArray> CreateTrivialArrayAccessor(TStringBuf data);

TDictStats BuildStats(const std::initializer_list<std::pair<TStringBuf, EValueType>>& columns);

std::shared_ptr<TSubColumnsArray> BuildArrayWithStoredPaths(const std::initializer_list<std::pair<TStringBuf, TStringBuf>>& columns,
    TStringBuf otherName, TStringBuf otherValue);

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns::NTesting
