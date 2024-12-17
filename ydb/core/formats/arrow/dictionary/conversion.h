#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_dict.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/system/types.h>

namespace NKikimr::NArrow {

bool IsDictionableArray(const std::shared_ptr<arrow::Array>& data);
std::shared_ptr<arrow::DictionaryArray> ArrayToDictionary(const std::shared_ptr<arrow::Array>& data);
std::shared_ptr<arrow::RecordBatch> ArrayToDictionary(const std::shared_ptr<arrow::RecordBatch>& data);
std::shared_ptr<arrow::Array> DictionaryToArray(const std::shared_ptr<arrow::DictionaryArray>& data);
std::shared_ptr<arrow::Array> DictionaryToArray(const arrow::DictionaryArray& data);
std::shared_ptr<arrow::RecordBatch> DictionaryToArray(const std::shared_ptr<arrow::RecordBatch>& data);

}
