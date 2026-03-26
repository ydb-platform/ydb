#pragma once
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/array_dict.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/record_batch.h>
#include <util/system/types.h>

namespace NKikimr::NArrow {

bool IsDictionableArray(const std::shared_ptr<arrow20::Array>& data);
std::shared_ptr<arrow20::DictionaryArray> ArrayToDictionary(const std::shared_ptr<arrow20::Array>& data);
std::shared_ptr<arrow20::RecordBatch> ArrayToDictionary(const std::shared_ptr<arrow20::RecordBatch>& data);
std::shared_ptr<arrow20::Array> DictionaryToArray(const std::shared_ptr<arrow20::DictionaryArray>& data);
std::shared_ptr<arrow20::Array> DictionaryToArray(const arrow20::DictionaryArray& data);
std::shared_ptr<arrow20::RecordBatch> DictionaryToArray(const std::shared_ptr<arrow20::RecordBatch>& data);

}
