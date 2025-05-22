#pragma once
#include "size_calcer.h"

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <library/cpp/cache/cache.h>
#include <util/generic/hash.h>

namespace NKikimr::NArrow {

class TThreadSimpleArraysCache {
private:
    class TCachedArrayData {
    private:
        YDB_READONLY_DEF(std::shared_ptr<arrow::Array>, Array);
        YDB_READONLY(ui64, Size, 0);

    public:
        TCachedArrayData() = default;
        TCachedArrayData(const std::shared_ptr<arrow::Array>& array)
            : Array(array)
            , Size(NArrow::GetArrayDataSize(array)) {
        }
    };

    struct TCachedSizeProvider {
        size_t operator()(const TCachedArrayData& data) {
            return data.GetSize();
        }
    };

    TLRUCache<TString, TCachedArrayData, TNoopDelete, TCachedSizeProvider> Arrays;
    static const ui64 MaxOneArrayMemorySize = 1 * 1024 * 1024;
    static const ui64 MaxSumMemorySize = 50 * 1024 * 1024;

    template <class TInitializeActor>
    std::shared_ptr<arrow::Array> InitializePosition(const TString& key, const ui32 recordsCountExt, const TInitializeActor actor) {
        TCachedArrayData currentValue;
        std::shared_ptr<arrow::Array> result;
        {
            auto it = Arrays.Find(key);
            if (it == Arrays.End() || it->GetArray()->length() < recordsCountExt) {
                result = actor(recordsCountExt);
                TCachedArrayData cache(result);
                if (cache.GetSize() < MaxOneArrayMemorySize) {
                    AFL_INFO(NKikimrServices::ARROW_HELPER)("event", "insert_to_cache")("key", key)("records", recordsCountExt)(
                        "size", cache.GetSize());
                    if (it != Arrays.End()) {
                        Arrays.Erase(it);
                    }
                    AFL_VERIFY(Arrays.Insert(key, result));
                } else {
                    AFL_INFO(NKikimrServices::ARROW_HELPER)("event", "too_big_to_add")("key", key)("records", recordsCountExt)(
                        "size", cache.GetSize());
                }
            } else {
                result = it->GetArray();
            }
        }
        AFL_VERIFY(result);
        AFL_VERIFY(recordsCountExt <= result->length())("result", result->length())("ext", recordsCountExt);
        AFL_INFO(NKikimrServices::ARROW_HELPER)("event", "slice_from_cache")("key", key)("records", recordsCountExt)("count", result->length());
        return result->Slice(0, recordsCountExt);
    }

    std::shared_ptr<arrow::Array> GetNullImpl(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount);
    std::shared_ptr<arrow::Array> GetConstImpl(
        const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<arrow::Scalar>& scalar, const ui32 recordsCount);

public:
    TThreadSimpleArraysCache()
        : Arrays(MaxSumMemorySize) {
    }

    static std::shared_ptr<arrow::Array> GetNull(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount);
    static std::shared_ptr<arrow::Array> GetConst(
        const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<arrow::Scalar>& scalar, const ui32 recordsCount);
    static std::shared_ptr<arrow::Array> Get(
        const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<arrow::Scalar>& scalar, const ui32 recordsCount);
};
}   // namespace NKikimr::NArrow
