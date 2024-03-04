#pragma once
#include "size_calcer.h"

#include <util/generic/hash.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>

namespace NKikimr::NArrow {

class TThreadSimpleArraysCache {
private:
    THashMap<TString, std::shared_ptr<arrow::Array>> Arrays;
    const ui64 MaxOneArrayMemorySize = 10 * 1024 * 1024;

    template <class TInitializeActor>
    std::shared_ptr<arrow::Array> InitializePosition(const TString& key, const ui32 recordsCount, const TInitializeActor actor) {
        auto it = Arrays.find(key);
        if (it == Arrays.end() || it->second->length() < recordsCount) {
            auto arrNew = actor(recordsCount);
            if (NArrow::GetArrayMemorySize(arrNew->data()) < MaxOneArrayMemorySize) {
                if (it == Arrays.end()) {
                    Arrays.emplace(key, arrNew);
                } else {
                    it->second = arrNew;
                }
            }
            return arrNew;
        } else {
            return it->second->Slice(0, recordsCount);
        }
    }

    std::shared_ptr<arrow::Array> GetNullImpl(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount);
    std::shared_ptr<arrow::Array> GetConstImpl(const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<arrow::Scalar>& scalar, const ui32 recordsCount);
public:
    static std::shared_ptr<arrow::Array> GetNull(const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount);
    static std::shared_ptr<arrow::Array> GetConst(const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<arrow::Scalar>& scalar, const ui32 recordsCount);
};
}
