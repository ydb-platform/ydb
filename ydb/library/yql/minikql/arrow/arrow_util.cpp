#include "arrow_util.h"
#include "mkql_bit_utils.h"

#include <arrow/array/array_base.h>
#include <arrow/chunked_array.h>

#include <ydb/library/yql/minikql/mkql_node_builder.h>

#include <util/system/yassert.h>

namespace NKikimr::NMiniKQL {

std::shared_ptr<arrow::ArrayData> Unwrap(const arrow::ArrayData& data, TType* itemType) {
    bool nested;
    if (itemType->IsPg()) {
        nested = false;
    } else {
        bool isOptional;
        auto unpacked = UnpackOptional(itemType, isOptional);
        MKQL_ENSURE(isOptional, "Expected optional");
        if (unpacked->IsOptional() || unpacked->IsVariant() || unpacked->IsPg()) {
            nested = true;
        } else {
            nested = false;
        }
    }

    if (nested) {
        MKQL_ENSURE(data.child_data.size() == 1, "Expected struct with one element");
        return data.child_data[0];
    } else {
        auto buffers = data.buffers;
        MKQL_ENSURE(buffers.size() >= 1, "Missing nullable bitmap");
        buffers[0] = nullptr;
        return arrow::ArrayData::Make(data.type, data.length, buffers, data.child_data, data.dictionary, 0, data.offset);
    }
}

template <typename T>
std::shared_ptr<arrow::DataType> GetPrimitiveDataType() {
    static std::shared_ptr<arrow::DataType> result = std::make_shared<typename TPrimitiveDataType<T>::TResult>();
    return result;
}

template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<bool>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i8>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui8>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i16>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui16>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i32>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui32>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i64>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui64>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<float>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<double>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<char*>();
template std::shared_ptr<arrow::DataType> GetPrimitiveDataType<NYql::NUdf::TUtf8>();

}
