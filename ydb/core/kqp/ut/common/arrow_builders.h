#pragma once

#include <ydb/core/base/defs.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>

#include <memory>
#include <optional>
#include <vector>

namespace NKikimr::NKqp::NTestArrow {

template <class ArrowType, class CppType>
inline std::shared_ptr<arrow::Array> MakeArrayNullable(const std::vector<std::optional<CppType>>& values) {
    using BuilderT = typename arrow::TypeTraits<ArrowType>::BuilderType;
    BuilderT builder;
    for (auto&& v : values) {
        if (v.has_value()) {
            Y_ABORT_UNLESS(builder.Append(static_cast<CppType>(*v)).ok());
        } else {
            Y_ABORT_UNLESS(builder.AppendNull().ok());
        }
    }

    std::shared_ptr<arrow::Array> out;
    Y_ABORT_UNLESS(builder.Finish(&out).ok());
    return out;
}

template <class ArrowType, class CppType>
inline std::shared_ptr<arrow::Array> MakeArray(const std::vector<CppType>& values) {
    using BuilderT = typename arrow::TypeTraits<ArrowType>::BuilderType;
    BuilderT builder;
    for (auto&& v : values) {
        Y_ABORT_UNLESS(builder.Append(static_cast<CppType>(v)).ok());
    }

    std::shared_ptr<arrow::Array> out;
    Y_ABORT_UNLESS(builder.Finish(&out).ok());
    return out;
}

inline std::shared_ptr<arrow::Array> MakeInt32Array(const std::vector<int32_t>& v) {
    return MakeArray<arrow::Int32Type, int32_t>(v);
}

inline std::shared_ptr<arrow::Array> MakeInt64Array(const std::vector<int64_t>& v) {
    return MakeArray<arrow::Int64Type, int64_t>(v);
}

inline std::shared_ptr<arrow::Array> MakeUInt8Array(const std::vector<uint8_t>& v) {
    return MakeArray<arrow::UInt8Type, uint8_t>(v);
}

inline std::shared_ptr<arrow::Array> MakeInt32ArrayNullable(const std::vector<std::optional<int32_t>>& v) {
    return MakeArrayNullable<arrow::Int32Type, int32_t>(v);
}

inline std::shared_ptr<arrow::Array> MakeInt64ArrayNullable(const std::vector<std::optional<int64_t>>& v) {
    return MakeArrayNullable<arrow::Int64Type, int64_t>(v);
}

inline std::shared_ptr<arrow::Array> MakeUInt8ArrayNullable(const std::vector<std::optional<uint8_t>>& v) {
    return MakeArrayNullable<arrow::UInt8Type, uint8_t>(v);
}

inline std::shared_ptr<arrow::Array> MakeBoolArrayAsUInt8(const std::vector<bool>& v) {
    std::vector<uint8_t> u8;
    u8.reserve(v.size());
    for (auto&& b : v) {
        u8.push_back(b ? 1u : 0u);
    }
 
    return MakeUInt8Array(u8);
}

inline std::shared_ptr<arrow::Array> MakeBoolArrayAsUInt8Nullable(const std::vector<std::optional<bool>>& v) {
    std::vector<std::optional<uint8_t>> u8;
    u8.reserve(v.size());
    for (auto&& b : v) {
        u8.emplace_back(b.has_value() ? std::optional<uint8_t>(*b ? 1u : 0u) : std::nullopt);
    }

    return MakeUInt8ArrayNullable(u8);
}

inline std::shared_ptr<arrow::RecordBatch> MakeBatch(
    const std::vector<std::shared_ptr<arrow::Field>>& fields, const std::vector<std::shared_ptr<arrow::Array>>& columns) {
    auto schema = arrow::schema(fields);
    const int64_t length = columns.empty() ? 0 : columns.front()->length();
    return arrow::RecordBatch::Make(schema, length, columns);
}

}   // namespace NKikimr::NKqp::NTestArrow
