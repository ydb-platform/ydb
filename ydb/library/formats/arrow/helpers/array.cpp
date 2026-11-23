#include "array.h"

#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <util/system/yassert.h>

#include <type_traits>

namespace NKikimr::NKernels {

std::shared_ptr<arrow::Array> NumVecToArray(
    const std::shared_ptr<arrow::DataType>& type, const std::vector<double>& vec, std::optional<double> nullValue) {
    std::shared_ptr<arrow::Array> out;
    NArrow::SwitchType(type->id(), [&](const auto& t) {
        using TWrap = std::decay_t<decltype(t)>;
        using T = typename TWrap::T;

        if constexpr (arrow::is_number_type<T>::value) {
            typename arrow::TypeTraits<T>::BuilderType builder;
            for (const auto val : vec) {
                if (nullValue && *nullValue == val) {
                    Y_ABORT_UNLESS(builder.AppendNull().ok());
                } else {
                    Y_ABORT_UNLESS(builder.Append(static_cast<typename T::c_type>(val)).ok());
                }
            }
            Y_ABORT_UNLESS(builder.Finish(&out).ok());
            return true;
        } else if constexpr (arrow::is_timestamp_type<T>()) {
            typename arrow::TypeTraits<T>::BuilderType builder(type, arrow::default_memory_pool());
            for (const auto val : vec) {
                Y_ABORT_UNLESS(builder.Append(static_cast<typename T::c_type>(val)).ok());
            }
            Y_ABORT_UNLESS(builder.Finish(&out).ok());
            return true;
        }
        return false;
    });
    return out;
}

std::shared_ptr<arrow::Array> BoolVecToArray(const std::vector<std::optional<bool>>& vec) {
    std::shared_ptr<arrow::Array> out;
    arrow::BooleanBuilder builder;
    for (const auto& value : vec) {
        if (value) {
            Y_ABORT_UNLESS(builder.Append(*value).ok());
        } else {
            Y_ABORT_UNLESS(builder.AppendNull().ok());
        }
    }
    Y_ABORT_UNLESS(builder.Finish(&out).ok());
    return out;
}

std::shared_ptr<arrow::Array> StringVecToArray(const std::vector<std::optional<std::string>>& vec) {
    std::shared_ptr<arrow::Array> out;
    arrow::StringBuilder builder;
    for (const auto& value : vec) {
        if (value) {
            Y_ABORT_UNLESS(builder.Append(*value).ok());
        } else {
            Y_ABORT_UNLESS(builder.AppendNull().ok());
        }
    }
    Y_ABORT_UNLESS(builder.Finish(&out).ok());
    return out;
}

std::shared_ptr<arrow::Array> UInt8VecToArray(const std::vector<std::optional<uint8_t>>& vec) {
    std::shared_ptr<arrow::Array> out;
    arrow::UInt8Builder builder;
    for (const auto& value : vec) {
        if (value) {
            Y_ABORT_UNLESS(builder.Append(*value).ok());
        } else {
            Y_ABORT_UNLESS(builder.AppendNull().ok());
        }
    }
    Y_ABORT_UNLESS(builder.Finish(&out).ok());
    return out;
}

}   // namespace NKikimr::NKernels
