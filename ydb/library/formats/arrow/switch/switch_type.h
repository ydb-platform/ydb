#pragma once
#include <ydb/library/formats/arrow/validation/validation.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/xxhash/xxhash.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/catalog/pg_type_d.h>
}

namespace NKikimr::NArrow {

template <typename TType>
class TTypeWrapper {
private:
    template <typename T, bool IsCType>
    struct ValueTypeSelector {
        using type = typename arrow::TypeTraits<T>::CType;
    };

    template <typename T>
    struct ValueTypeSelector<T, false> {
        using type = arrow::util::string_view;
    };

public:
    using T = TType;
    static constexpr bool IsCType = arrow::has_c_type<T>() && !std::is_same_v<arrow::HalfFloatType, T>;
    static constexpr bool IsStringView = arrow::has_string_view<T>();
    static_assert(!IsCType || !IsStringView);
    static constexpr bool IsAppropriate = IsCType || IsStringView;
    static constexpr bool IsIndexType() {
        return std::is_same_v<arrow::UInt32Type, T> || std::is_same_v<arrow::UInt16Type, T> || std::is_same_v<arrow::UInt8Type, T>;
    }
    using ValueType = ValueTypeSelector<T, IsCType>::type;
    using TArray = typename arrow::TypeTraits<T>::ArrayType;
    using TBuilder = typename arrow::TypeTraits<T>::BuilderType;
    using TScalar = typename arrow::TypeTraits<T>::ScalarType;

    template <class TExt>
    static TBuilder* CastBuilder(TExt* builder) {
        return static_cast<TBuilder*>(builder);
    }

    template <class TExt>
    static TScalar* CastScalar(TExt* scalar) {
        return static_cast<TScalar*>(scalar);
    }

    template <class TExt>
    static const TArray* CastArray(const TExt* arr) {
        return static_cast<const TArray*>(arr);
    }

    template <class TExt>
    static TArray* CastArray(TExt* arr) {
        return static_cast<TArray*>(arr);
    }

    template <class TExt>
    static std::shared_ptr<TArray> CastArray(const std::shared_ptr<TExt>& arr) {
        return std::static_pointer_cast<TArray>(arr);
    }

    ui64 CalcHash(const ValueType val) const {
        if constexpr (IsCType) {
            return XXH3_64bits((const ui8*)&val, sizeof(val));
        }
        if constexpr (IsStringView) {
            return XXH3_64bits((const ui8*)val.data(), val.size());
        }
        Y_FAIL();
    }

    void AppendValue(arrow::ArrayBuilder& builder, const ValueType val) const {
        if constexpr (IsCType) {
            TStatusValidator::Validate(static_cast<TBuilder&>(builder).Append(val));
            return;
        }
        if constexpr (IsStringView) {
            TStatusValidator::Validate(static_cast<TBuilder&>(builder).Append(val));
            return;
        }
        Y_FAIL();
    }

    template <class TValue>
    std::shared_ptr<arrow::Scalar> BuildScalar(const TValue val, const std::shared_ptr<arrow::DataType>& dType) const {
        if constexpr (IsCType) {
            if constexpr (arrow::is_parameter_free_type<TType>::value) {
                return std::make_shared<TScalar>(val);
            }
            if constexpr (!arrow::is_parameter_free_type<TType>::value) {
                return std::make_shared<TScalar>(val, dType);
            }
        }
        if constexpr (IsStringView) {
            if constexpr (std::is_same<TValue, arrow::util::string_view>::value) {
                if constexpr (arrow::is_parameter_free_type<TType>::value) {
                    return std::make_shared<TScalar>(arrow::Buffer::FromString(std::string(val.data(), val.size())));
                }
                if constexpr (!arrow::is_parameter_free_type<TType>::value) {
                    return std::make_shared<TScalar>(arrow::Buffer::FromString(std::string(val.data(), val.size())), dType);
                }
            }
            if constexpr (!std::is_same<TValue, arrow::util::string_view>::value) {
                if constexpr (arrow::is_parameter_free_type<TType>::value) {
                    return std::make_shared<TScalar>(arrow::Buffer::FromString(val));
                }
                if constexpr (!arrow::is_parameter_free_type<TType>::value) {
                    return std::make_shared<TScalar>(arrow::Buffer::FromString(val), dType);
                }
            }
        }
        Y_FAIL();
        return nullptr;
    }

    TString ToString(const ValueType& value) const {
        if constexpr (IsCType) {
            return ::ToString(value);
        }
        if constexpr (IsStringView) {
            return TString(value.data(), value.size());
        }
        Y_FAIL();
        return "";
    }

    ValueType GetValue(const TArray& arr, const ui32 index) const {
        if constexpr (IsCType) {
            return arr.Value(index);
        }
        if constexpr (IsStringView) {
            return arr.GetView(index);
        }
        Y_FAIL();
        return ValueType{};
    }
};

template <class TResult, TResult defaultValue, typename TFunc, bool EnableNull = false>
TResult SwitchTypeImpl(arrow::Type::type typeId, TFunc&& f) {
    switch (typeId) {
        case arrow::Type::NA: {
            if constexpr (EnableNull) {
                return f(TTypeWrapper<arrow::NullType>());
            }
            break;
        }
        case arrow::Type::BOOL:
            return f(TTypeWrapper<arrow::BooleanType>());
        case arrow::Type::UINT8:
            return f(TTypeWrapper<arrow::UInt8Type>());
        case arrow::Type::INT8:
            return f(TTypeWrapper<arrow::Int8Type>());
        case arrow::Type::UINT16:
            return f(TTypeWrapper<arrow::UInt16Type>());
        case arrow::Type::INT16:
            return f(TTypeWrapper<arrow::Int16Type>());
        case arrow::Type::UINT32:
            return f(TTypeWrapper<arrow::UInt32Type>());
        case arrow::Type::INT32:
            return f(TTypeWrapper<arrow::Int32Type>());
        case arrow::Type::UINT64:
            return f(TTypeWrapper<arrow::UInt64Type>());
        case arrow::Type::INT64:
            return f(TTypeWrapper<arrow::Int64Type>());
        case arrow::Type::HALF_FLOAT:
            return f(TTypeWrapper<arrow::HalfFloatType>());
        case arrow::Type::FLOAT:
            return f(TTypeWrapper<arrow::FloatType>());
        case arrow::Type::DOUBLE:
            return f(TTypeWrapper<arrow::DoubleType>());
        case arrow::Type::STRING:
            return f(TTypeWrapper<arrow::StringType>());
        case arrow::Type::BINARY:
            return f(TTypeWrapper<arrow::BinaryType>());
        case arrow::Type::FIXED_SIZE_BINARY:
            return f(TTypeWrapper<arrow::FixedSizeBinaryType>());
        case arrow::Type::DATE32:
            return f(TTypeWrapper<arrow::Date32Type>());
        case arrow::Type::DATE64:
            return f(TTypeWrapper<arrow::Date64Type>());
        case arrow::Type::TIMESTAMP:
            return f(TTypeWrapper<arrow::TimestampType>());
        case arrow::Type::TIME32:
            return f(TTypeWrapper<arrow::Time32Type>());
        case arrow::Type::TIME64:
            return f(TTypeWrapper<arrow::Time64Type>());
        case arrow::Type::INTERVAL_MONTHS:
            return f(TTypeWrapper<arrow::MonthIntervalType>());
        case arrow::Type::DECIMAL:
            return f(TTypeWrapper<arrow::Decimal128Type>());
        case arrow::Type::DURATION:
            return f(TTypeWrapper<arrow::DurationType>());
        case arrow::Type::LARGE_STRING:
            return f(TTypeWrapper<arrow::LargeStringType>());
        case arrow::Type::LARGE_BINARY:
            return f(TTypeWrapper<arrow::LargeBinaryType>());
        case arrow::Type::DECIMAL256:
        case arrow::Type::DENSE_UNION:
        case arrow::Type::DICTIONARY:
        case arrow::Type::EXTENSION:
        case arrow::Type::FIXED_SIZE_LIST:
        case arrow::Type::INTERVAL_DAY_TIME:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::LIST:
        case arrow::Type::MAP:
        case arrow::Type::MAX_ID:
        case arrow::Type::SPARSE_UNION:
        case arrow::Type::STRUCT:
            break;
    }

    return defaultValue;
}

template <typename TFunc, bool EnableNull = false>
bool SwitchType(arrow::Type::type typeId, TFunc&& f) {
    return SwitchTypeImpl<bool, false, TFunc, EnableNull>(typeId, std::move(f));
}

template <typename TFunc>
bool SwitchTypeWithNull(arrow::Type::type typeId, TFunc&& f) {
    return SwitchType<TFunc, true>(typeId, std::move(f));
}

template <typename TFunc>
bool SwitchArrayType(const arrow::Datum& column, TFunc&& f) {
    auto type = column.type();
    Y_ABORT_UNLESS(type);
    return SwitchType(type->id(), std::forward<TFunc>(f));
}

template <typename T>
bool Append(arrow::ArrayBuilder& builder, const typename T::c_type& value) {
    using TBuilder = typename arrow::TypeTraits<T>::BuilderType;
    TStatusValidator::Validate(static_cast<TBuilder&>(builder).Append(value));
    return true;
}

template <typename T>
bool AppendValues(arrow::ArrayBuilder& builder, const typename T::c_type& value, const ui32 count) {
    using TBuilder = typename arrow::TypeTraits<T>::BuilderType;
    for (ui32 i = 0; i < count; ++i) {
        TStatusValidator::Validate(static_cast<TBuilder&>(builder).Append(value));
    }
    return true;
}

template <typename T>
bool Append(arrow::ArrayBuilder& builder, arrow::util::string_view value) {
    using TBuilder = typename arrow::TypeTraits<T>::BuilderType;

    TStatusValidator::Validate(static_cast<TBuilder&>(builder).Append(value));
    return true;
}

template <typename T>
bool Append(arrow::ArrayBuilder& builder, const typename T::c_type* values, size_t size) {
    using TBuilder = typename arrow::NumericBuilder<T>;

    TStatusValidator::Validate(static_cast<TBuilder&>(builder).AppendValues(values, size));
    return true;
}

template <typename T>
bool Append(arrow::ArrayBuilder& builder, const std::vector<typename T::c_type>& values) {
    using TBuilder = typename arrow::NumericBuilder<T>;

    TStatusValidator::Validate(static_cast<TBuilder&>(builder).AppendValues(values.data(), values.size()));
    return true;
}

template <typename T>
[[nodiscard]] bool Append(T& builder, const arrow::Type::type typeId, const arrow::Array& array, int position, ui64* recordSize = nullptr) {
    Y_DEBUG_ABORT_UNLESS(builder.type()->id() == array.type_id());
    Y_DEBUG_ABORT_UNLESS(typeId == array.type_id());
    return SwitchType(typeId, [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
        using TBuilder = typename arrow::TypeTraits<typename TWrap::T>::BuilderType;

        auto& typedArray = static_cast<const TArray&>(array);
        auto& typedBuilder = static_cast<TBuilder&>(builder);

        if (typedArray.IsNull(position)) {
            TStatusValidator::Validate(typedBuilder.AppendNull());
            if (recordSize) {
                *recordSize += 4;
            }
            return true;
        } else {
            if constexpr (!arrow::has_string_view<typename TWrap::T>::value) {
                TStatusValidator::Validate(typedBuilder.Append(typedArray.GetView(position)));
                if (recordSize) {
                    *recordSize += sizeof(typedArray.GetView(position));
                }
                return true;
            }
            if constexpr (arrow::has_string_view<typename TWrap::T>::value) {
                TStatusValidator::Validate(typedBuilder.Append(typedArray.GetView(position)));
                if (recordSize) {
                    *recordSize += typedArray.GetView(position).size();
                }
                return true;
            }
        }
        Y_ABORT_UNLESS(false, "unpredictable variant");
        return false;
    });
}

template <typename T>
[[nodiscard]] bool Append(T& builder, const arrow::Array& array, int position, ui64* recordSize = nullptr) {
    return Append<T>(builder, array.type_id(), array, position, recordSize);
}

}   // namespace NKikimr::NArrow
