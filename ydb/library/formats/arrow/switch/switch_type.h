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
        using type = typename arrow20::TypeTraits<T>::CType;
    };

    template <typename T>
    struct ValueTypeSelector<T, false> {
        using type = arrow20::util::string_view;
    };

public:
    using T = TType;
    static constexpr bool IsCType = arrow20::has_c_type<T>() && !std::is_same_v<arrow20::HalfFloatType, T>;
    static constexpr bool IsStringView = arrow20::has_string_view<T>();
    static_assert(!IsCType || !IsStringView);
    static constexpr bool IsAppropriate = IsCType || IsStringView;
    static constexpr bool IsIndexType() {
        return std::is_same_v<arrow20::UInt32Type, T> || std::is_same_v<arrow20::UInt16Type, T> || std::is_same_v<arrow20::UInt8Type, T>;
    }
    using ValueType = ValueTypeSelector<T, IsCType>::type;
    using TArray = typename arrow20::TypeTraits<T>::ArrayType;
    using TBuilder = typename arrow20::TypeTraits<T>::BuilderType;
    using TScalar = typename arrow20::TypeTraits<T>::ScalarType;

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

    void AppendValue(arrow20::ArrayBuilder& builder, const ValueType val) const {
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
    std::shared_ptr<arrow20::Scalar> BuildScalar(const TValue val, const std::shared_ptr<arrow20::DataType>& dType) const {
        if constexpr (IsCType) {
            if constexpr (arrow20::is_parameter_free_type<TType>::value) {
                return std::make_shared<TScalar>(val);
            }
            if constexpr (!arrow20::is_parameter_free_type<TType>::value) {
                return std::make_shared<TScalar>(val, dType);
            }
        }
        if constexpr (IsStringView) {
            if constexpr (std::is_same<TValue, arrow20::util::string_view>::value) {
                if constexpr (arrow20::is_parameter_free_type<TType>::value) {
                    return std::make_shared<TScalar>(arrow20::Buffer::FromString(std::string(val.data(), val.size())));
                }
                if constexpr (!arrow20::is_parameter_free_type<TType>::value) {
                    return std::make_shared<TScalar>(arrow20::Buffer::FromString(std::string(val.data(), val.size())), dType);
                }
            }
            if constexpr (!std::is_same<TValue, arrow20::util::string_view>::value) {
                if constexpr (arrow20::is_parameter_free_type<TType>::value) {
                    return std::make_shared<TScalar>(arrow20::Buffer::FromString(val));
                }
                if constexpr (!arrow20::is_parameter_free_type<TType>::value) {
                    return std::make_shared<TScalar>(arrow20::Buffer::FromString(val), dType);
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

    ValueType GetValue(const TScalar& scalar) const {
        if constexpr (IsCType) {
            return scalar.value;
        }
        if constexpr (IsStringView) {
            return (arrow20::util::string_view)*scalar.value;
        }
        Y_FAIL();
        return ValueType{};
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
TResult SwitchTypeImpl(arrow20::Type::type typeId, TFunc&& f) {
    switch (typeId) {
        case arrow20::Type::NA: {
            if constexpr (EnableNull) {
                return f(TTypeWrapper<arrow20::NullType>());
            }
            break;
        }
        case arrow20::Type::BOOL:
            return f(TTypeWrapper<arrow20::BooleanType>());
        case arrow20::Type::UINT8:
            return f(TTypeWrapper<arrow20::UInt8Type>());
        case arrow20::Type::INT8:
            return f(TTypeWrapper<arrow20::Int8Type>());
        case arrow20::Type::UINT16:
            return f(TTypeWrapper<arrow20::UInt16Type>());
        case arrow20::Type::INT16:
            return f(TTypeWrapper<arrow20::Int16Type>());
        case arrow20::Type::UINT32:
            return f(TTypeWrapper<arrow20::UInt32Type>());
        case arrow20::Type::INT32:
            return f(TTypeWrapper<arrow20::Int32Type>());
        case arrow20::Type::UINT64:
            return f(TTypeWrapper<arrow20::UInt64Type>());
        case arrow20::Type::INT64:
            return f(TTypeWrapper<arrow20::Int64Type>());
        case arrow20::Type::HALF_FLOAT:
            return f(TTypeWrapper<arrow20::HalfFloatType>());
        case arrow20::Type::FLOAT:
            return f(TTypeWrapper<arrow20::FloatType>());
        case arrow20::Type::DOUBLE:
            return f(TTypeWrapper<arrow20::DoubleType>());
        case arrow20::Type::STRING:
            return f(TTypeWrapper<arrow20::StringType>());
        case arrow20::Type::BINARY:
            return f(TTypeWrapper<arrow20::BinaryType>());
        case arrow20::Type::FIXED_SIZE_BINARY:
            return f(TTypeWrapper<arrow20::FixedSizeBinaryType>());
        case arrow20::Type::DATE32:
            return f(TTypeWrapper<arrow20::Date32Type>());
        case arrow20::Type::DATE64:
            return f(TTypeWrapper<arrow20::Date64Type>());
        case arrow20::Type::TIMESTAMP:
            return f(TTypeWrapper<arrow20::TimestampType>());
        case arrow20::Type::TIME32:
            return f(TTypeWrapper<arrow20::Time32Type>());
        case arrow20::Type::TIME64:
            return f(TTypeWrapper<arrow20::Time64Type>());
        case arrow20::Type::INTERVAL_MONTHS:
            return f(TTypeWrapper<arrow20::MonthIntervalType>());
        case arrow20::Type::DECIMAL:
            return f(TTypeWrapper<arrow20::Decimal128Type>());
        case arrow20::Type::DURATION:
            return f(TTypeWrapper<arrow20::DurationType>());
        case arrow20::Type::LARGE_STRING:
            return f(TTypeWrapper<arrow20::LargeStringType>());
        case arrow20::Type::LARGE_BINARY:
            return f(TTypeWrapper<arrow20::LargeBinaryType>());
        case arrow20::Type::DECIMAL256:
        case arrow20::Type::DENSE_UNION:
        case arrow20::Type::DICTIONARY:
        case arrow20::Type::EXTENSION:
        case arrow20::Type::FIXED_SIZE_LIST:
        case arrow20::Type::INTERVAL_DAY_TIME:
        case arrow20::Type::LARGE_LIST:
        case arrow20::Type::LIST:
        case arrow20::Type::MAP:
        case arrow20::Type::MAX_ID:
        case arrow20::Type::SPARSE_UNION:
        case arrow20::Type::STRUCT:
            break;
    }

    return defaultValue;
}

template <typename TFunc, bool EnableNull = false>
bool SwitchType(arrow20::Type::type typeId, TFunc&& f) {
    return SwitchTypeImpl<bool, false, TFunc, EnableNull>(typeId, std::move(f));
}

template <typename TFunc>
bool SwitchTypeWithNull(arrow20::Type::type typeId, TFunc&& f) {
    return SwitchType<TFunc, true>(typeId, std::move(f));
}

template <typename TFunc>
bool SwitchArrayType(const arrow20::Datum& column, TFunc&& f) {
    auto type = column.type();
    Y_ABORT_UNLESS(type);
    return SwitchType(type->id(), std::forward<TFunc>(f));
}

template <typename T>
bool Append(arrow20::ArrayBuilder& builder, const typename T::c_type& value) {
    using TBuilder = typename arrow20::TypeTraits<T>::BuilderType;
    TStatusValidator::Validate(static_cast<TBuilder&>(builder).Append(value));
    return true;
}

template <typename T>
bool AppendValues(arrow20::ArrayBuilder& builder, const typename T::c_type& value, const ui32 count) {
    using TBuilder = typename arrow20::TypeTraits<T>::BuilderType;
    for (ui32 i = 0; i < count; ++i) {
        TStatusValidator::Validate(static_cast<TBuilder&>(builder).Append(value));
    }
    return true;
}

template <typename T>
bool Append(arrow20::ArrayBuilder& builder, arrow20::util::string_view value) {
    using TBuilder = typename arrow20::TypeTraits<T>::BuilderType;

    TStatusValidator::Validate(static_cast<TBuilder&>(builder).Append(value));
    return true;
}

template <typename T>
bool Append(arrow20::ArrayBuilder& builder, const typename T::c_type* values, size_t size) {
    using TBuilder = typename arrow20::NumericBuilder<T>;

    TStatusValidator::Validate(static_cast<TBuilder&>(builder).AppendValues(values, size));
    return true;
}

template <typename T>
bool Append(arrow20::ArrayBuilder& builder, const std::vector<typename T::c_type>& values) {
    using TBuilder = typename arrow20::NumericBuilder<T>;

    TStatusValidator::Validate(static_cast<TBuilder&>(builder).AppendValues(values.data(), values.size()));
    return true;
}

template <typename T>
[[nodiscard]] bool Append(T& builder, const arrow20::Type::type typeId, const arrow20::Array& array, int position, ui64* recordSize = nullptr) {
    Y_DEBUG_ABORT_UNLESS(builder.type()->id() == array.type_id());
    Y_DEBUG_ABORT_UNLESS(typeId == array.type_id());
    return SwitchType(typeId, [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow20::TypeTraits<typename TWrap::T>::ArrayType;
        using TBuilder = typename arrow20::TypeTraits<typename TWrap::T>::BuilderType;

        auto& typedArray = static_cast<const TArray&>(array);
        auto& typedBuilder = static_cast<TBuilder&>(builder);

        if (typedArray.IsNull(position)) {
            TStatusValidator::Validate(typedBuilder.AppendNull());
            if (recordSize) {
                *recordSize += 4;
            }
            return true;
        } else {
            if constexpr (!arrow20::has_string_view<typename TWrap::T>::value) {
                TStatusValidator::Validate(typedBuilder.Append(typedArray.GetView(position)));
                if (recordSize) {
                    *recordSize += sizeof(typedArray.GetView(position));
                }
                return true;
            }
            if constexpr (arrow20::has_string_view<typename TWrap::T>::value) {
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
[[nodiscard]] bool Append(T& builder, const arrow20::Array& array, int position, ui64* recordSize = nullptr) {
    return Append<T>(builder, array.type_id(), array, position, recordSize);
}

}   // namespace NKikimr::NArrow
