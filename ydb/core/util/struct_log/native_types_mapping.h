#pragma once

#include "native_types_support.h"
#include "overloaded.h"

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <functional>
#include <unordered_map>

namespace NKikimr::NStructuredLog {

enum class TNativeTypeCode : std::uint8_t {
    Int8 = 0,
    UInt8,
    Int16,
    UInt16,
    Int32,
    UInt32,
    Int64,
    UInt64,
    Bool,
    String,
    Float,
    Double,
    LongDouble
};

using TInvoker = std::function<bool(const void* data, std::size_t length)>;
template <typename TValueType, typename C>
static TInvoker CreateTypedInvoker(C& callable) {
    TInvoker invoker = [&callable](const void* data, std::size_t length)->bool {
        TValueType value;
        if (!TNativeTypeSupport<TValueType>::Deserialize(value, data, length) ) {
            return false;
        }
        callable(value);
        return true;
    };
    return invoker;
}
using TInvokerMap = std::unordered_map<TNativeTypeCode, TInvoker>;

template <typename T, TNativeTypeCode C>
struct TNativeTypeCodePair
{
    using Type = T;
    static constexpr TNativeTypeCode Code{C};
};

template <typename TPair, typename ... Other>
struct TNativeTypeCodeMapping
{
    using TBase = TNativeTypeCodeMapping<Other...>;
    using TValueType = typename TPair::Type;

    template <typename T>
    static TNativeTypeCode GetCode() {
        if constexpr( std::is_same_v<TValueType, T>) {
            return TPair::Code;
        } else {
            return TBase::template GetCode<T>();
        }
    }

    template <typename T>
    static inline void Serialize(const T& value, TBinaryData& data) {
        if constexpr (std::is_same_v<TValueType, T>) {
            TNativeTypeSupport<TValueType>::Serialize(value, data);
        } else {
            TBase::Serialize(value, data);
        }
    }

    template <typename T>
    static bool Deserialize(T& value, TNativeTypeCode code, const void* data, std::size_t length) {
        return Invoke(code, data, length,
            MakeOverloaded(
                [&value](const T& v) { value = v;},
                [](const auto& ) {}
            )
        );
    }

    template <typename T>
    static TString ToString(const T& value) {
        if constexpr (std::is_same_v<TValueType, T>) {
            return TNativeTypeSupport<TValueType>::ToString(value);
        } else {
            return TBase::ToString(value);
        }
    }

    template <typename T>
    static void AppendToString(const T& value, TStringBuilder& stringBuffer) {
        if constexpr (std::is_same_v<TValueType, T>) {
            return TNativeTypeSupport<TValueType>::AppendToString(value, stringBuffer);
        } else {
            return TBase::AppendToString(value, stringBuffer);
        }
    }

    template <typename C>
    static TInvokerMap CreateInvokerMap(C& callable) {
        TInvokerMap result = TBase:: template CreateInvokerMap(callable);
        TInvoker invoker = CreateTypedInvoker<TValueType>(callable);
        result.insert({TPair::Code, invoker});
        return result;
    }

    template <typename C>
    static bool Invoke(TNativeTypeCode code, const void* data, std::size_t length, const C& callable) {
        if (TPair::Code == code) {
            TValueType value;
            if (!TNativeTypeSupport<TValueType>::Deserialize(value, data, length) ) {
                return false;
            }
            callable(value);
            return true;
        } else {
            return TBase::Invoke(code, data, length, callable);
        }
    }
};

template <typename TPair>
struct TNativeTypeCodeMapping<TPair>
{
    using TValueType = typename TPair::Type;

    template <typename T>
    static TNativeTypeCode GetCode() {
        static_assert(std::is_same_v<TValueType, T>, "Unsupported type");
        return TPair::Code;
    }

    template <typename T>
    static void inline Serialize(const T& value, TBinaryData& data) {
        static_assert( std::is_same_v<TValueType, T>, "Unsupported type");
        TNativeTypeSupport<TValueType>::Serialize(value, data);
    }

    template <typename T>
    static bool Deserialize(T& value, TNativeTypeCode code, const void* data, std::size_t length) {
        return Invoke(code, data, length,
            MakeOverloaded(
                [&value](const T& v) { value = v;},
                [](const auto& ) {}
            )
        );
    }

    template <typename T>
    static TString ToString(const T& value) {
        static_assert( std::is_same_v<TValueType, T>, "Unsupported type");
        return TNativeTypeSupport<TValueType>::ToString(value);
    }

    template <typename T>
    static void AppendToString(const T& value, TStringBuilder& stringBuffer) {
        static_assert( std::is_same_v<TValueType, T>, "Unsupported type");
        return TNativeTypeSupport<TValueType>::AppendToString(value, stringBuffer);
    }

    template <typename C>
    static TInvokerMap CreateInvokerMap(C& callable) {
        TInvoker invoker = CreateTypedInvoker<TValueType>(callable);
        TInvokerMap result{{TPair::Code, invoker}};
        return result;
    }

    template <typename C>
    static bool Invoke(TNativeTypeCode code, const void* data, std::size_t length, const C& callable) {
        if (TPair::Code != code) {
            return false;
        }
        TValueType value;
        if (!TNativeTypeSupport<TValueType>::Deserialize(value, data, length) ) {
            return false;
        }
        callable(value);
        return true;
    }
};

using TTypesMapping = TNativeTypeCodeMapping<
    TNativeTypeCodePair<ui8, TNativeTypeCode::UInt8>,
    TNativeTypeCodePair<i8, TNativeTypeCode::Int8>,
    TNativeTypeCodePair<ui16, TNativeTypeCode::UInt16>,
    TNativeTypeCodePair<i16, TNativeTypeCode::Int16>,
    TNativeTypeCodePair<ui32, TNativeTypeCode::UInt32>,
    TNativeTypeCodePair<i32, TNativeTypeCode::Int32>,
    TNativeTypeCodePair<ui64, TNativeTypeCode::UInt64>,
    TNativeTypeCodePair<i64, TNativeTypeCode::Int64>,
    TNativeTypeCodePair<bool, TNativeTypeCode::Bool>,
    TNativeTypeCodePair<TString, TNativeTypeCode::String>,
    TNativeTypeCodePair<float, TNativeTypeCode::Float>,
    TNativeTypeCodePair<double, TNativeTypeCode::Double>,
    TNativeTypeCodePair<long double, TNativeTypeCode::LongDouble>
>;

}
