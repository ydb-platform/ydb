#pragma once
#include "signature.h"

#include <util/datetime/base.h>
#include <util/string/builder.h>

#include <limits>

#ifndef LWTRACE_DISABLE

namespace NLWTrace {

    template <>
    struct TParamTraits<TInstant> {
        using TStoreType = double;
        using TFuncParam = TInstant;

        inline static void ToString(TStoreType value, TString* out) {
            *out = TParamConv<TStoreType>::ToString(value);
        }

        inline static TStoreType ToStoreType(TInstant value) {
            if (value == TInstant::Max()) {
                return std::numeric_limits<TStoreType>::infinity();
            } else {
                return static_cast<TStoreType>(value.MicroSeconds()) / 1000000.0; // seconds count
            }
        }
    };

    template <>
    struct TParamTraits<TDuration> {
        using TStoreType = double;
        using TFuncParam = TDuration;

        inline static void ToString(TStoreType value, TString* out) {
            *out = TParamConv<TStoreType>::ToString(value);
        }

        inline static TStoreType ToStoreType(TDuration value) {
            if (value == TDuration::Max()) {
                return std::numeric_limits<TStoreType>::infinity();
            } else {
                return static_cast<TStoreType>(value.MicroSeconds()) / 1000.0; // milliseconds count
            }
        }
    };

    // Param for enum with GENERATE_ENUM_SERIALIZATION enabled or operator<< implemented
    template <class TEnum>
    struct TEnumParamWithSerialization {
        using TStoreType = typename TParamTraits<std::underlying_type_t<TEnum>>::TStoreType;
        using TFuncParam = TEnum;

        inline static void ToString(TStoreType stored, TString* out) {
            *out = TStringBuilder() << static_cast<TEnum>(stored) << " (" << stored << ")";
        }

        inline static TStoreType ToStoreType(TFuncParam v) {
            return static_cast<TStoreType>(v);
        }
    };

} // namespace NLWTrace

#endif // LWTRACE_DISABLE
