#pragma once

#include "preprocessor.h"

#include <library/cpp/lwtrace/protos/lwtrace.pb.h>

#include <util/generic/cast.h>
#include <util/generic/string.h>
#include <util/generic/typetraits.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_enum_reflection.h>

#include <library/cpp/lwtrace/protos/lwtrace.pb.h>

#include <type_traits>

namespace NLWTrace {
    // Class to hold parameter values parsed from trace query predicate operators
    template <class T>
    struct TParamConv {
        static T FromString(const TString& text) {
            return ::FromString<T>(text);
        }
        static TString ToString(const T& param) {
            return ::ToString(param);
        }
    };

    template <>
    struct TParamConv<TString const*> {
        static TString FromString(const TString& text) {
            return text;
        }
        static TString ToString(TString const* param) {
            return TString(*param);
        }
    };

    template <>
    struct TParamConv<ui8> {
        static ui8 FromString(const TString& text) {
            return (ui8)::FromString<ui16>(text);
        }
        static TString ToString(ui8 param) {
            return ::ToString((ui16)param);
        }
    };

    template <>
    struct TParamConv<i8> {
        static i8 FromString(const TString& text) {
            return (i8)::FromString<i16>(text);
        }
        static TString ToString(i8 param) {
            return ::ToString((i16)param);
        }
    };

    template <>
    struct TParamConv<double> {
        static double FromString(const TString& text) {
            return ::FromString<double>(text);
        }
        static TString ToString(double param) {
            return Sprintf("%.6lf", param);
        }
    };

    // Fake parameter type used as a placeholder for not used parameters (above the number of defined params for a specific probe)
    class TNil {
    };

    // Struct that holds and handles a value of parameter of any supported type.
    struct TParam {
        char Data[LWTRACE_MAX_PARAM_SIZE];

        template <class T>
        const T& Get() const {
            return *reinterpret_cast<const T*>(Data);
        }

        template <class T>
        T& Get() {
            return *reinterpret_cast<T*>(Data);
        }

        template <class T>
        void DefaultConstruct() {
            new (&Data) T();
        }

        template <class T>
        void CopyConstruct(const T& other) {
            new (&Data) T(other);
        }

        template <class T>
        void Destruct() {
            Get<T>().~T();
        }
    };

    template <>
    inline void TParam::DefaultConstruct<TNil>() {
    }

    template <>
    inline void TParam::CopyConstruct<TNil>(const TNil&) {
    }

    template <>
    inline void TParam::Destruct<TNil>() {
    }

    class TTypedParam {
    private:
        EParamTypePb Type;
        TParam Param; // Contains garbage if PT_UNKNOWN
    public:
        TTypedParam()
            : Type(PT_UNKNOWN)
        {
        }

        explicit TTypedParam(EParamTypePb type)
            : Type(type)
        {
            switch (Type) {
#define FOREACH_PARAMTYPE_MACRO(n, t, v) \
    case PT_##v:                         \
        Param.DefaultConstruct<t>();     \
        return;
                FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO
                case PT_UNKNOWN:
                    return;
                default:
                    Y_ABORT("unknown param type");
            }
        }

        template <class T>
        explicit TTypedParam(const T& x, EParamTypePb type = PT_UNKNOWN)
            : Type(type)
        {
            Param.CopyConstruct<T>(x);
        }

#define FOREACH_PARAMTYPE_MACRO(n, t, v) \
    explicit TTypedParam(const t& x)     \
        : Type(PT_##v) {                 \
        Param.CopyConstruct<t>(x);       \
    }
        FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO

        TTypedParam(const TTypedParam& o) {
            Assign(o);
        }

        TTypedParam& operator=(const TTypedParam& o) {
            Reset();
            Assign(o);
            return *this;
        }

        void Assign(const TTypedParam& o) {
            Type = o.Type;
            switch (Type) {
#define FOREACH_PARAMTYPE_MACRO(n, t, v)          \
    case PT_##v:                                  \
        Param.CopyConstruct<t>(o.Param.Get<t>()); \
        return;
                FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO
                case PT_UNKNOWN:
                    return;
                default:
                    Y_ABORT("unknown param type");
            }
        }

        TTypedParam(TTypedParam&& o)
            : Type(o.Type)
            , Param(o.Param)
        {
            o.Type = PT_UNKNOWN; // To avoid Param destroy by source object dtor
        }

        TTypedParam(EParamTypePb type, const TParam& param)
            : Type(type)
        {
            Y_UNUSED(param); // for disabled lwtrace
            switch (Type) {
#define FOREACH_PARAMTYPE_MACRO(n, t, v)        \
    case PT_##v:                                \
        Param.CopyConstruct<t>(param.Get<t>()); \
        return;
                FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO
                case PT_UNKNOWN:
                    return;
                default:
                    Y_ABORT("unknown param type");
            }
        }

        ~TTypedParam() {
            Reset();
        }

        void Reset() {
            switch (Type) {
#define FOREACH_PARAMTYPE_MACRO(n, t, v) \
    case PT_##v:                         \
        Param.Destruct<t>();             \
        return;
                FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO
                case PT_UNKNOWN:
                    return;
                default:
                    Y_ABORT("unknown param type");
            }
            Type = PT_UNKNOWN;
        }

        bool operator==(const TTypedParam& rhs) const {
            if (Y_LIKELY(Type == rhs.Type)) {
                switch (Type) {
#define FOREACH_PARAMTYPE_MACRO(n, t, v) \
    case PT_##v:                         \
        return Param.Get<t>() == rhs.Param.Get<t>();
                    FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO
                    case PT_UNKNOWN:
                        return false; // All unknowns are equal
                    default:
                        Y_ABORT("unknown param type");
                }
            } else {
                return false;
            }
        }
        bool operator!=(const TTypedParam& rhs) const {
            return !operator==(rhs);
        }

        bool operator<(const TTypedParam& rhs) const {
            if (Y_LIKELY(Type == rhs.Type)) {
                switch (Type) {
#define FOREACH_PARAMTYPE_MACRO(n, t, v) \
    case PT_##v:                         \
        return Param.Get<t>() < rhs.Param.Get<t>();
                    FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO
                    case PT_UNKNOWN:
                        return false; // All unknowns are equal
                    default:
                        Y_ABORT("unknown param type");
                }
            } else {
                return Type < rhs.Type;
            }
        }

        bool operator<=(const TTypedParam& rhs) const {
            if (Y_LIKELY(Type == rhs.Type)) {
                switch (Type) {
#define FOREACH_PARAMTYPE_MACRO(n, t, v) \
    case PT_##v:                         \
        return Param.Get<t>() <= rhs.Param.Get<t>();
                    FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO
                    case PT_UNKNOWN:
                        return true; // All unknowns are equal
                    default:
                        Y_ABORT("unknown param type");
                }
            } else {
                return Type < rhs.Type;
            }
        }

        bool operator>(const TTypedParam& rhs) const {
            return !operator<=(rhs);
        }
        bool operator>=(const TTypedParam& rhs) const {
            return !operator<(rhs);
        }

        EParamTypePb GetType() const {
            return Type;
        }
        const TParam& GetParam() const {
            return Param;
        }
    };

    class TLiteral {
    private:
        TTypedParam Values[EParamTypePb_ARRAYSIZE];

    public:
        explicit TLiteral(const TString& text) {
            Y_UNUSED(text); /* That's for windows, where we have lwtrace disabled. */

#define FOREACH_PARAMTYPE_MACRO(n, t, v)                               \
    try {                                                              \
        Values[PT_##v] = TTypedParam(TParamConv<t>::FromString(text)); \
    } catch (...) {                                                    \
        Values[PT_##v] = TTypedParam();                                \
    }                                                                  \
    /**/
            FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO
        }

        TLiteral() {
        }

        TLiteral(const TLiteral& o) {
            for (size_t i = 0; i < EParamTypePb_ARRAYSIZE; i++) {
                Values[i] = o.Values[i];
            }
        }

        TLiteral& operator=(const TLiteral& o) {
            for (size_t i = 0; i < EParamTypePb_ARRAYSIZE; i++) {
                Values[i] = o.Values[i];
            }
            return *this;
        }

        const TTypedParam& GetValue(EParamTypePb type) const {
            return Values[type];
        }

        bool operator==(const TTypedParam& rhs) const {
            return Values[rhs.GetType()] == rhs;
        }
        bool operator!=(const TTypedParam& rhs) const {
            return !operator==(rhs);
        }

        bool operator<(const TTypedParam& rhs) const {
            return Values[rhs.GetType()] < rhs;
        }

        bool operator<=(const TTypedParam& rhs) const {
            return Values[rhs.GetType()] <= rhs;
        }

        bool operator>(const TTypedParam& rhs) const {
            return !operator<=(rhs);
        }
        bool operator>=(const TTypedParam& rhs) const {
            return !operator<(rhs);
        }
    };

    inline bool operator==(const TTypedParam& lhs, const TLiteral& rhs) {
        return lhs == rhs.GetValue(lhs.GetType());
    }
    inline bool operator!=(const TTypedParam& lhs, const TLiteral& rhs) {
        return !operator==(lhs, rhs);
    }

    inline bool operator<(const TTypedParam& lhs, const TLiteral& rhs) {
        return lhs < rhs.GetValue(lhs.GetType());
    }

    inline bool operator<=(const TTypedParam& lhs, const TLiteral& rhs) {
        return lhs <= rhs.GetValue(lhs.GetType());
    }

    inline bool operator>(const TTypedParam& lhs, const TLiteral& rhs) {
        return !operator<=(lhs, rhs);
    }
    inline bool operator>=(const TTypedParam& lhs, const TLiteral& rhs) {
        return !operator<(lhs, rhs);
    }

    // Struct that holds and handles all parameter values of different supported types
    struct TParams {
        TParam Param[LWTRACE_MAX_PARAMS];
    };

    using TSerializedParams = google::protobuf::RepeatedPtrField<NLWTrace::TTraceParam>;

    // Represents a common class for all function "signatures" (parameter types and names).
    // Provides non-virtual interface to handle the signature and (emulated) virtual interface to handle TParams corresponding to the signature
    struct TSignature {
        const char** ParamTypes;
        const char* ParamNames[LWTRACE_MAX_PARAMS + 1];
        size_t ParamCount;

        // Virtual table
        void (*SerializeParamsFunc)(const TParams& params, TString* values);
        void (*CloneParamsFunc)(TParams& newParams, const TParams& oldParams);
        void (*DestroyParamsFunc)(TParams& params);
        void (*SerializeToPbFunc)(const TParams& params, TSerializedParams& arr);
        bool (*DeserializeFromPbFunc)(TParams& params, const TSerializedParams& arr);

        // Virtual calls emulation
        void SerializeParams(const TParams& params, TString* values) const {
            (*SerializeParamsFunc)(params, values);
        }

        void CloneParams(TParams& newParams, const TParams& oldParams) const {
            (*CloneParamsFunc)(newParams, oldParams);
        }

        void DestroyParams(TParams& params) const {
            (*DestroyParamsFunc)(params);
        }

        void SerializeToPb(const TParams& params, TSerializedParams& arr) const
        {
            (*SerializeToPbFunc)(params, arr);
        }

        bool DeserializeFromPb(TParams& params, const TSerializedParams& arr) const
        {
            return (*DeserializeFromPbFunc)(params, arr);
        }

        void ToProtobuf(TEventPb& pb) const;

        size_t FindParamIndex(const TString& param) const {
            for (size_t i = 0; i < ParamCount; i++) {
                if (ParamNames[i] == param) {
                    return i;
                }
            }
            return size_t(-1);
        }
    };

#ifndef LWTRACE_DISABLE

    // Implementation. Used for compilation error if not all expected parameters passed to a function call
    struct ERROR_not_enough_parameters : TNil {};

    // Struct that holds static string with a name of parameter type
    template <class T>
    struct TParamType {
        enum { Supported = 0 };
        static const char* NameString;
    };

#define FOREACH_PARAMTYPE_MACRO(n, t, v) \
    template <>                          \
    struct TParamType<t> {               \
        enum { Supported = 1 };          \
        static const char* NameString;   \
    };                                   \
    /**/
    FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
    FOR_NIL_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO

    template <class T>
    struct TParamTraits;

    // Enum types traits impl.
    template <class TEnum, class = std::enable_if_t<std::is_enum_v<TEnum>>>
    struct TEnumParamTraitsImpl {
        using TStoreType = typename TParamTraits<std::underlying_type_t<TEnum>>::TStoreType;
        using TFuncParam = TEnum;

        inline static void ToString(typename TTypeTraits<TStoreType>::TFuncParam stored, TString* out) {
            if constexpr (google::protobuf::is_proto_enum<TEnum>::value) {
                const google::protobuf::EnumValueDescriptor* valueDescriptor = google::protobuf::GetEnumDescriptor<TEnum>()->FindValueByNumber(stored);
                if (valueDescriptor) {
                    *out = TStringBuilder() << valueDescriptor->name() << " (" << stored << ")";
                } else {
                    *out = TParamConv<TStoreType>::ToString(stored);
                }
            } else {
                *out = TParamConv<TStoreType>::ToString(stored);
            }
        }

        inline static TStoreType ToStoreType(TFuncParam v) {
            return static_cast<TStoreType>(v);
        }
    };

    template <class TCustomType>
    struct TCustomTraitsImpl {
        using TStoreType = typename TParamTraits<typename TCustomType::TStoreType>::TStoreType; //see STORE_TYPE_AS
        using TFuncParam = typename TCustomType::TFuncParam;

        inline static void ToString(typename TTypeTraits<TStoreType>::TFuncParam stored, TString* out) {
            TCustomType::ToString(stored, out);
        }

        inline static TStoreType ToStoreType(TFuncParam v) {
            return TCustomType::ToStoreType(v);
        }
    };

    template <class T, bool isEnum>
    struct TParamTraitsImpl;

    template <class TEnum>
    struct TParamTraitsImpl<TEnum, true> : TEnumParamTraitsImpl<TEnum> {
    };

    template <class TCustomType>
    struct TParamTraitsImpl<TCustomType, false> : TCustomTraitsImpl<TCustomType> {
    };

    template <class T>
    struct TParamTraits : TParamTraitsImpl<T, std::is_enum_v<T>> {
    };

    // Standard stored types traits.

#define STORE_TYPE_AS(input_t, store_as_t)                                                      \
    template <>                                                                                 \
    struct TParamTraits<input_t> {                                                              \
        using TStoreType = store_as_t;                                                          \
        using TFuncParam = typename TTypeTraits<input_t>::TFuncParam;                           \
                                                                                                \
        inline static void ToString(typename TTypeTraits<TStoreType>::TFuncParam stored, TString* out) { \
            *out = TParamConv<TStoreType>::ToString(stored);                                    \
        }                                                                                       \
                                                                                                \
        inline static TStoreType ToStoreType(TFuncParam v) {                                    \
            return v;                                                                           \
        }                                                                                       \
    };                                                                                          \
    /**/
    STORE_TYPE_AS(ui8, ui64);
    STORE_TYPE_AS(i8, i64);
    STORE_TYPE_AS(ui16, ui64);
    STORE_TYPE_AS(i16, i64);
    STORE_TYPE_AS(ui32, ui64);
    STORE_TYPE_AS(i32, i64);
    STORE_TYPE_AS(bool, ui64);
    STORE_TYPE_AS(float, double);
#define FOREACH_PARAMTYPE_MACRO(n, t, v) STORE_TYPE_AS(t, t)
    FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef STORE_TYPE_AS
#undef FOREACH_PARAMTYPE_MACRO

    // Nil type staits.
    template <>
    struct TParamTraits<TNil> {
        using TStoreType = TNil;
        using TFuncParam = TTypeTraits<TNil>::TFuncParam;

        inline static void ToString(typename TTypeTraits<TNil>::TFuncParam, TString*) {
        }

        inline static TNil ToStoreType(TFuncParam v) {
            return v;
        }
    };

    inline EParamTypePb ParamTypeToProtobuf(const char* paramType) {
#define FOREACH_PARAMTYPE_MACRO(n, t, v) \
    if (strcmp(paramType, n) == 0) {     \
        return PT_##v;                   \
    }                                    \
    /**/
        FOREACH_PARAMTYPE(FOREACH_PARAMTYPE_MACRO)
#undef FOREACH_PARAMTYPE_MACRO
        return PT_UNKNOWN;
    }

    template <typename T>
    inline void SaveParamToPb(TSerializedParams& msg, const TParam& param);

    template <>
    inline void SaveParamToPb<TNil>(TSerializedParams& msg, const TParam& param)
    {
        Y_UNUSED(msg);
        Y_UNUSED(param);
    }

    template <>
    inline void SaveParamToPb<i64>(TSerializedParams& msg, const TParam& param)
    {
        msg.Add()->SetIntValue(param.Get<typename TParamTraits<i64>::TStoreType>());
    }

    template <>
    inline void SaveParamToPb<ui64>(TSerializedParams& msg, const TParam& param)
    {
        msg.Add()->SetUintValue(param.Get<typename TParamTraits<ui64>::TStoreType>());
    }

    template <>
    inline void SaveParamToPb<double>(TSerializedParams& msg, const TParam& param)
    {
        msg.Add()->SetDoubleValue(param.Get<typename TParamTraits<double>::TStoreType>());
    }

    template <>
    inline void SaveParamToPb<TString>(TSerializedParams& msg, const TParam& param)
    {
        msg.Add()->SetStrValue(param.Get<typename TParamTraits<TString>::TStoreType>());
    }

    template <>
    inline void SaveParamToPb<TSymbol>(TSerializedParams& msg, const TParam& param)
    {
        msg.Add()->SetStrValue(*param.Get<typename TParamTraits<TSymbol>::TStoreType>().Str);
    }

    template <>
    inline void SaveParamToPb<TCheck>(TSerializedParams& msg, const TParam& param)
    {
        msg.Add()->SetIntValue(param.Get<typename TParamTraits<TCheck>::TStoreType>().Value);
    }

    template <typename T>
    inline void LoadParamFromPb(const TTraceParam& msg, TParam& param);

    template <>
    inline void LoadParamFromPb<i64>(const TTraceParam& msg, TParam& param)
    {
        param.DefaultConstruct<i64>();
        param.Get<i64>() = msg.GetIntValue();
    }

    template <>
    inline void LoadParamFromPb<ui64>(const TTraceParam& msg, TParam& param)
    {
        param.DefaultConstruct<ui64>();
        param.Get<ui64>() = msg.GetUintValue();
    }

    template <>
    inline void LoadParamFromPb<double>(const TTraceParam& msg, TParam& param)
    {
        param.DefaultConstruct<double>();
        param.Get<double>() = msg.GetDoubleValue();
    }

    template <>
    inline void LoadParamFromPb<TCheck>(const TTraceParam& msg, TParam& param)
    {
        param.CopyConstruct<TCheck>(TCheck(msg.GetIntValue()));
    }

    template <>
    inline void LoadParamFromPb<TSymbol>(const TTraceParam& msg, TParam& param)
    {
        Y_UNUSED(msg);
        Y_UNUSED(param);
        static TString unsupported("unsupported");
        // so far TSymbol deserialization is not supported
        // since it is not used for probes, it is ok
        param.CopyConstruct<TSymbol>(TSymbol(&unsupported));
    }

    template <>
    inline void LoadParamFromPb<TString>(const TTraceParam& msg, TParam& param)
    {
        param.DefaultConstruct<TString>();
        param.Get<TString>() = msg.GetStrValue();
    }

    template <>
    inline void LoadParamFromPb<TNil>(const TTraceParam& msg, TParam& param)
    {
        Y_UNUSED(msg);
        Y_UNUSED(param);
    }

    // Class representing a specific signature
    template <LWTRACE_TEMPLATE_PARAMS>
    struct TUserSignature {
#define FOREACH_PARAMNUM_MACRO(i) static_assert(TParamType<typename TParamTraits<TP##i>::TStoreType>::Supported == 1, "expect TParamType< typename TParamTraits<TP ## i>::TStoreType >::Supported == 1");
        FOREACH_PARAMNUM(FOREACH_PARAMNUM_MACRO) // ERROR: unsupported type used as probe/event parameter type
#undef FOREACH_PARAMNUM_MACRO
        static const char* ParamTypes[];
        static const int ParamCount = LWTRACE_COUNT_PARAMS;

        // Implementation of virtual function (TSignature derived classes vtable emulation)
        inline static void SerializeParams(const TParams& params, TString* values) {
#define FOREACH_PARAMNUM_MACRO(i) TParamTraits<TP##i>::ToString(params.Param[i].Get<typename TParamTraits<TP##i>::TStoreType>(), values + i);
            FOREACH_PARAMNUM(FOREACH_PARAMNUM_MACRO);
#undef FOREACH_PARAMNUM_MACRO
        }

        // Implementation of virtual function (TSignature derived classes vtable emulation)
        inline static void CloneParams(TParams& newParams, const TParams& oldParams) {
#define FOREACH_PARAMNUM_MACRO(i) newParams.Param[i].CopyConstruct<typename TParamTraits<TP##i>::TStoreType>(oldParams.Param[i].Get<typename TParamTraits<TP##i>::TStoreType>());
            FOREACH_PARAMNUM(FOREACH_PARAMNUM_MACRO);
#undef FOREACH_PARAMNUM_MACRO
        }

        // Implementation of virtual function (TSignature derived classes vtable emulation)
        inline static void DestroyParams(TParams& params) {
#define FOREACH_PARAMNUM_MACRO(i) params.Param[i].Destruct<typename TParamTraits<TP##i>::TStoreType>();
            FOREACH_PARAMNUM(FOREACH_PARAMNUM_MACRO);
#undef FOREACH_PARAMNUM_MACRO
        }

        // Implementation of virtual function (TSignature derived classes vtable emulation)
        inline static void SerializeToPb(const TParams& params, TSerializedParams& arr)
        {
#define FOREACH_PARAMNUM_MACRO(i)                                              \
    SaveParamToPb<typename TParamTraits<TP##i>::TStoreType>(                   \
        arr,                                                                   \
        params.Param[i]);                                                      \
// FOREACH_PARAMNUM_MACRO
            FOREACH_PARAMNUM(FOREACH_PARAMNUM_MACRO);
#undef FOREACH_PARAMNUM_MACRO
        }

        // Implementation of virtual function (TSignature derived classes vtable emulation)
        inline static bool DeserializeFromPb(TParams& params, const TSerializedParams& arr) {
            if (arr.size() != ParamCount) {
                return false;
            }
            if (!ParamCount) {
                return true;
            }

            int paramIdx = 0;
#define FOREACH_PARAMNUM_MACRO(i)                                              \
    if (paramIdx >= arr.size()) {                                              \
        return true;                                                           \
    };                                                                         \
    LoadParamFromPb<typename TParamTraits<TP##i>::TStoreType>(                 \
        arr.Get(paramIdx),                                                     \
        params.Param[paramIdx]);                                               \
    ++paramIdx;                                                                \
//  FOREACH_PARAMNUM_MACRO
            FOREACH_PARAMNUM(FOREACH_PARAMNUM_MACRO);
#undef FOREACH_PARAMNUM_MACRO
            return true;
        }
    };

    // Array of static strings pointers for names of parameter types in a specific signature
    template <LWTRACE_TEMPLATE_PARAMS_NODEF>
    const char* TUserSignature<LWTRACE_TEMPLATE_ARGS>::ParamTypes[] = {
#define FOREACH_PARAMNUM_MACRO(i) TParamType<typename TParamTraits<TP##i>::TStoreType>::NameString,
        FOREACH_PARAMNUM(FOREACH_PARAMNUM_MACRO) nullptr
#undef FOREACH_PARAMNUM_MACRO
    };

    inline void TSignature::ToProtobuf(TEventPb& pb) const {
        for (size_t i = 0; i < ParamCount; i++) {
            pb.AddParamTypes(ParamTypeToProtobuf(ParamTypes[i]));
            pb.AddParamNames(ParamNames[i]);
        }
    }

#else

    inline void TSignature::ToProtobuf(TEventPb&) const {
    }

    inline EParamTypePb ParamTypeToProtobuf(const char*) {
        return PT_UNKNOWN;
    }

#endif

}
