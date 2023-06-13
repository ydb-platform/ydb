#pragma once

#include "udf_value.h"
#include "udf_registrator.h"
#include "udf_value_builder.h"
#include "udf_terminator.h"
#include "udf_type_builder.h"
#include "udf_type_inspection.h"
#include "udf_version.h"

#include <util/generic/yexception.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/string/builder.h>


namespace NYql {
namespace NUdf {

    inline TSourcePosition GetSourcePosition(IFunctionTypeInfoBuilder& builder) {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 9)
        return builder.GetSourcePosition();
#else
        Y_UNUSED(builder);
        return {};
#endif
    }

    TString LoadResourceOnce(TStringBuf resourceId);

    inline void SetIRImplementation(IFunctionTypeInfoBuilder& builder, TStringBuf resourceId, TStringBuf functionName) {
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 16)
        if (functionName) {
            builder.IRImplementation(LoadResourceOnce(resourceId), resourceId, functionName);
        }
#else
        Y_UNUSED(builder);
        Y_UNUSED(resourceId);
        Y_UNUSED(functionName);
#endif
    }
}
}

#define UDF_IMPL_EX(udfName, typeBody, members, init, irResourceId, irFunctionName, blockType, create_impl) \
    class udfName: public ::NYql::NUdf::TBoxedValue {                                    \
    public:                                                                              \
        using TBlockType = blockType;                                                    \
        explicit udfName(::NYql::NUdf::IFunctionTypeInfoBuilder& builder)                \
            : Pos_(GetSourcePosition(builder))                                              \
        {                                                                                   \
            init                                                                            \
        }                                                                                   \
        static const ::NYql::NUdf::TStringRef& Name() {                                  \
            static auto name = ::NYql::NUdf::TStringRef::Of(#udfName).Substring(1, 256); \
            return name;                                                                    \
        }                                                                                   \
        inline ::NYql::NUdf::TUnboxedValue RunImpl(                                      \
            const ::NYql::NUdf::IValueBuilder* valueBuilder,                             \
            const ::NYql::NUdf::TUnboxedValuePod* args) const;                           \
        ::NYql::NUdf::TUnboxedValue Run(                                                 \
            const ::NYql::NUdf::IValueBuilder* valueBuilder,                             \
            const ::NYql::NUdf::TUnboxedValuePod* args) const override {                 \
            try {                                                                           \
                return RunImpl(valueBuilder, args);                                         \
            } catch (const std::exception&) {                                               \
                    TStringBuilder sb;                                                      \
                    sb << Pos_ << " ";                                                      \
                    sb << CurrentExceptionMessage();                                        \
                    sb << Endl << "[" << TStringBuf(Name()) << "]" ;                        \
                    UdfTerminate(sb.c_str());                                               \
            }                                                                               \
        }                                                                                   \
        static bool DeclareSignature(                                                       \
            const ::NYql::NUdf::TStringRef& name,                                        \
            ::NYql::NUdf::TType* userType,                                               \
            ::NYql::NUdf::IFunctionTypeInfoBuilder& builder,                             \
            bool typesOnly) {                                                               \
            Y_UNUSED(userType);                                                             \
            if (Name() == name) {                                                           \
                typeBody if (!typesOnly) {                                                  \
                    create_impl                                                             \
                    SetIRImplementation(builder, irResourceId, irFunctionName);             \
                }                                                                           \
                return true;                                                                \
            }                                                                               \
            return false;                                                                   \
        }                                                                                   \
    private:                                                                                \
        ::NYql::NUdf::TSourcePosition Pos_;                                              \
        members                                                                             \
    };                                                                                      \
    ::NYql::NUdf::TUnboxedValue udfName::RunImpl(                                        \
        const ::NYql::NUdf::IValueBuilder* valueBuilder,                                 \
        const ::NYql::NUdf::TUnboxedValuePod* args) const

#define UDF_IMPL(udfName, typeBody, members, init, irResourceId, irFunctionName, blockType) \
        UDF_IMPL_EX(udfName, typeBody, members, init, irResourceId, irFunctionName, blockType, builder.Implementation(new udfName(builder));)

#define UDF(udfName, typeBody) UDF_IMPL(udfName, typeBody, ;, ;, "", "", void)

#define UDF_RUN_IMPL(udfName, typeBody, members, init, irResourceId, irFunctionName)        \
    struct udfName##Members {                                                               \
        members                                                                             \
    };                                                                                      \
                                                                                            \
    class udfName: public ::NYql::NUdf::TBoxedValue, public udfName##Members {           \
    public:                                                                                 \
        explicit udfName(::NYql::NUdf::IFunctionTypeInfoBuilder& builder)                \
            : Pos_(GetSourcePosition(builder))                                              \
        {                                                                                   \
            init                                                                            \
        }                                                                                   \
        static const ::NYql::NUdf::TStringRef& Name() {                                  \
            static auto name = ::NYql::NUdf::TStringRef::Of(#udfName).Substring(1, 256); \
            return name;                                                                    \
        }                                                                                   \
        class TImpl: public TBoxedValue, public udfName##Members {                          \
        public:                                                                             \
            TImpl(const udfName##Members& parent,                                           \
                const ::NYql::NUdf::TUnboxedValuePod& runConfig,                         \
                ::NYql::NUdf::TSourcePosition pos)                                       \
                : udfName##Members(parent)                                                  \
                , RunConfig(::NYql::NUdf::TUnboxedValuePod(runConfig))                   \
                , Pos_(pos)                                                                 \
            {}                                                                              \
            inline ::NYql::NUdf::TUnboxedValue RunImpl(                                  \
                const ::NYql::NUdf::IValueBuilder* valueBuilder,                         \
                const ::NYql::NUdf::TUnboxedValuePod* args) const;                       \
            ::NYql::NUdf::TUnboxedValue Run(                                             \
                const ::NYql::NUdf::IValueBuilder* valueBuilder,                         \
                const ::NYql::NUdf::TUnboxedValuePod* args) const override {             \
                try {                                                                       \
                    return RunImpl(valueBuilder, args);                                     \
                } catch (const std::exception&) {                                           \
                    TStringBuilder sb;                                                      \
                    sb << Pos_ << " ";                                                      \
                    sb << CurrentExceptionMessage();                                        \
                    sb << Endl << "[" << TStringBuf(Name()) << "]" ;                        \
                    UdfTerminate(sb.c_str());                                               \
                }                                                                           \
            }                                                                               \
                                                                                            \
        private:                                                                            \
            ::NYql::NUdf::TUnboxedValue RunConfig;                                       \
            ::NYql::NUdf::TSourcePosition Pos_;                                          \
        };                                                                                  \
        ::NYql::NUdf::TUnboxedValue Run(                                                 \
            const ::NYql::NUdf::IValueBuilder* valueBuilder,                             \
            const ::NYql::NUdf::TUnboxedValuePod* args) const override {                 \
            Y_UNUSED(valueBuilder);                                                         \
            return ::NYql::NUdf::TUnboxedValuePod(new TImpl(*this, args[0], Pos_));      \
        }                                                                                   \
        static bool DeclareSignature(                                                       \
            const ::NYql::NUdf::TStringRef& name,                                        \
            ::NYql::NUdf::TType* userType,                                               \
            ::NYql::NUdf::IFunctionTypeInfoBuilder& builder,                             \
            bool typesOnly) {                                                               \
            Y_UNUSED(userType);                                                             \
            if (Name() == name) {                                                           \
                typeBody if (!typesOnly) {                                                  \
                    builder.Implementation(new udfName(builder));                           \
                    SetIRImplementation(builder, irResourceId, irFunctionName);             \
                }                                                                           \
                return true;                                                                \
            }                                                                               \
            return false;                                                                   \
        }                                                                                   \
    private:                                                                                \
        ::NYql::NUdf::TSourcePosition Pos_;                                              \
    };                                                                                      \
    ::NYql::NUdf::TUnboxedValue udfName::TImpl::RunImpl(                                 \
        const ::NYql::NUdf::IValueBuilder* valueBuilder,                                 \
        const ::NYql::NUdf::TUnboxedValuePod* args) const

#define UDF_RUN(udfName, typeBody) UDF_RUN_IMPL(udfName, typeBody, ;, ;, "", "")

#define SIMPLE_UDF(udfName, signature) \
    UDF(udfName, builder.SimpleSignature<signature>();)

#define SIMPLE_STRICT_UDF(udfName, signature) \
    UDF(udfName, builder.SimpleSignature<signature>().IsStrict();)

#define SIMPLE_UDF_WITH_IR(udfName, signature, irResourceId, irFunctionName) \
    UDF_IMPL(udfName, builder.SimpleSignature<signature>();, ;, ;, irResourceId, irFunctionName, void)

#define SIMPLE_UDF_WITH_CREATE_IMPL(udfName, signature, create_impl) \
    UDF_IMPL_EX(udfName, builder.SimpleSignature<signature>();, ;, ;, "", "", void, create_impl)

#define SIMPLE_UDF_OPTIONS(udfName, signature, options) \
    UDF(udfName, builder.SimpleSignature<signature>(); options;)

#define SIMPLE_STRICT_UDF_OPTIONS(udfName, signature, options) \
    UDF(udfName, builder.SimpleSignature<signature>().IsStrict(); options;)

#define SIMPLE_UDF_RUN_OPTIONS(udfName, signature, options) \
    UDF_RUN(udfName, builder.SimpleSignature<signature>(); options;)

#define SIMPLE_UDF_RUN(udfName, signature, runConfigSignature) \
    SIMPLE_UDF_RUN_OPTIONS(udfName, signature, builder.RunConfig<runConfigSignature>())

#define SIMPLE_MODULE(moduleName, ...)                                               \
    class moduleName: public ::NYql::NUdf::TSimpleUdfModuleHelper<__VA_ARGS__> {  \
    public:                                                                          \
        ::NYql::NUdf::TStringRef Name() const {                                   \
            auto name = ::NYql::NUdf::TStringRef::Of(#moduleName);                \
            return name.Substring(1, name.Size() - 7);                               \
        }                                                                            \
    };

#define EMPTY_RESULT_ON_EMPTY_ARG(n)             \
    if (!args[n]) {                              \
        return ::NYql::NUdf::TUnboxedValue(); \
    }

namespace NYql {
namespace NUdf {

template<bool CheckOptional, const char* TFuncName, template<class> class TFunc, typename... TUserTypes>
class TUserDataTypeFuncFactory : public ::NYql::NUdf::TBoxedValue {
public:
    typedef bool TTypeAwareMarker;

public:
    static const ::NYql::NUdf::TStringRef& Name() {
        static auto name = ::NYql::NUdf::TStringRef(TFuncName, std::strlen(TFuncName));
        return name;
    }

    template<typename TUserType>
    static bool DeclareSignatureImpl(
        TDataTypeId typeId,
        ::NYql::NUdf::TType* userType,
        ::NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (TDataType<TUserType>::Id != typeId) {
            return false;
        }
        TFunc<TUserType>::DeclareSignature(userType, builder, typesOnly);
        return true;
    }

    template<typename TUserType, typename THead, typename... TTail>
    static bool DeclareSignatureImpl(
        TDataTypeId typeId,
        ::NYql::NUdf::TType* userType,
        ::NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (DeclareSignatureImpl<TUserType>(typeId, userType, builder, typesOnly)) {
            return true;
        }
        return DeclareSignatureImpl<THead, TTail...>(typeId, userType, builder, typesOnly);
    }

    static bool DeclareSignature(
        const ::NYql::NUdf::TStringRef& name,
        ::NYql::NUdf::TType* userType,
        ::NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            // the only case when we return false
            return false;
        }

        if (!userType) {
            builder.SetError("User type is not specified");
            return true;
        }

        auto typeHelper = builder.TypeInfoHelper();
        auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
        if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
            builder.SetError("Missing or invalid user type");
            return true;
        }

        auto argsTypeInspector = TTupleTypeInspector(*typeHelper, userTypeInspector.GetElementType(0));
        if (!argsTypeInspector || argsTypeInspector.GetElementsCount() < 1) {
            builder.SetError("Missing or invalid user type arguments");
            return true;
        }

        auto argType = argsTypeInspector.GetElementType(0);
        if (CheckOptional) {
            TOptionalTypeInspector optionalTypeInspector(*typeHelper, argType);
            if (optionalTypeInspector) {
                argType = optionalTypeInspector.GetItemType();
            }
        }

        TDataTypeInspector dataTypeInspector(*typeHelper, argType);
        if (!dataTypeInspector) {
            builder.SetError("User type must be a data type");
            return true;
        }

        builder.UserType(userType);

        auto typeId = dataTypeInspector.GetTypeId();
        if (!DeclareSignatureImpl<TUserTypes...>(typeId, userType, builder, typesOnly)) {
            TStringBuilder sb;
            sb << "User type " << NYql::NUdf::GetDataTypeInfo(NYql::NUdf::GetDataSlot(typeId)).Name << " is not supported";
            builder.SetError(sb);
        }

        return true;
    }
};

template<typename... TUdfs>
class TSimpleUdfModuleHelper : public IUdfModule
{
    Y_HAS_SUBTYPE(TTypeAwareMarker);
    Y_HAS_SUBTYPE(TBlockType);

public:
    void CleanupOnTerminate() const override {
    }

    template<typename TUdfType>
    void GetAllFunctionsImpl(IFunctionNamesSink& names) const {
        auto r = names.Add(TUdfType::Name());
        if (THasTTypeAwareMarker<TUdfType>::value) {
            r->SetTypeAwareness();
        }

        if constexpr (THasTBlockType<TUdfType>::value) {
            if constexpr (!std::is_same_v<typename TUdfType::TBlockType, void>) {
                auto rBlocks = names.Add(TUdfType::TBlockType::Name());
                rBlocks->SetTypeAwareness();
            }
        }
    }

    template<typename THead1, typename THead2, typename... TTail>
    void GetAllFunctionsImpl(IFunctionNamesSink& names) const {
        GetAllFunctionsImpl<THead1>(names);
        GetAllFunctionsImpl<THead2, TTail...>(names);
    }

    template<typename TUdfType>
    bool BuildFunctionTypeInfoImpl(
                const TStringRef& name,
                TType* userType,
                const TStringRef& typeConfig,
                ui32 flags,
                IFunctionTypeInfoBuilder& builder) const
    {
        Y_UNUSED(typeConfig);
        bool typesOnly = (flags & TFlags::TypesOnly);
        bool found = TUdfType::DeclareSignature(name, userType, builder, typesOnly);
        if (!found) {
            if constexpr (THasTBlockType<TUdfType>::value) {
                if constexpr (!std::is_same_v<typename TUdfType::TBlockType, void>) {
                    found = TUdfType::TBlockType::DeclareSignature(name, userType, builder, typesOnly);
                }
            }
        }

        return found;
    }

    template<typename THead1, typename THead2, typename... TTail>
    bool BuildFunctionTypeInfoImpl(
                const TStringRef& name,
                TType* userType,
                const TStringRef& typeConfig,
                ui32 flags,
                IFunctionTypeInfoBuilder& builder) const
    {
        bool found = BuildFunctionTypeInfoImpl<THead1>(name, userType, typeConfig, flags, builder);
        if (!found) {
            found = BuildFunctionTypeInfoImpl<THead2, TTail...>(name, userType, typeConfig, flags, builder);
        }
        return found;
    }

    void GetAllFunctions(IFunctionsSink& sink) const final {
        GetAllFunctionsImpl<TUdfs...>(sink);
    }

    void BuildFunctionTypeInfo(
                        const TStringRef& name,
                        TType* userType,
                        const TStringRef& typeConfig,
                        ui32 flags,
                        IFunctionTypeInfoBuilder& builder) const override
    {
        try {
            bool found = BuildFunctionTypeInfoImpl<TUdfs...>(name, userType, typeConfig, flags, builder);
            if (!found) {
                TStringBuilder sb;
                sb << "Unknown function: " << name.Data();
                builder.SetError(sb);
            }
        } catch (const std::exception&) {
            builder.SetError(CurrentExceptionMessage());
        }
    }

};

} // namspace NUdf
} // namspace NYql
