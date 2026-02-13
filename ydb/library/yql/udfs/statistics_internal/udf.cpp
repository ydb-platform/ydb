#include <ydb/library/yql/udfs/statistics_internal/all_agg_funcs.h>
#include <yql/essentials/public/udf/udf_helpers.h>
#include <util/string/cast.h>

namespace NKikimr::NStat::NAggFuncs {

extern const char RESOURCE_NAME[] = "StatisticsInternal.StatisticState";

template<typename TAggFunc>
using TResource = NYql::NUdf::TBoxedResource<typename TAggFunc::TState, RESOURCE_NAME>;

template<typename TAggFunc>
class TCreateFunc : public NYql::NUdf::TBoxedValue {
    TTypeId ColumnTypeId;
    decltype(TAggFunc::CreateStateUpdater(ColumnTypeId)) Updater;
public:
    explicit TCreateFunc(NYql::NUdf::TDataTypeId typeId)
        : ColumnTypeId(typeId)
        , Updater(TAggFunc::CreateStateUpdater(typeId))
    {}

    static NYql::NUdf::TStringRef Name() {
        static const TString name = TString(TAggFunc::GetName()) + "Create";
        return name;
    }

    static bool DeclareSignature(
            const NYql::NUdf::TStringRef& name,
            NYql::NUdf::TType* userType,
            NYql::NUdf::IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
        if (name != Name()) {
            return false;
        }

        if (!userType) {
            builder.SetError("User type is not specified");
            return true;
        }
        builder.UserType(userType);

        auto typeHelper = builder.TypeInfoHelper();
        auto userTypeInspector = NYql::NUdf::TTupleTypeInspector(*typeHelper, userType);
        if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
            builder.SetError("Invalid user type");
            return true;
        }

        auto argsTypeInspector = NYql::NUdf::TTupleTypeInspector(
            *typeHelper, userTypeInspector.GetElementType(0));
        if (!argsTypeInspector
            || argsTypeInspector.GetElementsCount() != TAggFunc::ParamsCount + 1) {
            builder.SetError("Missing or invalid user type arguments");
            return true;
        }

        auto argType = argsTypeInspector.GetElementType(0);
        NYql::NUdf::TDataTypeInspector argInspector(*typeHelper, argType);
        if (!argInspector) {
            builder.SetError("Invalid argument type");
            return true;
        }
        builder.Args()->Add(argType);

        for (size_t i = 0; i < TAggFunc::ParamsCount; ++i) {
            builder.Args()->Add(argsTypeInspector.GetElementType(i + 1));
        }

        builder.Returns<NYql::NUdf::TResource<RESOURCE_NAME>>();

        if (!typesOnly) {
            builder.Implementation(new TCreateFunc<TAggFunc>(argInspector.GetTypeId()));
        }
        return true;
    }

private:
    NYql::NUdf::TUnboxedValue
    Run(const NYql::NUdf::IValueBuilder*, const NYql::NUdf::TUnboxedValuePod* args) const override {
        std::span<const TValue, TAggFunc::ParamsCount> params(
            args + 1, TAggFunc::ParamsCount);
        auto state = TAggFunc::CreateState(ColumnTypeId, params);
        Updater(state, args[0]);
        return NYql::NUdf::TUnboxedValuePod(new TResource<TAggFunc>(std::move(state)));
    }
};

template<typename TAggFunc>
class TAddValueFunc : public NYql::NUdf::TBoxedValue {
    TTypeId ColumnTypeId;
    decltype(TAggFunc::CreateStateUpdater(ColumnTypeId)) Updater;
public:
    explicit TAddValueFunc(NYql::NUdf::TDataTypeId typeId)
        : ColumnTypeId(typeId)
        , Updater(TAggFunc::CreateStateUpdater(typeId))
    {}

    static NYql::NUdf::TStringRef Name() {
        static const TString name = TString(TAggFunc::GetName()) + "AddValue";
        return name;
    }

    static bool DeclareSignature(
            const NYql::NUdf::TStringRef& name,
            NYql::NUdf::TType* userType,
            NYql::NUdf::IFunctionTypeInfoBuilder& builder,
            bool typesOnly) {
        if (name != Name()) {
            return false;
        }

        if (!userType) {
            builder.SetError("User type is not specified");
            return true;
        }
        builder.UserType(userType);

        auto typeHelper = builder.TypeInfoHelper();
        auto userTypeInspector = NYql::NUdf::TTupleTypeInspector(*typeHelper, userType);
        if (!userTypeInspector || userTypeInspector.GetElementsCount() < 1) {
            builder.SetError("Invalid user type");
            return true;
        }

        auto argsTypeInspector = NYql::NUdf::TTupleTypeInspector(
            *typeHelper, userTypeInspector.GetElementType(0));
        if (!argsTypeInspector || argsTypeInspector.GetElementsCount() != 2) {
            builder.SetError("Missing or invalid user type arguments");
            return true;
        }

        auto arg0Type = argsTypeInspector.GetElementType(0);
        NYql::NUdf::TResourceTypeInspector arg0Inspector(*typeHelper, arg0Type);
        if (!arg0Inspector || arg0Inspector.GetTag() != RESOURCE_NAME) {
            builder.SetError("Invalid argument 0 type");
            return true;
        }
        builder.Args()->Add(arg0Type);

        auto arg1Type = argsTypeInspector.GetElementType(1);
        NYql::NUdf::TDataTypeInspector arg1Inspector(*typeHelper, arg1Type);
        if (!arg1Inspector) {
            builder.SetError("Invalid argument 1 type");
            return true;
        }
        builder.Args()->Add(arg1Type);

        builder.Returns<NYql::NUdf::TResource<RESOURCE_NAME>>();

        if (!typesOnly) {
            builder.Implementation(new TAddValueFunc(arg1Inspector.GetTypeId()));
        }
        return true;
    }

private:
    NYql::NUdf::TUnboxedValue Run(
            const NYql::NUdf::IValueBuilder*, const NYql::NUdf::TUnboxedValuePod* args) const override {
        auto& state = *static_cast<TResource<TAggFunc>*>(
            args[0].AsBoxed().Get())->Get();
        Updater(state, args[1]);
        return NYql::NUdf::TUnboxedValuePod(args[0]);
    }
};

template<typename TSignature, typename TDerived>
class TSimpleUdfHelper : public NYql::NUdf::TBoxedValue {
protected:
    NYql::NUdf::TSourcePosition Pos_;
public:
    explicit TSimpleUdfHelper(
        NYql::NUdf::IFunctionTypeInfoBuilder& builder)
        : Pos_(GetSourcePosition(builder))
    {}

    NYql::NUdf::TUnboxedValue Run(
        const NYql::NUdf::IValueBuilder* valueBuilder,
        const NYql::NUdf::TUnboxedValuePod* args) const override {
        try {
            return TDerived::RunImpl(valueBuilder, args);
        } catch (const std::exception& ex) {
            TStringBuilder sb;
            APPEND_SOURCE_LOCATION(sb, valueBuilder, Pos_)
            sb << ex.what();
            UdfTerminate(sb.c_str());
        }
    }

    static bool DeclareSignature(
        const NYql::NUdf::TStringRef& name,
        NYql::NUdf::TType* userType,
        NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly) {
        Y_UNUSED(userType);
        if (TDerived::Name() == name) {
            builder.SimpleSignature<TSignature>();
            if (!typesOnly) {
                builder.Implementation(new TDerived(builder));
            }
            return true;
        }
        return false;
    }
};

template<typename TAggFunc>
class TSerializeFunc : public
    TSimpleUdfHelper<
        char*(NYql::NUdf::TResource<RESOURCE_NAME>),
        TSerializeFunc<TAggFunc>> {
public:
    using TSimpleUdfHelper<
        char*(NYql::NUdf::TResource<RESOURCE_NAME>),
        TSerializeFunc<TAggFunc>>::TSimpleUdfHelper;

    static NYql::NUdf::TStringRef Name() {
        static const TString name = TString(TAggFunc::GetName()) + "Serialize";
        return name;
    }

    static NYql::NUdf::TUnboxedValue RunImpl(
        const NYql::NUdf::IValueBuilder* valueBuilder,
        const NYql::NUdf::TUnboxedValuePod* args) {
        const auto& state = *static_cast<TResource<TAggFunc>*>(
            args[0].AsBoxed().Get())->Get();
        const auto str = TAggFunc::SerializeState(state);
        return valueBuilder->NewString(str);
    }
};

template<typename TAggFunc>
class TDeserializeFunc : public
    TSimpleUdfHelper<
        NYql::NUdf::TResource<RESOURCE_NAME>(char*),
        TDeserializeFunc<TAggFunc>> {
public:
    using TBase = TSimpleUdfHelper<
        NYql::NUdf::TResource<RESOURCE_NAME>(char*),
        TDeserializeFunc<TAggFunc>>;

    using TBase::TBase;

    static NYql::NUdf::TStringRef Name() {
        static const TString name = TString(TAggFunc::GetName()) + "Deserialize";
        return name;
    }

    static NYql::NUdf::TUnboxedValue RunImpl(
        const NYql::NUdf::IValueBuilder*,
        const NYql::NUdf::TUnboxedValuePod* args) {
        auto str = args[0].AsStringRef();
        auto state = TAggFunc::DeserializeState(str.Data(), str.Size());
        return NYql::NUdf::TUnboxedValuePod(
            new TResource<TAggFunc>(std::move(state)));
    }
};

template<typename TAggFunc>
class TMergeFunc : public
    TSimpleUdfHelper<
        NYql::NUdf::TResource<RESOURCE_NAME>(
            NYql::NUdf::TResource<RESOURCE_NAME>,
            NYql::NUdf::TResource<RESOURCE_NAME>),
        TMergeFunc<TAggFunc>> {
public:
    using TBase = TSimpleUdfHelper<
        NYql::NUdf::TResource<RESOURCE_NAME>(
            NYql::NUdf::TResource<RESOURCE_NAME>,
            NYql::NUdf::TResource<RESOURCE_NAME>),
        TMergeFunc<TAggFunc>>;

    using TBase::TBase;

    static NYql::NUdf::TStringRef Name() {
        static const TString name = TString(TAggFunc::GetName()) + "Merge";
        return name;
    }

    static NYql::NUdf::TUnboxedValue RunImpl(
        const NYql::NUdf::IValueBuilder*,
        const NYql::NUdf::TUnboxedValuePod* args) {
        const auto& left = *static_cast<TResource<TAggFunc>*>(
            args[0].AsBoxed().Get())->Get();
        auto& right = *static_cast<TResource<TAggFunc>*>(
            args[1].AsBoxed().Get())->Get();
        TAggFunc::MergeStates(left, right);
        return NYql::NUdf::TUnboxedValuePod(args[1]);
    }
};

template<typename TAggFunc>
class TFinalizeFunc : public
    TSimpleUdfHelper<
        char*(NYql::NUdf::TResource<RESOURCE_NAME>),
        TFinalizeFunc<TAggFunc>> {
public:
    using TBase = TSimpleUdfHelper<
        char*(NYql::NUdf::TResource<RESOURCE_NAME>), TFinalizeFunc<TAggFunc>>;
    using TBase::TBase;

    static NYql::NUdf::TStringRef Name() {
        static const TString name = TString(TAggFunc::GetName()) + "Finalize";
        return name;
    }

    static NYql::NUdf::TUnboxedValue RunImpl(
        const NYql::NUdf::IValueBuilder* valueBuilder,
        const NYql::NUdf::TUnboxedValuePod* args) {
        const auto& state = *static_cast<TResource<TAggFunc>*>(
            args[0].AsBoxed().Get())->Get();
        auto finalized = TAggFunc::FinalizeState(state);
        return valueBuilder->NewString(
            NYql::NUdf::TStringRef(finalized.data(), finalized.size()));
    }
};

template<typename T> class TStatisticsInternalModuleImpl;

template<typename... TAggFuncList>
class TStatisticsInternalModuleImpl<TTypeList<TAggFuncList...>>
    : public NYql::NUdf::IUdfModule {

    template<typename TAggFunc>
    void GetAllFunctionsImpl(NYql::NUdf::IFunctionsSink& names) const {
        names.Add(TCreateFunc<TAggFunc>::Name())->SetTypeAwareness();
        names.Add(TAddValueFunc<TAggFunc>::Name())->SetTypeAwareness();
        names.Add(TMergeFunc<TAggFunc>::Name());
        names.Add(TFinalizeFunc<TAggFunc>::Name());
        names.Add(TSerializeFunc<TAggFunc>::Name());
        names.Add(TDeserializeFunc<TAggFunc>::Name());
    }


    template<typename TAggFunc>
    bool BuildFunctionTypeInfoImpl(
        const NYql::NUdf::TStringRef& name,
        NYql::NUdf::TType* userType,
        const NYql::NUdf::TStringRef& typeConfig,
        ui32 flags,
        NYql::NUdf::IFunctionTypeInfoBuilder& builder) const {
        Y_UNUSED(typeConfig);
        const bool typesOnly = (flags & TFlags::TypesOnly);
        const bool found =
            TCreateFunc<TAggFunc>::DeclareSignature(name, userType, builder, typesOnly)
            || TAddValueFunc<TAggFunc>::DeclareSignature(name, userType, builder, typesOnly)
            || TMergeFunc<TAggFunc>::DeclareSignature(name, userType, builder, typesOnly)
            || TFinalizeFunc<TAggFunc>::DeclareSignature(name, userType, builder, typesOnly)
            || TSerializeFunc<TAggFunc>::DeclareSignature(name, userType, builder, typesOnly)
            || TDeserializeFunc<TAggFunc>::DeclareSignature(name, userType, builder, typesOnly);
        return found;
    }

public:
    static const NYql::NUdf::TStringRef& Name() {
        static auto name = NYql::NUdf::TStringRef("StatisticsInternal");
        return name;
    }

    void CleanupOnTerminate() const final {}

    void GetAllFunctions(NYql::NUdf::IFunctionsSink& sink) const final {
        (GetAllFunctionsImpl<TAggFuncList>(sink), ...);
    }

    void BuildFunctionTypeInfo(
        const NYql::NUdf::TStringRef& name,
        NYql::NUdf::TType* userType,
        const NYql::NUdf::TStringRef& typeConfig,
        ui32 flags,
        NYql::NUdf::IFunctionTypeInfoBuilder& builder) const override {
        try {
            bool found = (BuildFunctionTypeInfoImpl<TAggFuncList>(
                name, userType, typeConfig, flags, builder) || ...);
            if (!found) {
                TStringBuilder sb;
                sb << "Unknown function: " << TStringBuf(name);
                builder.SetError(sb);
            }
        } catch (const std::exception&) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

using TStatisticsInternalModule = TStatisticsInternalModuleImpl<TAllAggFuncsList>;

REGISTER_MODULES(TStatisticsInternalModule)

} // NKikimr::NStat::NAggFuncs
