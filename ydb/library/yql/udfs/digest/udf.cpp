#include "canonicalize.h"

#include <ydb/library/table_digest/aggregate.h>

#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_type_inspection.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace {

using namespace NYql;
using namespace NYql::NDigest;

constexpr char AggregateResourceName[] = "Digest.AggregateState";
using TAggregateResource = NUdf::TBoxedResource<TState, AggregateResourceName>;

TState& GetState(const NUdf::TUnboxedValuePod& value) {
    Y_ENSURE(value.GetResourceTag() == NUdf::TStringRef(AggregateResourceName),
             "Invalid Digest aggregate state resource");
    return *static_cast<TState*>(value.GetResource());
}

NUdf::TUnboxedValue GetWritableState(const NUdf::TUnboxedValuePod& value) {
    const auto& state = GetState(value);
    // Constant initializers can share a resource across aggregate groups.
    if (value.RefCount() == 1) {
        return value;
    }
    return NUdf::TUnboxedValuePod(new TAggregateResource(state));
}

SIMPLE_STRICT_UDF(TAggregateCreate, NUdf::TResource<AggregateResourceName>(char*)) {
    Y_UNUSED(valueBuilder);
    return NUdf::TUnboxedValuePod(new TAggregateResource(ParseAlgorithm(args[0].AsStringRef())));
}

class TAggregateFunction: public NUdf::TBoxedValue {
public:
    explicit TAggregateFunction(NUdf::TSourcePosition pos)
        : Pos_(pos)
    {
    }

private:
    NUdf::TUnboxedValue Run(const NUdf::IValueBuilder* builder,
                            const NUdf::TUnboxedValuePod* args) const final {
        try {
            return RunImpl(builder, args);
        } catch (const std::exception& ex) {
            TStringBuilder message;
            APPEND_SOURCE_LOCATION(message, builder, Pos_)
            message << ex.what();
            UdfTerminate(message.c_str());
        }
    }

    virtual NUdf::TUnboxedValue RunImpl(const NUdf::IValueBuilder* builder,
                                        const NUdf::TUnboxedValuePod* args) const = 0;

    const NUdf::TSourcePosition Pos_;
};

class TCanonicalize: public TAggregateFunction {
public:
    using TTypeAwareMarker = bool;

    TCanonicalize(TCanonicalizePlan plan, NUdf::TSourcePosition pos)
        : TAggregateFunction(pos)
        , Plan_(std::move(plan))
    {
    }

    static const NUdf::TStringRef& Name() {
        static const auto name = NUdf::TStringRef::Of("Canonicalize");
        return name;
    }

    static bool DeclareSignature(const NUdf::TStringRef& name, NUdf::TType* userType,
                                 NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        if (name != Name()) {
            return false;
        }
        Y_ENSURE(userType, "Digest canonicalization requires its input type");
        const auto helper = builder.TypeInfoHelper();
        const NUdf::TTupleTypeInspector user(*helper, userType);
        Y_ENSURE(user && user.GetElementsCount() == 3, "Invalid Digest canonicalization user type");
        const NUdf::TTupleTypeInspector args(*helper, user.GetElementType(0));
        Y_ENSURE(args && args.GetElementsCount() == 1, "Canonicalize expects one argument");
        const auto* inputType = args.GetElementType(0);
        auto plan = TCanonicalizePlan::Build(inputType, builder);
        builder.UserType(userType);
        builder.Args(1)->Add(inputType).Done();
        builder.Returns(plan.GetResultType());
        if (!typesOnly) {
            builder.Implementation(new TCanonicalize(std::move(plan), NUdf::GetSourcePosition(builder)));
        }
        return true;
    }

private:
    NUdf::TUnboxedValue RunImpl(const NUdf::IValueBuilder* builder,
                                const NUdf::TUnboxedValuePod* args) const override {
        return Plan_.Apply(builder, args[0]);
    }

    const TCanonicalizePlan Plan_;
};

class TAggregateAddValue: public TAggregateFunction {
public:
    using TTypeAwareMarker = bool;

    TAggregateAddValue(EAlgorithm algorithm, NUdf::TSourcePosition pos)
        : TAggregateFunction(pos)
        , Algorithm_(algorithm)
    {
    }

    static const NUdf::TStringRef& Name() {
        static const auto name = NUdf::TStringRef::Of("AggregateAddValue");
        return name;
    }

    static bool DeclareSignature(const NUdf::TStringRef& name, NUdf::TType* userType,
                                 NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        if (name != Name()) {
            return false;
        }
        Y_ENSURE(userType, "Digest aggregate requires its hash type");
        const auto helper = builder.TypeInfoHelper();
        NUdf::TTupleTypeInspector user(*helper, userType);
        Y_ENSURE(user && user.GetElementsCount() == 3, "Invalid Digest aggregate user type");
        NUdf::TTupleTypeInspector args(*helper, user.GetElementType(0));
        Y_ENSURE(args && args.GetElementsCount() == 2, "AggregateAddValue expects a state and a hash");
        auto* hashType = args.GetElementType(1);
        const NUdf::TDataTypeInspector data(*helper, hashType);
        const NUdf::TTupleTypeInspector tuple(*helper, hashType);
        EAlgorithm algorithm;
        if (data && data.GetTypeId() == NUdf::TDataType<ui64>::Id) {
            algorithm = EAlgorithm::CityHash64;
        } else if (data && data.GetTypeId() == NUdf::TDataType<char*>::Id) {
            algorithm = EAlgorithm::Sha256;
        } else {
            Y_ENSURE(tuple && tuple.GetElementsCount() == 2,
                     "Digest aggregate expects Uint64, Tuple<Uint64, Uint64>, or String");
            for (ui32 i = 0; i != 2; ++i) {
                const NUdf::TDataTypeInspector word(*helper, tuple.GetElementType(i));
                Y_ENSURE(word && word.GetTypeId() == NUdf::TDataType<ui64>::Id,
                         "Digest aggregate expects Tuple<Uint64, Uint64>");
            }
            algorithm = EAlgorithm::CityHash128;
        }
        builder.UserType(userType);
        builder.Args(2)->Add<NUdf::TResource<AggregateResourceName>>().Add(hashType).Done();
        builder.Returns(builder.Resource(AggregateResourceName));
        builder.IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TAggregateAddValue(algorithm, NUdf::GetSourcePosition(builder)));
        }
        return true;
    }

private:
    NUdf::TUnboxedValue RunImpl(const NUdf::IValueBuilder*,
                                const NUdf::TUnboxedValuePod* args) const override {
        Y_ENSURE(GetState(args[0]).GetAlgorithm() == Algorithm_, "Digest algorithm does not match hash type");
        auto value = GetWritableState(args[0]);
        auto& state = GetState(value);
        switch (Algorithm_) {
            case EAlgorithm::CityHash64:
                state.AddHash(TWords{args[1].Get<ui64>()});
                break;
            case EAlgorithm::CityHash128:
                state.AddHash(TWords{args[1].GetElement(0).Get<ui64>(), args[1].GetElement(1).Get<ui64>()});
                break;
            case EAlgorithm::Sha256:
                state.AddHash(args[1].AsStringRef());
                break;
        }
        return value;
    }

    const EAlgorithm Algorithm_;
};

SIMPLE_STRICT_UDF(TAggregateMerge, NUdf::TResource<AggregateResourceName>(NUdf::TResource<AggregateResourceName>, NUdf::TResource<AggregateResourceName>)) {
    Y_UNUSED(valueBuilder);
    auto value = GetWritableState(args[0]);
    GetState(value).Merge(GetState(args[1]));
    return value;
}

SIMPLE_STRICT_UDF(TAggregateSerialize, char*(NUdf::TResource<AggregateResourceName>)) {
    return valueBuilder->NewString(GetState(args[0]).Serialize());
}

SIMPLE_STRICT_UDF(TAggregateDeserialize, NUdf::TResource<AggregateResourceName>(char*)) {
    Y_UNUSED(valueBuilder);
    return NUdf::TUnboxedValuePod(new TAggregateResource(TState::Deserialize(args[0].AsStringRef())));
}

struct TAggregateResultFields {
    explicit TAggregateResultFields(NUdf::IFunctionTypeInfoBuilder& builder) {
        Type = builder.Struct(4)
                   ->AddField<NUdf::TUtf8>("algorithm", &Algorithm)
                   .AddField<ui64>("count", &Count)
                   .AddField<char*>("hash", &Hash)
                   .AddField<ui32>("version", &Version)
                   .Build();
    }

    NUdf::TType* Type = nullptr;
    ui32 Algorithm = 0;
    ui32 Count = 0;
    ui32 Hash = 0;
    ui32 Version = 0;
};

class TAggregateFinalize: public TAggregateFunction {
public:
    TAggregateFinalize(TAggregateResultFields fields, NUdf::TSourcePosition pos)
        : TAggregateFunction(pos)
        , Fields_(fields)
    {
    }

    static const NUdf::TStringRef& Name() {
        static const auto name = NUdf::TStringRef::Of("AggregateFinalize");
        return name;
    }

    static bool DeclareSignature(const NUdf::TStringRef& name, NUdf::TType*,
                                 NUdf::IFunctionTypeInfoBuilder& builder, bool typesOnly) {
        if (name != Name()) {
            return false;
        }
        const TAggregateResultFields fields(builder);
        builder.Args()->Add<NUdf::TResource<AggregateResourceName>>().Done();
        builder.Returns(fields.Type);
        builder.IsStrict();
        if (!typesOnly) {
            builder.Implementation(new TAggregateFinalize(fields, NUdf::GetSourcePosition(builder)));
        }
        return true;
    }

private:
    NUdf::TUnboxedValue RunImpl(const NUdf::IValueBuilder* builder,
                                const NUdf::TUnboxedValuePod* args) const override {
        const auto& state = GetState(args[0]);
        NUdf::TUnboxedValue* fields = nullptr;
        auto result = builder->NewArray(4, fields);
        fields[Fields_.Algorithm] = builder->NewString(GetAlgorithmName(state.GetAlgorithm()));
        fields[Fields_.Count] = NUdf::TUnboxedValuePod(state.GetCount());
        fields[Fields_.Hash] = builder->NewString(state.GetHashBytes());
        fields[Fields_.Version] = NUdf::TUnboxedValuePod(TState::Version);
        return result;
    }

    const TAggregateResultFields Fields_;
};

SIMPLE_MODULE(TDigestAggregateModule,
              TCanonicalize,
              TAggregateCreate,
              TAggregateAddValue,
              TAggregateMerge,
              TAggregateSerialize,
              TAggregateDeserialize,
              TAggregateFinalize)

} // namespace

REGISTER_MODULES(TDigestAggregateModule)
