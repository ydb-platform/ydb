#include <yql/essentials/udfs/common/reservoir_sampling/lib/reservoir.h>

#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_type_ops.h>
#include <yql/essentials/public/langver/yql_langver.h>

#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/system/env.h>

#include <util/system/mutex.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

static const auto CreateName = TStringRef::Of("Create");
static const auto AddName = TStringRef::Of("Add");
static const auto SerializeName = TStringRef::Of("Serialize");
static const auto DeserializeName = TStringRef::Of("Deserialize");
static const auto MergeName = TStringRef::Of("Merge");
static const auto GetResultValueName = TStringRef::Of("GetResultValue");
static const auto GetResultListName = TStringRef::Of("GetResultList");
static const auto ResourcePrefix = TStringRef::Of("ReservoirSampling_");

using namespace NReservoirSampling;

class TReservoirWrapper: public TReservoir<TUnboxedValue> {
public:
    template <typename... Args>
    TReservoirWrapper(Args&&... args)
        : TReservoir<TUnboxedValue>(std::forward<Args>(args)...)
    {
    }

    TUnboxedValue Serialize(const IValueBuilder* builder) {
        TUnboxedValue* tupleItems;
        auto result = builder->NewArray(3, tupleItems);
        tupleItems[0] = GetResultList(builder);
        tupleItems[1] = TUnboxedValuePod(GetCount());
        tupleItems[2] = TUnboxedValuePod(GetLimit());
        return result;
    }

    TUnboxedValue GetResultList(const IValueBuilder* builder) const {
        auto copy = GetItems();
        auto result = builder->NewList(copy.data(), copy.size());
        return result;
    }

    TUnboxedValue GetResultValue() const {
        Y_DEBUG_ABORT_UNLESS(GetLimit() == 1);
        auto res = GetItems()[0];
        return res;
    }

    bool MergeAndMarkDirty(IRandomProvider& rnd, TReservoirWrapper& rhs) {
        if (Merge(rnd, rhs)) {
            Dirty_ = true;
            return true;
        }
        rhs.Dirty_ = true;
        return false;
    }

    bool IsDirty() const {
        return Dirty_;
    }

private:
    bool Dirty_ = false;
};

using TReservoirSamplingResource = TBoxedDynamicResource<TReservoirWrapper>;

class TThreadSafeRandomProviderWrapper: public IRandomProvider {
public:
    TThreadSafeRandomProviderWrapper(TIntrusivePtr<IRandomProvider>&& underlying)
        : Underlying_(std::move(underlying))
    {
    }

    TGUID GenGuid() noexcept override final {
        TGuard guard(Mutex_);
        return Underlying_->GenGuid();
    }

    TGUID GenUuid4() noexcept override final {
        TGuard guard(Mutex_);
        return Underlying_->GenUuid4();
    }

    ui64 GenRand() noexcept override final {
        TGuard guard(Mutex_);
        return Underlying_->GenRand();
    }

private:
    TIntrusivePtr<IRandomProvider> Underlying_;
    TMutex Mutex_;
};

TIntrusivePtr<IRandomProvider> WrapRandomProviderThreadSafe(TIntrusivePtr<IRandomProvider>&& provider) {
    return MakeIntrusive<TThreadSafeRandomProviderWrapper>(std::move(provider));
}

class TReservoirSamplingBase {
public:
    TReservoirSamplingBase(TString&& tag)
        : RandomPovider_(GetEnv("YQL_DETERMINISTIC_MODE") ? WrapRandomProviderThreadSafe(CreateDeterministicRandomProvider(1)) : CreateDefaultRandomProvider())
        , ResourceTag_(std::move(tag))
    {
    }

    IRandomProvider& RandomPovider() const {
        return RandomPovider_.GetRef();
    }

    TReservoirWrapper& CastResource(const TUnboxedValuePod& v) const {
        auto& res = *CastDynamicResource<TReservoirSamplingResource>(v, ResourceTag_)->Get();
        if (res.IsDirty()) {
            UdfTerminate("ReservoirSampling: Got dirty resource");
        }
        return res;
    }

    template <typename... Args>
    TUnboxedValuePod CreateResource(Args&&... args) const {
        return TUnboxedValuePod(new TReservoirSamplingResource(ResourceTag_, std::forward<Args>(args)...));
    }

    virtual ~TReservoirSamplingBase() {};

private:
    TIntrusivePtr<IRandomProvider> RandomPovider_;
    TString ResourceTag_;
};

template <typename Derived>
class TReservoirSamplingImpl final: public TBoxedValue {
public:
    TReservoirSamplingImpl(TString tag)
        : Implementation_(std::move(tag))
    {
    }

private:
    TUnboxedValue Run(const IValueBuilder* builder, const TUnboxedValuePod* args) const override {
        return Implementation_.Run(builder, args);
    }
    Derived Implementation_;
};

#define METHOD(Name)                                                                         \
    class T##Name: TReservoirSamplingBase {                                                  \
    public:                                                                                  \
        template <typename... Args>                                                          \
        T##Name(Args&&... args)                                                              \
            : TReservoirSamplingBase(std::forward<Args>(args)...)                            \
        {                                                                                    \
        }                                                                                    \
        TUnboxedValue Run(const IValueBuilder* builder, const TUnboxedValuePod* args) const; \
    };                                                                                       \
    TUnboxedValue T##Name::Run(const IValueBuilder* builder, const TUnboxedValuePod* args) const

METHOD(Create) {
    Y_UNUSED(builder);
    return CreateResource(args[0], args[1].Get<ui64>());
}

METHOD(Add) {
    Y_UNUSED(builder);
    CastResource(args[0]).Add(RandomPovider(), args[1]);
    return args[0];
}

METHOD(Merge) {
    Y_UNUSED(builder);
    auto& l = CastResource(args[0]);
    auto& r = CastResource(args[1]);
    if (&l == &r) {
        UdfTerminate("ReservoirSampling: Got same resources as Merge() arguments");
    }
    if (l.MergeAndMarkDirty(RandomPovider(), r)) {
        return args[1];
    }
    return args[0];
}

METHOD(Serialize) {
    return CastResource(args[0]).Serialize(builder);
}

METHOD(Deserialize) {
    Y_UNUSED(builder);
    auto tuple = args[0];
    auto itemsUv = tuple.GetElement(0);
    auto count = tuple.GetElement(1).Get<ui64>();
    auto limit = tuple.GetElement(2).Get<ui64>();
    TVector<TUnboxedValue> items(Reserve(limit));
    for (size_t i = 0; i < itemsUv.GetListLength(); ++i) {
        items.emplace_back(std::move(itemsUv.GetElement(i)));
    }
    return CreateResource(std::move(items), count, limit);
};

METHOD(GetResultList) {
    return CastResource(args[0]).GetResultList(builder);
}

METHOD(GetResultValue) {
    Y_UNUSED(builder);
    return CastResource(args[0]).GetResultValue();
}

bool CheckUint64(ITypeInfoHelper& helper, const TType* type) {
    auto data = TDataTypeInspector(helper, type);
    if (!data) {
        return false;
    }
    auto maybeSlot = FindDataSlot(data.GetTypeId());
    if (!maybeSlot) {
        return false;
    }
    return *maybeSlot == EDataSlot::Uint64;
}

TString BuildResourceTag(ITypeInfoHelper& helper, const TType* type) {
    TTypePrinter printer(helper, type);
    TStringStream tagOut;
    tagOut << ResourcePrefix;
    printer.Out(tagOut);
    return tagOut.Str();
}

TString FormatType(ITypeInfoHelper& helper, const TType* type) {
    TTypePrinter printer(helper, type);
    TStringStream out;
    printer.Out(out);
    return out.Str();
}

class TReservoirSamplingModule: public IUdfModule {
public:
    TStringRef Name() const {
        return TStringRef::Of("ReservoirSampling");
    }

    void CleanupOnTerminate() const final {
    }

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(CreateName)->SetTypeAwareness();
        sink.Add(AddName)->SetTypeAwareness();
        sink.Add(SerializeName)->SetTypeAwareness();
        sink.Add(DeserializeName)->SetTypeAwareness();
        sink.Add(MergeName)->SetTypeAwareness();
        sink.Add(GetResultListName)->SetTypeAwareness();
        sink.Add(GetResultValueName)->SetTypeAwareness();
    }

    // clang-format off
    void BuildFunctionTypeInfo(
        const TStringRef& name,
        TType* userType,
        const TStringRef&,
        ui32 flags,
        IFunctionTypeInfoBuilder& builder) const final
    {
        builder.SetMinLangVer(NYql::MakeLangVersion(2025, 4));
        try {
            bool typesOnly = (flags & TFlags::TypesOnly);
            builder.UserType(userType);

            auto typeHelper = builder.TypeInfoHelper();

            auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() != 3) {
                builder.SetError("User type is not a 3-tuple");
                return;
            }

            auto args = TTupleTypeInspector(*typeHelper, userTypeInspector.GetElementType(0));
            if (!args) {
                builder.SetError("Expected positional args as a tuple");
                return;
            }

            if (name == DeserializeName) {
                if (args.GetElementsCount() != 1) {
                    builder.SetError("Expected exactly one argument");
                    return;
                }
                auto serializedT = TTupleTypeInspector(*typeHelper, args.GetElementType(0));
                if (serializedT.GetElementsCount() != 3) {
                    builder.SetError("Serialized must be a Tuple<List<T>, Uint64, Uint64>");
                    return;
                }
                auto listT = TListTypeInspector(*typeHelper, serializedT.GetElementType(0));
                if (!listT) {
                    builder.SetError("Serialized's first element in tuple must be List<T>");
                    return;
                }
                if (!CheckUint64(*typeHelper, serializedT.GetElementType(1))) {
                    builder.SetError("Serialized's second element in tuple must be Uint64");
                    return;
                }
                if (!CheckUint64(*typeHelper, serializedT.GetElementType(2))) {
                    builder.SetError("Serialized's third element in tuple must be Uint64");
                    return;
                }
                auto tag = BuildResourceTag(*typeHelper, listT.GetItemType());
                builder.Args()->Add(args.GetElementType(0)).Done().Returns(builder.Resource(tag));
                if (!typesOnly) {
                    builder.Implementation(new TReservoirSamplingImpl<TDeserialize>(tag));
                }
                return;
            }

            if (name == CreateName) {
                if (args.GetElementsCount() != 2) {
                    builder.SetError("Expected exactly two arguments: element and limit");
                    return;
                }
                auto tag = BuildResourceTag(*typeHelper, args.GetElementType(0));
                builder.Args()->Add(args.GetElementType(0)).Add<ui64>().Done().Returns(builder.Resource(tag));
                if (!typesOnly) {
                    builder.Implementation(new TReservoirSamplingImpl<TCreate>(tag));
                }
                return;
            }

            if (name == AddName) {
                if (args.GetElementsCount() != 2) {
                    builder.SetError("Expected exactly two arguments: resource and element");
                    return;
                }
                auto tag = BuildResourceTag(*typeHelper, args.GetElementType(1));
                auto resource = TResourceTypeInspector(*typeHelper, args.GetElementType(0));
                if (!resource || resource.GetTag().Compare(tag)) {
                    builder.SetError(TStringBuilder() << "Expected Resource<'" << tag << "> as first argument, but got" << FormatType(*typeHelper, args.GetElementType(0)));
                    return;
                }
                auto resourceT = builder.Resource(tag);
                builder.Args()->Add(resourceT).Add(args.GetElementType(1)).Done().Returns(resourceT);
                if (!typesOnly) {
                    builder.Implementation(new TReservoirSamplingImpl<TAdd>(tag));
                }
                return;
            }

            auto extraTypesInspector = TTupleTypeInspector(*typeHelper, userTypeInspector.GetElementType(2));
            if (!extraTypesInspector || extraTypesInspector.GetElementsCount() != 1) {
                builder.SetError("Expected exactly 1 extra type in user type: type of values stored inside");
                return;
            }

            auto valueType = extraTypesInspector.GetElementType(0);
            auto tag = BuildResourceTag(*typeHelper, valueType);
            auto resourceT = builder.Resource(tag);

            if (name == MergeName) {
                if (args.GetElementsCount() != 2) {
                    builder.SetError("Expected exactly 2 positional args for merge: both are resources");
                    return;
                }
                auto arg0 = TResourceTypeInspector(*typeHelper, args.GetElementType(0));
                if (!arg0 || arg0.GetTag().Compare(tag)) {
                    builder.SetError(TStringBuilder() << "Expected Resource<'" << tag << "> as first argument, but got " << FormatType(*typeHelper, args.GetElementType(0)));
                    return;
                }
                auto arg1 = TResourceTypeInspector(*typeHelper, args.GetElementType(1));
                if (!arg1 || arg1.GetTag().Compare(tag)) {
                    builder.SetError(TStringBuilder() << "Expected Resource<'" << tag << "> as first argument, but got " << FormatType(*typeHelper, args.GetElementType(1)));
                    return;
                }
                builder.Args()->Add(resourceT).Add(resourceT).Done().Returns(resourceT);
                if (!typesOnly) {
                    builder.Implementation(new TReservoirSamplingImpl<TMerge>(tag));
                }
                return;
            }

            if (name == SerializeName) {
                if (args.GetElementsCount() != 1) {
                    builder.SetError("Expected exactly 1 positional argument: Resource");
                    return;
                }
                auto resource = TResourceTypeInspector(*typeHelper, args.GetElementType(0));
                if (!resource || resource.GetTag().Compare(tag)) {
                    builder.SetError(TStringBuilder() << "Expected Resource<'" << tag << "> as first argument, but got " << FormatType(*typeHelper, args.GetElementType(0)));
                    return;
                }
                auto serializedType = builder.Tuple(3)
                                                ->Add(builder.List()->Item(valueType).Build())
                                                .Add<ui64>()
                                                .Add<ui64>()
                                            .Build();
                builder.Args()->Add(resourceT).Done().Returns(serializedType);
                if (!typesOnly) {
                    builder.Implementation(new TReservoirSamplingImpl<TSerialize>(tag));
                }
                return;
            }

            if (name == GetResultListName) {
                if (args.GetElementsCount() != 1) {
                    builder.SetError("Expected exactly 1 positional argument: Resource");
                    return;
                }
                auto resource = TResourceTypeInspector(*typeHelper, args.GetElementType(0));
                if (!resource || resource.GetTag().Compare(tag)) {
                    builder.SetError(TStringBuilder() << "Expected Resource<'" << tag << "> as first argument, but got" << FormatType(*typeHelper, args.GetElementType(0)));
                    return;
                }
                builder.Args()->Add(resourceT).Done().Returns(builder.List()->Item(valueType).Build());
                if (!typesOnly) {
                    builder.Implementation(new TReservoirSamplingImpl<TGetResultList>(tag));
                }
                return;
            }

            if (name == GetResultValueName) {
                if (args.GetElementsCount() != 1) {
                    builder.SetError("Expected exactly 1 positional argument: Resource");
                    return;
                }
                auto resource = TResourceTypeInspector(*typeHelper, args.GetElementType(0));
                if (!resource || resource.GetTag().Compare(tag)) {
                    builder.SetError(TStringBuilder() << "Expected Resource<'" << tag << "> as first argument, but got" << FormatType(*typeHelper, args.GetElementType(0)));
                    return;
                }
                builder.Args()->Add(resourceT).Done().Returns(valueType);
                if (!typesOnly) {
                    builder.Implementation(new TReservoirSamplingImpl<TGetResultValue>(tag));
                }
                return;
            }
        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
    // clang-format on
};

} // namespace

REGISTER_MODULES(TReservoirSamplingModule)
