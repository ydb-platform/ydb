#include <yql/essentials/public/udf/udf_type_ops.h>
#include <yql/essentials/public/udf/udf_helpers.h>

#include <vector>

using namespace NKikimr;
using namespace NUdf;

namespace {

class TVector {
private:
    std::vector<TUnboxedValue, TUnboxedValue::TAllocator> Vector_;

public:
    TVector()
        : Vector_()
    {}

    TUnboxedValue GetResult(const IValueBuilder* builder) {
        TUnboxedValue* values = nullptr;
        auto list = builder->NewArray(Vector_.size(), values);
        std::copy(Vector_.begin(), Vector_.end(), values);

        return list;
    }

    void Emplace(const ui64 index, const TUnboxedValuePod& value) {
        if (index < Vector_.size()) {
            Vector_[index] = value;
        } else {
            Vector_.push_back(value);
        }
    }

    void Swap(const ui64 a, const ui64 b) {
        if (a < Vector_.size() && b < Vector_.size()) {
            std::swap(Vector_[a], Vector_[b]);
        }
    }

    void Reserve(ui64 expectedSize) {
        Vector_.reserve(expectedSize);
    }
};

extern const char VectorResourceName[] = "Vector.VectorResource";
class TVectorResource:
    public TBoxedResource<TVector, VectorResourceName>
{
public:
    template <typename... Args>
    inline TVectorResource(Args&&... args)
        : TBoxedResource(std::forward<Args>(args)...)
    {}
};

TVectorResource* GetVectorResource(const TUnboxedValuePod& arg) {
    TVectorResource::Validate(arg);
    return static_cast<TVectorResource*>(arg.AsBoxed().Get());
}

class TVectorCreate: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto resource = new TVectorResource;
        resource->Get()->Reserve(args[0].Get<ui64>());
        return TUnboxedValuePod(resource);
    }
};

class TVectorEmplace: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto resource = GetVectorResource(args[0]);
        resource->Get()->Emplace(args[1].Get<ui64>(), args[2]);
        return TUnboxedValuePod(resource);
    }
};

class TVectorSwap: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto resource = GetVectorResource(args[0]);
        resource->Get()->Swap(args[1].Get<ui64>(), args[2].Get<ui64>());
        return TUnboxedValuePod(resource);
    }
};

class TVectorGetResult: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        return GetVectorResource(args[0])->Get()->GetResult(valueBuilder);
    }
};

static const auto CreateName = TStringRef::Of("Create");
static const auto EmplaceName = TStringRef::Of("Emplace");
static const auto SwapName = TStringRef::Of("Swap");
static const auto GetResultName = TStringRef::Of("GetResult");

class TVectorModule: public IUdfModule {
public:
    TStringRef Name() const {
        return TStringRef::Of("Vector");
    }

    void CleanupOnTerminate() const final {
    }

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(CreateName)->SetTypeAwareness();
        sink.Add(EmplaceName)->SetTypeAwareness();
        sink.Add(SwapName)->SetTypeAwareness();
        sink.Add(GetResultName)->SetTypeAwareness();
    }

    void BuildFunctionTypeInfo(
        const TStringRef& name,
        TType* userType,
        const TStringRef& typeConfig,
        ui32 flags,
        IFunctionTypeInfoBuilder& builder) const final
    {
        Y_UNUSED(typeConfig);

        try {
            const bool typesOnly = (flags & TFlags::TypesOnly);
            builder.UserType(userType);

            auto typeHelper = builder.TypeInfoHelper();

            auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() != 3) {
                builder.SetError("User type is not a 3-tuple");
                return;
            }

            auto valueType = userTypeInspector.GetElementType(2);
            TType* vectorType = builder.Resource(VectorResourceName);

            if (name == CreateName) {
                builder.IsStrict();

                builder.Args()->Add<ui64>().Done().Returns(vectorType);

                if (!typesOnly) {
                    builder.Implementation(new TVectorCreate);
                }
            }

            if (name == EmplaceName) {
                builder.IsStrict();

                builder.Args()->Add(vectorType).Add<ui64>().Add(valueType).Done().Returns(vectorType);

                if (!typesOnly) {
                    builder.Implementation(new TVectorEmplace);
                }
            }

            if (name == SwapName) {
                builder.IsStrict();

                builder.Args()->Add(vectorType).Add<ui64>().Add<ui64>().Done().Returns(vectorType);

                if (!typesOnly) {
                    builder.Implementation(new TVectorSwap);
                }
            }

            if (name == GetResultName) {
                auto resultType = builder.List()->Item(valueType).Build();

                builder.IsStrict();

                builder.Args()->Add(vectorType).Done().Returns(resultType);

                if (!typesOnly) {
                    builder.Implementation(new TVectorGetResult);
                }
            }

        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace

REGISTER_MODULES(TVectorModule)
