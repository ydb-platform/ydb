#include "mkql_erased.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TErasedPayload {
    TType* Type;
    NUdf::TUnboxedValue Value;
};

class TErasedResource: public TComputationValue<TErasedResource> {
public:
    TErasedResource(TMemoryUsageInfo* memInfo, TType* type, NUdf::TUnboxedValue value)
        : TComputationValue(memInfo)
        , Payload_{type, std::move(value)}
    {
    }

    NUdf::TStringRef GetResourceTag() const override {
        return NUdf::TStringRef(ErasedResourceTag);
    }

    void* GetResource() override {
        return &Payload_;
    }

private:
    TErasedPayload Payload_;
};

class TAsErasedWrapper: public TMutableComputationNode<TAsErasedWrapper> {
    using TBaseComputation = TMutableComputationNode<TAsErasedWrapper>;

public:
    TAsErasedWrapper(TComputationMutables& mutables, IComputationNode* value, TType* type)
        : TBaseComputation(mutables)
        , Value(value)
        , Type(type)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TErasedResource>(Type, Value->GetValue(ctx));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Value);
    }

    IComputationNode* const Value;
    TType* const Type;
};

class TPeekErasedWrapper: public TMutableComputationNode<TPeekErasedWrapper> {
    using TBaseComputation = TMutableComputationNode<TPeekErasedWrapper>;

public:
    TPeekErasedWrapper(TComputationMutables& mutables, IComputationNode* resource, TType* expectedType)
        : TBaseComputation(mutables)
        , Resource(resource)
        , ExpectedType(expectedType)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto res = Resource->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(res.GetResourceTag() == NUdf::TStringRef(ErasedResourceTag), "Expected _Erased resource");
        auto* payload = static_cast<TErasedPayload*>(res.GetResource());
        // Both the stored and the expected types go through InternType at wrap
        // time, so structural equality is encoded as pointer equality here.
        if (payload->Type != ExpectedType) {
            return NUdf::TUnboxedValue(); // empty Optional<U>
        }
        return NUdf::TUnboxedValuePod(payload->Value).MakeOptional();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource);
    }

    IComputationNode* const Resource;
    TType* const ExpectedType;
};

} // namespace

IComputationNode* WrapAsErased(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    const auto type = ctx.Env.InternType(callable.GetInput(0).GetStaticType());
    return new TAsErasedWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), type);
}

IComputationNode* WrapPeekErased(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    AS_TYPE(TResourceType, callable.GetInput(0));
    MKQL_ENSURE(callable.GetInput(1).GetNode()->GetType()->IsType(), "Expected type");
    const auto expectedType = ctx.Env.InternType(static_cast<TType*>(callable.GetInput(1).GetNode()));
    return new TPeekErasedWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), expectedType);
}

} // namespace NMiniKQL
} // namespace NKikimr
