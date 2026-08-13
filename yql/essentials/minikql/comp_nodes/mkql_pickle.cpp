#include "mkql_pickle.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/computation/presort.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL {

namespace {

template <bool Stable>
class TPickleWrapper: public TMutableComputationNode<TPickleWrapper<Stable>> {
    using TBaseComputation = TMutableComputationNode<TPickleWrapper<Stable>>;

public:
    TPickleWrapper(TComputationMutables& mutables, TType* type, IComputationNode* data)
        : TBaseComputation(mutables)
        , Type_(type)
        , ValuePacker_(mutables)
        , Data_(data)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return MakeString(ValuePacker_.RefMutableObject(ctx, Stable, Type_).Pack(Data_->GetValue(ctx)));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Data_);
    }

    TType* Type_;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> ValuePacker_;
    IComputationNode* const Data_;
};

class TUnpickleWrapper: public TMutableComputationNode<TUnpickleWrapper> {
    using TBaseComputation = TMutableComputationNode<TUnpickleWrapper>;

public:
    TUnpickleWrapper(TComputationMutables& mutables, TType* type, IComputationNode* data)
        : TBaseComputation(mutables)
        , Type_(type)
        , ValuePacker_(mutables)
        , Data_(data)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        try {
            auto data = Data_->GetValue(ctx);
            auto buffer = data.AsStringRef();
            return ValuePacker_.RefMutableObject(ctx, false, Type_).Unpack(buffer, ctx.HolderFactory).Release();
        } catch (const std::exception& e) {
            UdfTerminate((TStringBuilder() << "Unpack failed. Original error is: " << e.what()).data());
        }
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Data_);
    }

    TType* const Type_;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> ValuePacker_;
    IComputationNode* const Data_;
};

class TGenericPresortEncoderBoxed: public TComputationValue<TGenericPresortEncoderBoxed>, public TGenericPresortEncoder {
    using TBase = TComputationValue<TGenericPresortEncoderBoxed>;

public:
    TGenericPresortEncoderBoxed(TMemoryUsageInfo* memInfo, TType* type)
        : TBase(memInfo)
        , TGenericPresortEncoder(type)
    {
    }
};

template <bool Desc>
class TPresortEncodeWrapper: public TMutableComputationNode<TPresortEncodeWrapper<Desc>> {
    using TBaseComputation = TMutableComputationNode<TPresortEncodeWrapper<Desc>>;

public:
    TPresortEncodeWrapper(TComputationMutables& mutables, TType* type, IComputationNode* data)
        : TBaseComputation(mutables)
        , Type_(type)
        , Encoder_(mutables)
        , Data_(data)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return MakeString(Encoder_.RefMutableObject(ctx, Type_).Encode(Data_->GetValue(ctx), Desc));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Data_);
    }

    TType* Type_;
    TMutableObjectOverBoxedValue<TGenericPresortEncoderBoxed> Encoder_;
    IComputationNode* const Data_;
};

} // namespace

IComputationNode* WrapPickle(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    return new TPickleWrapper<false>(ctx.Mutables, callable.GetInput(0).GetStaticType(), LocateNode(ctx.NodeLocator, callable, 0));
}

IComputationNode* WrapStablePickle(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    return new TPickleWrapper<true>(ctx.Mutables, callable.GetInput(0).GetStaticType(), LocateNode(ctx.NodeLocator, callable, 0));
}

IComputationNode* WrapUnpickle(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).IsImmediate() && callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    return new TUnpickleWrapper(ctx.Mutables, static_cast<TType*>(callable.GetInput(0).GetNode()), LocateNode(ctx.NodeLocator, callable, 1));
}

IComputationNode* WrapAscending(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    return new TPresortEncodeWrapper<false>(ctx.Mutables, callable.GetInput(0).GetStaticType(), LocateNode(ctx.NodeLocator, callable, 0));
}

IComputationNode* WrapDescending(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    return new TPresortEncodeWrapper<true>(ctx.Mutables, callable.GetInput(0).GetStaticType(), LocateNode(ctx.NodeLocator, callable, 0));
}

} // namespace NKikimr::NMiniKQL
