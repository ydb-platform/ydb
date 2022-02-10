#include "mkql_pickle.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/computation/presort.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool Stable>
class TPickleWrapper : public TMutableComputationNode<TPickleWrapper<Stable>> {
    typedef TMutableComputationNode<TPickleWrapper<Stable>> TBaseComputation;
public:
    TPickleWrapper(TComputationMutables& mutables, TType* type, IComputationNode* data)
        : TBaseComputation(mutables)
        , Type(type)
        , ValuePacker(mutables)
        , Data(data)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return MakeString(ValuePacker.RefMutableObject(ctx, Stable, Type).Pack(Data->GetValue(ctx)));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Data);
    }

    TType* Type;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> ValuePacker;
    IComputationNode *const Data;
};

class TUnpickleWrapper : public TMutableComputationNode<TUnpickleWrapper> {
    typedef TMutableComputationNode<TUnpickleWrapper> TBaseComputation;
public:
    TUnpickleWrapper(TComputationMutables& mutables, TType* type, IComputationNode* data)
        : TBaseComputation(mutables)
        , Type(type)
        , ValuePacker(mutables)
        , Data(data)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto data = Data->GetValue(ctx);
        auto buffer = data.AsStringRef();
        return ValuePacker.RefMutableObject(ctx, false, Type).Unpack(buffer, ctx.HolderFactory).Release();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Data);
    }

    TType* const Type;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> ValuePacker;
    IComputationNode *const Data;
};


class TGenericPresortEncoderBoxed : public TComputationValue<TGenericPresortEncoderBoxed>, public TGenericPresortEncoder {
    typedef TComputationValue<TGenericPresortEncoderBoxed> TBase;
public:
    TGenericPresortEncoderBoxed(TMemoryUsageInfo* memInfo, TType* type)
        : TBase(memInfo)
        , TGenericPresortEncoder(type)
    {}
};

template <bool Desc>
class TPresortEncodeWrapper : public TMutableComputationNode<TPresortEncodeWrapper<Desc>> {
    typedef TMutableComputationNode<TPresortEncodeWrapper<Desc>> TBaseComputation;
public:
    TPresortEncodeWrapper(TComputationMutables& mutables, TType* type, IComputationNode* data)
        : TBaseComputation(mutables)
        , Type(type)
        , Encoder(mutables)
        , Data(data)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return MakeString(Encoder.RefMutableObject(ctx, Type).Encode(Data->GetValue(ctx), Desc));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Data);
    }

    TType* Type;
    TMutableObjectOverBoxedValue<TGenericPresortEncoderBoxed> Encoder;
    IComputationNode *const Data;
};

}

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

}
}
