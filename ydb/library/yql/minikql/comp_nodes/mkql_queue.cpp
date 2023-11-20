#include "mkql_queue.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/public/udf/udf_string.h>

namespace NKikimr {
using namespace NUdf;
namespace NMiniKQL {

namespace {

class TQueueResource : public TComputationValue<TQueueResource> {
public:
    TQueueResource(TMemoryUsageInfo* memInfo, const TStringBuf& tag, TMaybe<ui64> capacity, ui64 initSize)
        : TComputationValue(memInfo)
        , ResourceTag(tag)
        , Buffer(capacity, TUnboxedValue(), initSize)
        , BufferBytes(CurrentMemUsage())
    {
        MKQL_MEM_TAKE(memInfo, &Buffer, BufferBytes);
    }

    ~TQueueResource() {
        Y_DEBUG_ABORT_UNLESS(BufferBytes == CurrentMemUsage());
        MKQL_MEM_RETURN(GetMemInfo(), &Buffer, CurrentMemUsage());
        Buffer.Clear();
    }

    void UpdateBufferStats() {
        MKQL_MEM_RETURN(GetMemInfo(), &Buffer, BufferBytes);
        BufferBytes = CurrentMemUsage();
        MKQL_MEM_TAKE(GetMemInfo(), &Buffer, BufferBytes);
    }

    TSafeCircularBuffer<TUnboxedValue>& GetBuffer() {
        return Buffer;
    }

private:
    NUdf::TStringRef GetResourceTag() const override {
        return NUdf::TStringRef(ResourceTag);
    }

    void* GetResource() override {
        return this;
    }

    size_t CurrentMemUsage() const {
        return Buffer.Size() * sizeof(TUnboxedValue);
    }

    const TStringBuf ResourceTag;
    TSafeCircularBuffer<TUnboxedValue> Buffer;
    size_t BufferBytes;
};

class TQueueResource;
class TQueueResourceUser {
public:
    TQueueResourceUser(TStringBuf&& tag, IComputationNode* resource);
    TSafeCircularBuffer<NUdf::TUnboxedValue>& CheckAndGetBuffer(const NUdf::TUnboxedValuePod& resource) const;
    void UpdateBufferStats(const NUdf::TUnboxedValuePod& resource) const;

protected:
    const TStringBuf Tag;
    IComputationNode* const Resource;

    TQueueResource& GetResource(const NUdf::TUnboxedValuePod& resource) const;
};

TQueueResourceUser::TQueueResourceUser(TStringBuf&& tag, IComputationNode* resource)
    : Tag(tag)
    , Resource(resource)
{}

TSafeCircularBuffer<TUnboxedValue>& TQueueResourceUser::CheckAndGetBuffer(const TUnboxedValuePod& resource) const {
    return GetResource(resource).GetBuffer();
}

void TQueueResourceUser::UpdateBufferStats(const TUnboxedValuePod& resource) const {
    GetResource(resource).UpdateBufferStats();
}

TQueueResource& TQueueResourceUser::GetResource(const TUnboxedValuePod& resource) const {
    const TStringBuf tag = resource.GetResourceTag();
    Y_DEBUG_ABORT_UNLESS(tag == Tag, "Expected correct Queue resource");
    return *static_cast<TQueueResource*>(resource.GetResource());
}

class TQueueCreateWrapper : public TMutableComputationNode<TQueueCreateWrapper> {
    typedef TMutableComputationNode<TQueueCreateWrapper> TBaseComputation;
public:
    TQueueCreateWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, const TString& name, TMaybe<ui64> capacity, ui64 initSize)
        : TBaseComputation(mutables)
        , DependentNodes(std::move(dependentNodes))
        , Name(name)
        , Capacity(capacity)
        , InitSize(initSize)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return NUdf::TUnboxedValuePod(new TQueueResource(&ctx.HolderFactory.GetMemInfo(), Name, Capacity, InitSize));
    }

private:
    void RegisterDependencies() const final {
        std::for_each(DependentNodes.cbegin(), DependentNodes.cend(), std::bind(&TQueueCreateWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector DependentNodes;
    const TString Name;
    const TMaybe<ui64> Capacity;
    const ui64 InitSize;
};

class TQueuePushWrapper : public TMutableComputationNode<TQueuePushWrapper>, public TQueueResourceUser {
    typedef TMutableComputationNode<TQueuePushWrapper> TBaseComputation;
public:
    TQueuePushWrapper(TComputationMutables& mutables, const TResourceType* resourceType, IComputationNode* resource, IComputationNode* value)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Value(value)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto resource = Resource->GetValue(ctx);
        auto& buffer = CheckAndGetBuffer(resource);
        buffer.PushBack(Value->GetValue(ctx));
        if (buffer.IsUnbounded()) {
            UpdateBufferStats(resource);
        }
        return resource.Release();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource);
        DependsOn(Value);
    }

    IComputationNode* const Value;
};

class TQueuePopWrapper : public TMutableComputationNode<TQueuePopWrapper>, public TQueueResourceUser {
    typedef TMutableComputationNode<TQueuePopWrapper> TBaseComputation;
public:
    TQueuePopWrapper(TComputationMutables& mutables, const TResourceType* resourceType, IComputationNode* resource)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto resource = Resource->GetValue(ctx);
        CheckAndGetBuffer(resource).PopFront();
        return resource.Release();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource);
    }
};

class TQueuePeekWrapper : public TMutableComputationNode<TQueuePeekWrapper>, public TQueueResourceUser {
    typedef TMutableComputationNode<TQueuePeekWrapper> TBaseComputation;
public:
    TQueuePeekWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, const TResourceType* resourceType, IComputationNode* resource, IComputationNode* index)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Index(index), DependentNodes(std::move(dependentNodes))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto resource = Resource->GetValue(ctx);
        auto index = Index->GetValue(ctx);
        const auto& valRef = CheckAndGetBuffer(resource).Get(index.Get<ui64>());
        return !valRef ? NUdf::TUnboxedValuePod() : valRef.MakeOptional();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource);
        DependsOn(Index);
        std::for_each(DependentNodes.cbegin(), DependentNodes.cend(), std::bind(&TQueuePeekWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Index;
    const TComputationNodePtrVector DependentNodes;
};

class TQueueRangeWrapper : public TMutableComputationNode<TQueueRangeWrapper>, public TQueueResourceUser {
    typedef TMutableComputationNode<TQueueRangeWrapper> TBaseComputation;
public:
    class TValue : public TComputationValue<TValue>, public TQueueResourceUser {
    public:
        class TIterator : public TComputationValue<TIterator>, public TQueueResourceUser {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TUnboxedValue queue, size_t begin, size_t end, ui64 generation, TStringBuf tag, IComputationNode* resource)
                : TComputationValue<TIterator>(memInfo)
                , TQueueResourceUser(std::move(tag), resource)
                , Queue(queue)
                , Buffer(CheckAndGetBuffer(queue))
                , Current(begin)
                , End(end)
                , Generation(generation)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                MKQL_ENSURE(Generation == Buffer.Generation(),
                    "Queue generation changed while doing QueueRange: expected " << Generation << ", got: " << Buffer.Generation());
                if (Current >= End) {
                    return false;
                }

                const auto& valRef = Buffer.Get(Current++);
                value = !valRef ? NUdf::TUnboxedValuePod() : valRef.MakeOptional();
                return true;
            }

            bool Skip() override {
                if (Current >= End) {
                    return false;
                }
                Current++;
                return true;
            }

            const TUnboxedValue Queue;
            const TSafeCircularBuffer<TUnboxedValue>& Buffer;
            size_t Current;
            const size_t End;
            const ui64 Generation;
        };

        TValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, TUnboxedValue queue, size_t begin, size_t end, TStringBuf tag, IComputationNode* resource)
            : TComputationValue<TValue>(memInfo)
            , TQueueResourceUser(std::move(tag), resource)
            , CompCtx(compCtx)
            , Queue(queue)
            , Begin(begin)
            , End(std::min(end, CheckAndGetBuffer(Queue).UsedSize()))
            , Generation(CheckAndGetBuffer(Queue).Generation())
        {
        }

    private:
        ui64 GetListLength() const final {
            return Begin < End ? (End - Begin) : 0;
        }

        bool HasListItems() const final {
            return GetListLength() != 0;
        }

        bool HasFastListLength() const final {
            return true;
        }

        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx.HolderFactory.Create<TIterator>(Queue, Begin, End, Generation, Tag, Resource);
        }

        TComputationContext& CompCtx;
        const TUnboxedValue Queue;
        const size_t Begin;
        const size_t End;
        const ui64 Generation;
    };

    TQueueRangeWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, const TResourceType* resourceType, IComputationNode* resource,
                       IComputationNode* begin, IComputationNode* end)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Begin(begin)
        , End(end)
        , DependentNodes(std::move(dependentNodes))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto queue = Resource->GetValue(ctx);

        auto begin = Begin->GetValue(ctx).Get<ui64>();
        auto end = End->GetValue(ctx).Get<ui64>();

        return ctx.HolderFactory.Create<TValue>(ctx, queue, begin, end, Tag, Resource);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource);
        DependsOn(Begin);
        DependsOn(End);
        std::for_each(DependentNodes.cbegin(), DependentNodes.cend(), std::bind(&TQueueRangeWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Begin;
    IComputationNode* const End;
    const TComputationNodePtrVector DependentNodes;
};

class TPreserveStreamValue : public TComputationValue<TPreserveStreamValue>, public TQueueResourceUser {
public:
    using TBase = TComputationValue<TPreserveStreamValue>;

    TPreserveStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream
        , NUdf::TUnboxedValue&& queue, TStringBuf tag, IComputationNode* resource, ui64 outpace)
        : TBase(memInfo)
        , TQueueResourceUser(std::move(tag), resource)
        , Stream(std::move(stream))
        , Queue(std::move(queue))
        , OutpaceGoal(outpace)
        , Buffer(CheckAndGetBuffer(Queue))
        , FrontIndex(Buffer.UsedSize())
    {}

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value) override {
        switch (State) {
            case EPreserveState::Done:
                return NUdf::EFetchStatus::Finish;
            case EPreserveState::Feed:
            case EPreserveState::Yield:
                break;
            default:
                Y_ABORT_UNLESS(Outpace > 0);
                Buffer.PopFront();
                --Outpace;
        }
        for (NUdf::TUnboxedValue item; State != EPreserveState::Emit && Outpace <= OutpaceGoal;) {
            switch (Stream.Fetch(item)) {
            case NUdf::EFetchStatus::Yield:
                State = EPreserveState::Yield;
                return NUdf::EFetchStatus::Yield;
            case NUdf::EFetchStatus::Finish:
                State = EPreserveState::Emit;
                break;
            case NUdf::EFetchStatus::Ok:
                Buffer.PushBack(std::move(item));
                if (Buffer.IsUnbounded()) {
                    UpdateBufferStats(Queue);
                }
                ++Outpace;
                if (Outpace > OutpaceGoal) {
                    State = EPreserveState::GoOn;
                } else {
                    State = EPreserveState::Feed;
                }
            }
        }
        if (!Outpace) {
            Buffer.Clean();
            State = EPreserveState::Done;
            return NUdf::EFetchStatus::Finish;
        }
        value = Buffer.Get(FrontIndex);
        return NUdf::EFetchStatus::Ok;
    }

    enum class EPreserveState {
        Feed,
        GoOn,
        Yield,
        Emit,
        Done
    };
    const NUdf::TUnboxedValue Stream;
    const NUdf::TUnboxedValue Queue;
    const ui64 OutpaceGoal;
    TSafeCircularBuffer<TUnboxedValue>& Buffer;
    const size_t FrontIndex;

    EPreserveState State = EPreserveState::Feed;
    ui64 Outpace = 0;
};

class TPreserveStreamWrapper : public TMutableComputationNode<TPreserveStreamWrapper>, public TQueueResourceUser {
    typedef TMutableComputationNode<TPreserveStreamWrapper> TBaseComputation;
public:
    TPreserveStreamWrapper(TComputationMutables& mutables, IComputationNode* stream, const TResourceType* resourceType, IComputationNode* resource, ui64 outpace)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Stream(stream)
        , Outpace(outpace)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TPreserveStreamValue>(Stream->GetValue(ctx), Resource->GetValue(ctx), Tag, Resource, Outpace);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource);
        DependsOn(Stream);
    }

    IComputationNode* const Stream;
    const ui64 Outpace;
};

template<class T, class...Args>
IComputationNode* MakeNodeWithDeps(TCallable& callable, const TComputationNodeFactoryContext& ctx, unsigned reqArgs, Args...args) {
    TComputationNodePtrVector dependentNodes(callable.GetInputsCount() - reqArgs);
    for (ui32 i = reqArgs; i < callable.GetInputsCount(); ++i) {
        dependentNodes[i - reqArgs] = LocateNode(ctx.NodeLocator, callable, i);
    }
    return new T(ctx.Mutables, std::move(dependentNodes), std::forward<Args>(args)...);
}

}

IComputationNode* WrapQueueCreate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const unsigned reqArgs = 3;
    MKQL_ENSURE(callable.GetInputsCount() >= reqArgs, "QueueCreate: Expected at least " << reqArgs << " arg");
    auto queueNameValue = AS_VALUE(TDataLiteral, callable.GetInput(0));
    TMaybe<ui64> capacity;
    if (!callable.GetInput(1).GetStaticType()->IsVoid()) {
        auto queueCapacityValue = AS_VALUE(TDataLiteral, callable.GetInput(1));
        capacity = queueCapacityValue->AsValue().Get<ui64>();
    }
    auto queueInitSizeValue = AS_VALUE(TDataLiteral, callable.GetInput(2));
    const TString name(queueNameValue->AsValue().AsStringRef());
    const auto initSize = queueInitSizeValue->AsValue().Get<ui64>();
    return MakeNodeWithDeps<TQueueCreateWrapper>(callable, ctx, reqArgs, name, capacity, initSize);
}

IComputationNode* WrapQueuePush(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "QueuePush: Expected 2 arg");
    auto resourceType = AS_TYPE(TResourceType, callable.GetInput(0));
    auto resource = LocateNode(ctx.NodeLocator, callable, 0);
    auto value = LocateNode(ctx.NodeLocator, callable, 1);
    return new TQueuePushWrapper(ctx.Mutables, resourceType, resource, value);
}

IComputationNode* WrapQueuePop(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "QueuePop: Expected 1 arg");
    auto resourceType = AS_TYPE(TResourceType, callable.GetInput(0));
    auto resource = LocateNode(ctx.NodeLocator, callable, 0);
    return new TQueuePopWrapper(ctx.Mutables, resourceType, resource);
}

IComputationNode* WrapQueuePeek(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const unsigned reqArgs = 2;
    MKQL_ENSURE(callable.GetInputsCount() >= reqArgs, "QueuePeek: Expected at least " << reqArgs << " arg");
    auto resourceType = AS_TYPE(TResourceType, callable.GetInput(0));
    TDataType* indexType = AS_TYPE(TDataType, callable.GetInput(1));
    MKQL_ENSURE(indexType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64 as queue index");
    auto resource = LocateNode(ctx.NodeLocator, callable, 0);
    auto index = LocateNode(ctx.NodeLocator, callable, 1);
    return MakeNodeWithDeps<TQueuePeekWrapper>(callable, ctx, reqArgs, resourceType, resource, index);
}

IComputationNode* WrapQueueRange(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const unsigned reqArgs = 3;
    MKQL_ENSURE(callable.GetInputsCount() >= reqArgs, "QueueRange: Expected at least " << reqArgs << " arg");
    auto resourceType = AS_TYPE(TResourceType, callable.GetInput(0));

    TDataType* beginIndexType = AS_TYPE(TDataType, callable.GetInput(1));
    MKQL_ENSURE(beginIndexType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64 as queue begin index");

    TDataType* endIndexType = AS_TYPE(TDataType, callable.GetInput(2));
    MKQL_ENSURE(endIndexType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64 as queue end index");

    auto resource = LocateNode(ctx.NodeLocator, callable, 0);
    auto beginIndex = LocateNode(ctx.NodeLocator, callable, 1);
    auto endIndex = LocateNode(ctx.NodeLocator, callable, 2);
    return MakeNodeWithDeps<TQueueRangeWrapper>(callable, ctx, reqArgs, resourceType, resource, beginIndex, endIndex);
}

IComputationNode* WrapPreserveStream(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    Y_UNUSED(ctx);
    MKQL_ENSURE(callable.GetInputsCount() == 3, "PreserveStream: Expected 3 arg");
    auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    auto resource = LocateNode(ctx.NodeLocator, callable, 1);
    auto resourceType = AS_TYPE(TResourceType, callable.GetInput(1));
    auto outpaceValue = AS_VALUE(TDataLiteral, callable.GetInput(2));
    const auto outpace = outpaceValue->AsValue().Get<ui64>();
    return new TPreserveStreamWrapper(ctx.Mutables, stream, resourceType, resource, outpace);
}

}
}
