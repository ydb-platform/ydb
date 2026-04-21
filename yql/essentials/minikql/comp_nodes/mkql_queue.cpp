#include "mkql_queue.h"

#include <yql/essentials/minikql/comp_nodes/mkql_window_range_pg_caller.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/utils/runtime_dispatch.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/core/sql_types/window_frame_bounds.h>
#include <yql/essentials/minikql/mkql_core_win_frames_collector.h>
#include <yql/essentials/public/udf/udf_string.h>
#include <yql/essentials/minikql/comp_nodes/mkql_window_frames_collector_params_deserializer.h>

namespace NKikimr {
using namespace NUdf;
namespace NMiniKQL {

namespace {

class TQueueResource: public TComputationValue<TQueueResource> {
public:
    TQueueResource(TMemoryUsageInfo* memInfo, const TStringBuf& tag, TMaybe<ui64> capacity, ui64 initSize)
        : TComputationValue(memInfo)
        , ResourceTag(tag)
        , Buffer(capacity, TUnboxedValue(), initSize)
        , BufferBytes(CurrentMemUsage())
    {
    }

    ~TQueueResource() {
        Buffer.Clear();
    }

    void UpdateBufferStats() {
        BufferBytes = CurrentMemUsage();
    }

    TSafeCircularBuffer<TUnboxedValue>& GetBuffer() {
        return Buffer;
    }

    const TFrameBoundsIndices& GetFrameBoundsIndices() const {
        return FrameBoundsIndices;
    }

    TFrameBoundsIndices& GetFrameBoundsIndices() {
        return FrameBoundsIndices;
    }

private:
    NUdf::TStringRef GetResourceTag() const override {
        return NUdf::TStringRef(ResourceTag);
    }

    void* GetResource() override {
        return this;
    }

    size_t CurrentMemUsage() const {
        return Buffer.Capacity() * sizeof(TUnboxedValue);
    }

    const TStringBuf ResourceTag;
    TSafeCircularBuffer<TUnboxedValue> Buffer;
    TFrameBoundsIndices FrameBoundsIndices;
    size_t BufferBytes;
};

class TQueueResourceUser {
public:
    TQueueResourceUser(TStringBuf&& tag, IComputationNode* resource);
    TSafeCircularBuffer<NUdf::TUnboxedValue>& CheckAndGetBuffer(const NUdf::TUnboxedValuePod& resource) const;
    TFrameBoundsIndices& CheckAndGetFrameBoundsIndices(const NUdf::TUnboxedValuePod& resource);
    const TFrameBoundsIndices& CheckAndGetFrameBoundsIndices(const NUdf::TUnboxedValuePod& resource) const;
    void UpdateBufferStats(const NUdf::TUnboxedValuePod& resource) const;

protected:
    const TStringBuf Tag;
    IComputationNode* const Resource;

    TQueueResource& GetResource(const NUdf::TUnboxedValuePod& resource) const;
};

TQueueResourceUser::TQueueResourceUser(TStringBuf&& tag, IComputationNode* resource)
    : Tag(tag)
    , Resource(resource)
{
}

TSafeCircularBuffer<TUnboxedValue>& TQueueResourceUser::CheckAndGetBuffer(const TUnboxedValuePod& resource) const {
    return GetResource(resource).GetBuffer();
}

TFrameBoundsIndices& TQueueResourceUser::CheckAndGetFrameBoundsIndices(const NUdf::TUnboxedValuePod& resource) {
    return GetResource(resource).GetFrameBoundsIndices();
}

const TFrameBoundsIndices& TQueueResourceUser::CheckAndGetFrameBoundsIndices(const NUdf::TUnboxedValuePod& resource) const {
    return GetResource(resource).GetFrameBoundsIndices();
}

void TQueueResourceUser::UpdateBufferStats(const TUnboxedValuePod& resource) const {
    GetResource(resource).UpdateBufferStats();
}

TQueueResource& TQueueResourceUser::GetResource(const TUnboxedValuePod& resource) const {
    const TStringBuf tag = resource.GetResourceTag();
    Y_DEBUG_ABORT_UNLESS(tag == Tag, "Expected correct Queue resource");
    return *static_cast<TQueueResource*>(resource.GetResource());
}

template <bool AlwaysExist>
class TQueueRange: public TComputationValue<TQueueRange<AlwaysExist>>, public TQueueResourceUser {
public:
    class TIterator: public TComputationValue<TIterator>, public TQueueResourceUser {
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

    TQueueRange(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, TUnboxedValue queue, size_t begin, size_t end, TStringBuf tag, IComputationNode* resource)
        : TComputationValue<TQueueRange<AlwaysExist>>(memInfo)
        , TQueueResourceUser(std::move(tag), resource)
        , CompCtx(compCtx)
        , Queue(queue)
        , Begin(begin)
        , End(std::min(end, CheckAndGetBuffer(Queue).Size()))
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

class TQueueCreateWrapper: public TMutableComputationNode<TQueueCreateWrapper> {
    typedef TMutableComputationNode<TQueueCreateWrapper> TBaseComputation;

public:
    TQueueCreateWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, const TString& name, TMaybe<ui64> capacity, ui64 initSize)
        : TBaseComputation(mutables)
        , DependentNodes(std::move(dependentNodes))
        , Name(name)
        , Capacity(capacity)
        , InitSize(initSize)
    {
    }

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

class TQueuePushWrapper: public TMutableComputationNode<TQueuePushWrapper>, public TQueueResourceUser {
    typedef TMutableComputationNode<TQueuePushWrapper> TBaseComputation;

public:
    TQueuePushWrapper(TComputationMutables& mutables, const TResourceType* resourceType, IComputationNode* resource, IComputationNode* value)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Value(value)
    {
    }

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

class TQueuePopWrapper: public TMutableComputationNode<TQueuePopWrapper>, public TQueueResourceUser {
    typedef TMutableComputationNode<TQueuePopWrapper> TBaseComputation;

public:
    TQueuePopWrapper(TComputationMutables& mutables, const TResourceType* resourceType, IComputationNode* resource)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
    {
    }

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

class TQueuePeekWrapper: public TMutableComputationNode<TQueuePeekWrapper>, public TQueueResourceUser {
    typedef TMutableComputationNode<TQueuePeekWrapper> TBaseComputation;

public:
    TQueuePeekWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, const TResourceType* resourceType, IComputationNode* resource, IComputationNode* index)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Index(index)
        , DependentNodes(std::move(dependentNodes))
    {
    }

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

class TQueueRangeWrapper: public TMutableComputationNode<TQueueRangeWrapper>, public TQueueResourceUser {
    typedef TMutableComputationNode<TQueueRangeWrapper> TBaseComputation;

public:
    TQueueRangeWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, const TResourceType* resourceType, IComputationNode* resource,
                       IComputationNode* begin, IComputationNode* end)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Begin(begin)
        , End(end)
        , DependentNodes(std::move(dependentNodes))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto queue = Resource->GetValue(ctx);

        auto begin = Begin->GetValue(ctx).Get<ui64>();
        auto end = End->GetValue(ctx).Get<ui64>();

        return ctx.HolderFactory.Create<TQueueRange</*AlwaysExist=*/false>>(ctx, queue, begin, end, Tag, Resource);
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

class TPreserveStreamValue: public TComputationValue<TPreserveStreamValue>, public TQueueResourceUser {
public:
    using TBase = TComputationValue<TPreserveStreamValue>;

    TPreserveStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream, NUdf::TUnboxedValue&& queue, TStringBuf tag, IComputationNode* resource, ui64 outpace)
        : TBase(memInfo)
        , TQueueResourceUser(std::move(tag), resource)
        , Stream(std::move(stream))
        , Queue(std::move(queue))
        , OutpaceGoal(outpace)
        , Buffer(CheckAndGetBuffer(Queue))
        , FrontIndex(Buffer.Size())
    {
    }

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

class TPreserveStreamWrapper: public TMutableComputationNode<TPreserveStreamWrapper>, public TQueueResourceUser {
    typedef TMutableComputationNode<TPreserveStreamWrapper> TBaseComputation;

public:
    TPreserveStreamWrapper(TComputationMutables& mutables, IComputationNode* stream, const TResourceType* resourceType, IComputationNode* resource, ui64 outpace)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Stream(stream)
        , Outpace(outpace)
    {
    }

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

template <typename TFactory, bool IsRangeSupported>
class TAggregateWindowValue: public TComputationValue<TAggregateWindowValue<TFactory, IsRangeSupported>>, public TQueueResourceUser {
public:
    using TBase = TComputationValue<TAggregateWindowValue<TFactory, IsRangeSupported>>;

    TAggregateWindowValue(TMemoryUsageInfo* memInfo,
                          NUdf::TUnboxedValue&& stream,
                          NUdf::TUnboxedValue&& queue,
                          TStringBuf tag,
                          IComputationNode* resource,
                          const TFactory& factory,
                          TComputationContext& ctx)
        : TBase(memInfo)
        , TQueueResourceUser(std::move(tag), resource)
        , Stream(std::move(stream))
        , Queue(std::move(queue))
        , Buffer(TQueueResourceUser::CheckAndGetBuffer(Queue))
        , AggregatedBounds(factory(Buffer,
                                   std::bind(&TAggregateWindowValue::ConsumeStream, this, std::placeholders::_1),
                                   TQueueResourceUser::CheckAndGetFrameBoundsIndices(Queue),
                                   ctx))
    {
    }

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value) override {
        switch (AggregatedBounds.Next()) {
            case EConsumeStatus::Ok:
                value = AggregatedBounds.GetCurrentElement();
                return NUdf::EFetchStatus::Ok;
            case EConsumeStatus::Wait:
                return NUdf::EFetchStatus::Yield;
            case EConsumeStatus::End:
                if (!Cleaned_) {
                    AggregatedBounds.Clean();
                    Cleaned_ = true;
                }
                return NUdf::EFetchStatus::Finish;
        }
    }

    EConsumeStatus ConsumeStream(TUnboxedValue& value) {
        switch (Stream.Fetch(value)) {
            case EFetchStatus::Ok:
                return EConsumeStatus::Ok;
            case EFetchStatus::Finish:
                return EConsumeStatus::End;
            case EFetchStatus::Yield:
                return EConsumeStatus::Wait;
        }
    }

    const NUdf::TUnboxedValue Stream;
    const NUdf::TUnboxedValue Queue;
    TSafeCircularBuffer<TUnboxedValue>& Buffer;
    bool Cleaned_ = false;
    std::invoke_result_t<TFactory, TSafeCircularBuffer<TUnboxedValue>&, std::function<EConsumeStatus(TUnboxedValue&)>, TFrameBoundsIndices&, TComputationContext&> AggregatedBounds;
};

template <typename TFactory, bool IsRangeSupported>
class WinFramesCollector: public TMutableComputationNode<WinFramesCollector<TFactory, IsRangeSupported>>, public TQueueResourceUser {
    typedef TMutableComputationNode<WinFramesCollector> TBaseComputation;

public:
    WinFramesCollector(TComputationMutables& mutables,
                       IComputationNode* stream,
                       const TResourceType* resourceType,
                       IComputationNode* resource,
                       TFactory&& factory,
                       const std::vector<IComputationNode*>& dependentNodes)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Stream(stream)
        , Factory(std::move(factory))
        , DependentNodes(dependentNodes)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TAggregateWindowValue<TFactory, IsRangeSupported>>(Stream->GetValue(ctx), Resource->GetValue(ctx), Tag, Resource, Factory, ctx);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Resource);
        this->DependsOn(Stream);
        std::for_each(DependentNodes.cbegin(), DependentNodes.cend(), std::bind(&WinFramesCollector::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Stream;
    const TFactory Factory;
    const std::vector<IComputationNode*> DependentNodes;
};

template <bool IsRange, bool IsIncremental, bool ReturnSingleElement>
class TWinFrame: public TMutableComputationNode<TWinFrame<IsRange, IsIncremental, ReturnSingleElement>>, public TQueueResourceUser {
    typedef TMutableComputationNode<TWinFrame<IsRange, IsIncremental, ReturnSingleElement>> TBaseComputation;

public:
    TWinFrame(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, const TResourceType* resourceType, IComputationNode* resource,
              ui64 handle)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Handle(handle)
        , DependentNodes(std::move(dependentNodes))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto queue = Resource->GetValue(ctx);

        auto windows = this->CheckAndGetFrameBoundsIndices(queue);
        auto frame = this->GetWindowFrame(Handle, windows);
        if constexpr (ReturnSingleElement) {
            if (frame.Size() == 0) {
                return TUnboxedValuePod();
            } else {
                const auto& valRef = CheckAndGetBuffer(queue).Get(frame.Max() - 1);
                return valRef.MakeOptional();
            }
            return CheckAndGetBuffer(queue).Get(frame.Min());
        } else {
            return ctx.HolderFactory.Create<TQueueRange</*AlwaysExist=*/true>>(ctx, queue, frame.Min(), frame.Max(), Tag, Resource);
        }
    }

    TRowWindowFrame GetWindowFrame(ui64 handle, const TFrameBoundsIndices& windows) const {
        if constexpr (IsRange) {
            if constexpr (IsIncremental) {
                return windows.GetIntervalInQueueByRangeIncremental(handle);
            } else {
                return windows.GetIntervalInQueueByRange(handle);
            }
        } else {
            if constexpr (IsIncremental) {
                return windows.GetIntervalInQueueByRowIncremental(handle);
            } else {
                return windows.GetIntervalInQueueByRow(handle);
            }
        }
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Resource);
        std::for_each(DependentNodes.cbegin(), DependentNodes.cend(), std::bind(&TWinFrame::DependsOn, this, std::placeholders::_1));
    }

    const ui64 Handle;
    const TComputationNodePtrVector DependentNodes;
};

template <class T, class... Args>
IComputationNode* MakeNodeWithDeps(TCallable& callable, const TComputationNodeFactoryContext& ctx, unsigned reqArgs, Args... args) {
    TComputationNodePtrVector dependentNodes(callable.GetInputsCount() - reqArgs);
    for (ui32 i = reqArgs; i < callable.GetInputsCount(); ++i) {
        dependentNodes[i - reqArgs] = LocateNode(ctx.NodeLocator, callable, i);
    }
    return new T(ctx.Mutables, std::move(dependentNodes), std::forward<Args>(args)...);
}

IComputationNode* DispatchWinStreamCollectorBasedOnSortedColumn(const TRuntimeNode& paramsNode,
                                                                const TComputationNodeFactoryContext& ctx,
                                                                IComputationNode* stream,
                                                                TResourceType* resourceType,
                                                                IComputationNode* resource,
                                                                const TStructType* streamStructType,
                                                                ESortOrder sortOrder) {
    auto memberExtractor = [](const TUnboxedValue& value, ui32 memberIndex) {
        return value.GetElement(memberIndex);
    };

    auto nullChecker = [](const TUnboxedValue& value) -> bool {
        return !static_cast<bool>(value);
    };

    auto elementExtractor =
        []<typename T>(const TUnboxedValue& pod) -> T {
        return pod.Get<T>();
    };

    auto nodeExtractor = [ctx](const TRuntimeNode& node) -> IComputationNode* {
        return LocateNode(ctx.NodeLocator, *node.GetNode());
    };

    auto [variantBounds, deps] = DeserializeBoundsAsVariant(paramsNode, streamStructType, nodeExtractor, ctx.Mutables.CurValueIndex);
    TDeserializerContext deserializerContext(memberExtractor, nullChecker, elementExtractor);
    auto bounds = ConvertBoundsToComparators<TUnboxedValue, TUnboxedValue, TComputationContext>(std::move(variantBounds), sortOrder, deserializerContext);

    auto factory = TCoreWinFramesCollector<TUnboxedValue, TComputationContext, /*IsRangeSupported=*/true>::CreateFactory(bounds);

    return new WinFramesCollector<decltype(factory), /*IsRangeSupported=*/true>(ctx.Mutables,
                                                                                stream,
                                                                                resourceType,
                                                                                resource,
                                                                                std::move(factory),
                                                                                deps);
}

IComputationNode* DispatchWinStreamCollectorBasedOnStreamType(const TRuntimeNode& paramsNode,
                                                              const TComputationNodeFactoryContext& ctx,
                                                              IComputationNode* stream,
                                                              TResourceType* resourceType,
                                                              IComputationNode* resource,
                                                              const TStructType* streamStructType,
                                                              ESortOrder sortOrder) {
    return DispatchWinStreamCollectorBasedOnSortedColumn(paramsNode, ctx, stream, resourceType, resource, streamStructType, sortOrder);
}

IComputationNode* DispatchWinStreamCollectorBasedOnOrderedColumn(const TRuntimeNode& paramsNode,
                                                                 const TComputationNodeFactoryContext& ctx,
                                                                 TType* streamType,
                                                                 IComputationNode* stream,
                                                                 TResourceType* resourceType,
                                                                 IComputationNode* resource) {
    MKQL_ENSURE(streamType->IsStream(), "Expected stream type.");
    auto streamItemType = AS_TYPE(TStreamType, streamType)->GetItemType();
    MKQL_ENSURE(streamItemType->IsStruct(), "Expected stream of struct type.");
    auto structType = AS_TYPE(TStructType, streamItemType);

    auto sortOrder = DeserializeSortOrder(paramsNode);
    if (!AnyRangeProvided(paramsNode)) {
        auto [variantBounds, deps] = DeserializeBoundsAsVariant(paramsNode, structType, TNodeExtractor{}, ctx.Mutables.CurValueIndex);
        MKQL_ENSURE(deps.empty(), "Unexpected dependent nodes.");
        auto bounds = ConvertBoundsToComparators<TUnboxedValue, TUnboxedValue, TComputationContext, TNoopDeserializerContext>(std::move(variantBounds), ESortOrder::Unimportant, TNoopDeserializerContext{});
        MKQL_ENSURE(bounds.RangeIntervals().empty() && bounds.RangeIncrementals().empty(), "Unexpected bounds.");
        auto factory = TCoreWinFramesCollector<TUnboxedValue, TComputationContext, /*IsRangeSupported=*/false>::CreateFactory(bounds);
        return new WinFramesCollector<decltype(factory), /*IsRangeSupported=*/false>(ctx.Mutables,
                                                                                     stream,
                                                                                     resourceType,
                                                                                     resource,
                                                                                     std::move(factory),
                                                                                     deps);
    }

    switch (sortOrder) {
        case ESortOrder::Asc:
        case ESortOrder::Desc:
            return DispatchWinStreamCollectorBasedOnStreamType(paramsNode, ctx, stream, resourceType, resource, structType, sortOrder);
        default:
            MKQL_ENSURE(false, "Unexpected sort order");
            return nullptr;
    }
}

} // namespace

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

// #############################################################################
// ###### Wrappers that are used by CoreWinFramesCollector API #######
// #############################################################################

template <bool IsRange, bool IsIncremental, bool ReturnSingleElement>
IComputationNode* MakeWinFrameWithDeps(TCallable& callable, const TComputationNodeFactoryContext& ctx, unsigned reqArgs, TResourceType* resourceType, IComputationNode* resource, ui64 handle) {
    return MakeNodeWithDeps<TWinFrame<IsRange, IsIncremental, ReturnSingleElement>>(callable, ctx, reqArgs, resourceType, resource, handle);
}

IComputationNode* WrapWinFramesCollector(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "WinFramesCollector: Expected 3 args");
    auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    auto streamType = callable.GetInput(0).GetStaticType();
    auto resource = LocateNode(ctx.NodeLocator, callable, 1);
    auto resourceType = AS_TYPE(TResourceType, callable.GetInput(1));
    auto paramsNode = callable.GetInput(2);

    return DispatchWinStreamCollectorBasedOnOrderedColumn(paramsNode, ctx, streamType, stream, resourceType, resource);
}

IComputationNode* WrapWinFrame(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const unsigned reqArgs = 5;
    MKQL_ENSURE(callable.GetInputsCount() >= reqArgs, "QueueRange: Expected at least " << reqArgs << " arg");
    auto resourceType = AS_TYPE(TResourceType, callable.GetInput(0));

    TDataType* handleDataType = AS_TYPE(TDataType, callable.GetInput(1));
    MKQL_ENSURE(handleDataType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64 as handle.");
    TDataType* IsIncrementalDataType = AS_TYPE(TDataType, callable.GetInput(2));
    MKQL_ENSURE(IsIncrementalDataType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool as IsIncremental marker.");
    TDataType* isRangeDataType = AS_TYPE(TDataType, callable.GetInput(3));
    MKQL_ENSURE(isRangeDataType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool as IsRange marker.");
    TDataType* isSingleElementDataType = AS_TYPE(TDataType, callable.GetInput(4));
    MKQL_ENSURE(isSingleElementDataType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool as IsSingleElement marker.");
    auto resource = LocateNode(ctx.NodeLocator, callable, 0);

    auto handle = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui64>();
    auto IsIncremental = AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().Get<bool>();
    auto isRange = AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().Get<bool>();
    bool isSingleElement = AS_VALUE(TDataLiteral, callable.GetInput(4))->AsValue().Get<bool>();

    return YQL_RUNTIME_DISPATCH(MakeWinFrameWithDeps, 3, isRange, IsIncremental, isSingleElement, callable, ctx, reqArgs, resourceType, resource, handle);
}

} // namespace NMiniKQL
} // namespace NKikimr
