#include "mkql_queue.h"
#include "mkql_window_frames_collector_params_deserializer.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/core/sql_types/window_frame_bounds.h>
#include <yql/essentials/minikql/mkql_core_win_frames_collector.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins_datetime.h>
#include <yql/essentials/public/udf/udf_string.h>

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

template <typename TFactory, ESortOrder SortOrder>
class TAggregateWindowValue: public TComputationValue<TAggregateWindowValue<TFactory, SortOrder>>, public TQueueResourceUser {
public:
    using TBase = TComputationValue<TAggregateWindowValue<TFactory, SortOrder>>;

    TAggregateWindowValue(TMemoryUsageInfo* memInfo,
                          NUdf::TUnboxedValue&& stream,
                          NUdf::TUnboxedValue&& queue,
                          TStringBuf tag,
                          IComputationNode* resource,
                          const TFactory& factory)
        : TBase(memInfo)
        , TQueueResourceUser(std::move(tag), resource)
        , Stream(std::move(stream))
        , Queue(std::move(queue))
        , Buffer(TQueueResourceUser::CheckAndGetBuffer(Queue))
        , AggregatedBounds(factory(Buffer,
                                   std::bind(&TAggregateWindowValue::ConsumeStream, this, std::placeholders::_1),
                                   TQueueResourceUser::CheckAndGetFrameBoundsIndices(Queue)))
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
    std::invoke_result_t<TFactory, TSafeCircularBuffer<TUnboxedValue>&, std::function<EConsumeStatus(TUnboxedValue&)>, TFrameBoundsIndices&> AggregatedBounds;
};

template <typename TFactory, ESortOrder SortOrder>
class WinFramesCollector: public TMutableComputationNode<WinFramesCollector<TFactory, SortOrder>>, public TQueueResourceUser {
    typedef TMutableComputationNode<WinFramesCollector> TBaseComputation;

public:
    WinFramesCollector(TComputationMutables& mutables,
                       IComputationNode* stream,
                       const TResourceType* resourceType,
                       IComputationNode* resource,
                       TFactory&& factory)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Stream(stream)
        , Factory(std::move(factory))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TAggregateWindowValue<TFactory, SortOrder>>(Stream->GetValue(ctx), Resource->GetValue(ctx), Tag, Resource, Factory);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Resource);
        this->DependsOn(Stream);
    }

    IComputationNode* const Stream;
    const TFactory Factory;
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

template <ESortOrder SortOrder, typename TStreamType, typename TBoundType, typename StreamScale, typename RangeBoundScale>
IComputationNode* DispatchWinStreamCollectorBasedOnSortedColumn(const TRuntimeNode& paramsNode,
                                                                const TComputationNodeFactoryContext& ctx,
                                                                IComputationNode* stream,
                                                                TResourceType* resourceType,
                                                                IComputationNode* resource,
                                                                ui32 memberIndex,
                                                                StreamScale streamScale,
                                                                [[maybe_unused]] RangeBoundScale boundScale) {
    using TStream = NUdf::TDataType<TStreamType>::TLayout;
    using TScaledStream = decltype(streamScale(TStream{}));
    using TComparator = TRangeComparator<TScaledStream>;

    auto bounds = DeserializeBounds<TScaledStream>(paramsNode, SortOrder);

    auto streamElementGetter = [memberIndex, streamScale](const TUnboxedValuePod& pod) -> TMaybe<TScaledStream> {
        auto structElement = pod.GetElement(memberIndex);
        if (!structElement) {
            return {};
        }
        return std::invoke(streamScale, structElement.Get<TStream>());
    };

    auto factory = TCoreWinFramesCollector<TUnboxedValue, decltype(streamElementGetter), TComparator, SortOrder>::CreateFactory(
        bounds, std::move(streamElementGetter));

    return new WinFramesCollector<decltype(factory), SortOrder>(ctx.Mutables,
                                                                stream,
                                                                resourceType,
                                                                resource,
                                                                std::move(factory));
}

template <typename T>
T NoScale(T elem) {
    return elem;
}

template <ESortOrder SortOrder>
IComputationNode* DispatchWinStreamCollectorBasedOnStreamType(const TRuntimeNode& paramsNode,
                                                              const TComputationNodeFactoryContext& ctx,
                                                              IComputationNode* stream,
                                                              TResourceType* resourceType,
                                                              IComputationNode* resource,
                                                              TType* sortColumnType,
                                                              ui32 memberIndex) {
    bool isOptional;
    sortColumnType = UnpackOptional(sortColumnType, isOptional);

    MKQL_ENSURE(sortColumnType->IsData(), "Expected data type.");
    switch (*AS_TYPE(TDataType, sortColumnType)->GetDataSlot()) {
        case EDataSlot::Int8:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, i8, i8>(paramsNode, ctx, stream, resourceType, resource, memberIndex, NoScale<i8>, NoScale<i8>);
        case EDataSlot::Uint8:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, ui8, ui8>(paramsNode, ctx, stream, resourceType, resource, memberIndex, NoScale<ui8>, NoScale<ui8>);
        case EDataSlot::Int16:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, i16, i16>(paramsNode, ctx, stream, resourceType, resource, memberIndex, NoScale<i16>, NoScale<i16>);
        case EDataSlot::Uint16:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, ui16, ui16>(paramsNode, ctx, stream, resourceType, resource, memberIndex, NoScale<ui16>, NoScale<ui16>);
        case EDataSlot::Int32:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, i32, i32>(paramsNode, ctx, stream, resourceType, resource, memberIndex, NoScale<i32>, NoScale<i32>);
        case EDataSlot::Uint32:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, ui32, ui32>(paramsNode, ctx, stream, resourceType, resource, memberIndex, NoScale<ui32>, NoScale<ui32>);
        case EDataSlot::Int64:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, i64, i64>(paramsNode, ctx, stream, resourceType, resource, memberIndex, NoScale<i64>, NoScale<i64>);
        case EDataSlot::Uint64:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, ui64, ui64>(paramsNode, ctx, stream, resourceType, resource, memberIndex, NoScale<ui64>, NoScale<ui64>);
        case EDataSlot::Double:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, double, double>(paramsNode, ctx, stream, resourceType, resource, memberIndex, NoScale<double>, NoScale<double>);
        case EDataSlot::Float:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, float, float>(paramsNode, ctx, stream, resourceType, resource, memberIndex, NoScale<float>, NoScale<float>);
        case EDataSlot::Date:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TDate, NUdf::TInterval>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TDate>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval>>);
        case EDataSlot::Datetime:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TDatetime, NUdf::TInterval>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TDatetime>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval>>);
        case EDataSlot::Timestamp:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TTimestamp, NUdf::TInterval>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TTimestamp>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval>>);
        case EDataSlot::Interval:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TInterval, NUdf::TInterval>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TInterval>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval>>);
        case EDataSlot::TzDate:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TTzDate, NUdf::TInterval>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TTzDate>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval>>);
        case EDataSlot::TzDatetime:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TTzDatetime, NUdf::TInterval>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval>>);
        case EDataSlot::TzTimestamp:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TTzTimestamp, NUdf::TInterval>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TTzTimestamp>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval>>);
        case EDataSlot::Date32:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TDate32, NUdf::TInterval64>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TDate32>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>);
        case EDataSlot::Datetime64:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TDatetime64, NUdf::TInterval64>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TDatetime64>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>);
        case EDataSlot::Timestamp64:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TTimestamp64, NUdf::TInterval64>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TTimestamp64>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>);
        case EDataSlot::Interval64:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TInterval64, NUdf::TInterval64>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>);
        case EDataSlot::TzDate32:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TTzDate32, NUdf::TInterval64>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TTzDate32>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>);
        case EDataSlot::TzDatetime64:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TTzDatetime64, NUdf::TInterval64>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TTzDatetime64>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>);
        case EDataSlot::TzTimestamp64:
            return DispatchWinStreamCollectorBasedOnSortedColumn<SortOrder, NUdf::TTzTimestamp64, NUdf::TInterval64>(paramsNode, ctx, stream, resourceType, resource, memberIndex, ToScaledDate<NUdf::TDataType<NUdf::TTzTimestamp64>>, ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>);
        default:
            MKQL_ENSURE(false, "Unexpected type for window collecting.");
            return nullptr;
    }
}

IComputationNode* DispatchWinStreamCollectorBasedOnOrderedColumn(const TRuntimeNode& paramsNode,
                                                                 const TComputationNodeFactoryContext& ctx,
                                                                 TType* streamType,
                                                                 IComputationNode* stream,
                                                                 TResourceType* resourceType,
                                                                 IComputationNode* resource) {
    auto sortOrder = DeserializeSortOrder(paramsNode);
    auto sortColumnName = DeserializeSortColumnName(paramsNode);

    if (!AnyRangeProvided(paramsNode)) {
        auto bounds = DeserializeBounds<ui64>(paramsNode, ESortOrder::Unimportant);
        MKQL_ENSURE(bounds.RangeIntervals().empty() && bounds.RangeIncrementals().empty(), "Unexpected bounds.");
        // TODO(atarasov5): Remove the fake getter in favor of explicitly specifying an void template.
        auto elementGetter = [](const TUnboxedValue&) -> TMaybe<ui64> {
            MKQL_ENSURE(0, "Shouldn't be called.");
            return ui64(0);
        };

        using TComparator = TRangeComparator<ui64>;
        auto factory = TCoreWinFramesCollector<TUnboxedValue, decltype(elementGetter), TComparator, ESortOrder::Unimportant>::CreateFactory(
            bounds, std::move(elementGetter));
        return new WinFramesCollector<decltype(factory), ESortOrder::Unimportant>(ctx.Mutables,
                                                                                  stream,
                                                                                  resourceType,
                                                                                  resource,
                                                                                  std::move(factory));
    }

    MKQL_ENSURE(streamType->IsStream(), "Expected stream type.");
    auto streamItemType = AS_TYPE(TStreamType, streamType)->GetItemType();
    MKQL_ENSURE(streamItemType->IsStruct(), "Expected stream of struct type.");
    auto structType = AS_TYPE(TStructType, streamItemType);

    auto memberIndex = structType->FindMemberIndex(sortColumnName);
    MKQL_ENSURE(memberIndex, "Stream struct must have a field named '" << sortColumnName << "' (params.SortedColumn)");

    auto sortColumnType = structType->GetMemberType(*memberIndex);

    switch (sortOrder) {
        case ESortOrder::Asc:
            return DispatchWinStreamCollectorBasedOnStreamType<ESortOrder::Asc>(paramsNode, ctx, stream, resourceType, resource, sortColumnType, *memberIndex);
        case ESortOrder::Desc:
            return DispatchWinStreamCollectorBasedOnStreamType<ESortOrder::Desc>(paramsNode, ctx, stream, resourceType, resource, sortColumnType, *memberIndex);
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

    // Instantiate the correct template specialization based on runtime values
    if (isRange) {
        if (IsIncremental) {
            if (isSingleElement) {
                return MakeNodeWithDeps<TWinFrame</*IsRange=*/true, /*IsIncremental=*/true, /*ReturnSingleElement=*/true>>(callable, ctx, reqArgs, resourceType, resource, handle);
            } else {
                return MakeNodeWithDeps<TWinFrame</*IsRange=*/true, /*IsIncremental=*/true, /*ReturnSingleElement=*/false>>(callable, ctx, reqArgs, resourceType, resource, handle);
            }
        } else {
            if (isSingleElement) {
                return MakeNodeWithDeps<TWinFrame</*IsRange=*/true, /*IsIncremental=*/false, /*ReturnSingleElement=*/true>>(callable, ctx, reqArgs, resourceType, resource, handle);
            } else {
                return MakeNodeWithDeps<TWinFrame</*IsRange=*/true, /*IsIncremental=*/false, /*ReturnSingleElement=*/false>>(callable, ctx, reqArgs, resourceType, resource, handle);
            }
        }
    } else {
        if (IsIncremental) {
            if (isSingleElement) {
                return MakeNodeWithDeps<TWinFrame</*IsRange=*/false, /*IsIncremental=*/true, /*ReturnSingleElement=*/true>>(callable, ctx, reqArgs, resourceType, resource, handle);
            } else {
                return MakeNodeWithDeps<TWinFrame</*IsRange=*/false, /*IsIncremental=*/true, /*ReturnSingleElement=*/false>>(callable, ctx, reqArgs, resourceType, resource, handle);
            }
        } else {
            if (isSingleElement) {
                return MakeNodeWithDeps<TWinFrame</*IsRange=*/false, /*IsIncremental=*/false, /*ReturnSingleElement=*/true>>(callable, ctx, reqArgs, resourceType, resource, handle);
            } else {
                return MakeNodeWithDeps<TWinFrame</*IsRange=*/false, /*IsIncremental=*/false, /*ReturnSingleElement=*/false>>(callable, ctx, reqArgs, resourceType, resource, handle);
            }
        }
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
