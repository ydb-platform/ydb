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

#include <utility>

namespace NKikimr {
using namespace NUdf;
namespace NMiniKQL {

namespace {

class TQueueResource: public TComputationValue<TQueueResource> {
public:
    TQueueResource(TMemoryUsageInfo* memInfo, const TStringBuf& tag, TMaybe<ui64> capacity, ui64 initSize)
        : TComputationValue(memInfo)
        , ResourceTag_(tag)
        , Buffer_(capacity, TUnboxedValue(), initSize)
        , BufferBytes_(CurrentMemUsage())
    {
    }

    ~TQueueResource() override {
        Buffer_.Clear();
    }

    void UpdateBufferStats() {
        BufferBytes_ = CurrentMemUsage();
    }

    TSafeCircularBuffer<TUnboxedValue>& GetBuffer() {
        return Buffer_;
    }

    const TFrameBoundsIndices& GetFrameBoundsIndices() const {
        return FrameBoundsIndices_;
    }

    TFrameBoundsIndices& GetFrameBoundsIndices() {
        return FrameBoundsIndices_;
    }

private:
    NUdf::TStringRef GetResourceTag() const override {
        return NUdf::TStringRef(ResourceTag_);
    }

    void* GetResource() override {
        return this;
    }

    size_t CurrentMemUsage() const {
        return Buffer_.Capacity() * sizeof(TUnboxedValue);
    }

    const TStringBuf ResourceTag_;
    TSafeCircularBuffer<TUnboxedValue> Buffer_;
    TFrameBoundsIndices FrameBoundsIndices_;
    size_t BufferBytes_;
};

class TQueueResourceUser {
public:
    TQueueResourceUser(TStringBuf&& tag, IComputationNode* resource);
    TSafeCircularBuffer<NUdf::TUnboxedValue>& CheckAndGetBuffer(const NUdf::TUnboxedValuePod& resource) const;
    TFrameBoundsIndices& CheckAndGetFrameBoundsIndices(const NUdf::TUnboxedValuePod& resource);
    const TFrameBoundsIndices& CheckAndGetFrameBoundsIndices(const NUdf::TUnboxedValuePod& resource) const;
    void UpdateBufferStats(const NUdf::TUnboxedValuePod& resource) const;

protected:
    const TStringBuf Tag_;
    IComputationNode* const Resource_;

    TQueueResource& GetResource(const NUdf::TUnboxedValuePod& resource) const;
};

TQueueResourceUser::TQueueResourceUser(TStringBuf&& tag, IComputationNode* resource)
    : Tag_(tag)
    , Resource_(resource)
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
    Y_DEBUG_ABORT_UNLESS(tag == Tag_, "Expected correct Queue resource");
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
            , Queue_(queue)
            , Buffer_(CheckAndGetBuffer(queue))
            , Current_(begin)
            , End_(end)
            , Generation_(generation)
        {
        }

    private:
        bool Next(NUdf::TUnboxedValue& value) override {
            MKQL_ENSURE(Generation_ == Buffer_.Generation(),
                        "Queue generation changed while doing QueueRange: expected " << Generation_ << ", got: " << Buffer_.Generation());
            if (Current_ >= End_) {
                return false;
            }

            const auto& valRef = Buffer_.Get(Current_++);
            value = !valRef ? NUdf::TUnboxedValuePod() : valRef.MakeOptional();
            return true;
        }

        bool Skip() override {
            if (Current_ >= End_) {
                return false;
            }
            Current_++;
            return true;
        }

        const TUnboxedValue Queue_;
        const TSafeCircularBuffer<TUnboxedValue>& Buffer_;
        size_t Current_;
        const size_t End_;
        const ui64 Generation_;
    };

    TQueueRange(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, TUnboxedValue queue, size_t begin, size_t end, TStringBuf tag, IComputationNode* resource)
        : TComputationValue<TQueueRange<AlwaysExist>>(memInfo)
        , TQueueResourceUser(std::move(tag), resource)
        , CompCtx_(compCtx)
        , Queue_(std::move(queue))
        , Begin_(begin)
        , End_(std::min(end, CheckAndGetBuffer(Queue_).Size()))
        , Generation_(CheckAndGetBuffer(Queue_).Generation())
    {
    }

private:
    ui64 GetListLength() const final {
        return Begin_ < End_ ? (End_ - Begin_) : 0;
    }

    bool HasListItems() const final {
        return GetListLength() != 0;
    }

    bool HasFastListLength() const final {
        return true;
    }

    NUdf::TUnboxedValue GetListIterator() const final {
        return CompCtx_.HolderFactory.Create<TIterator>(Queue_, Begin_, End_, Generation_, Tag_, Resource_);
    }

    TComputationContext& CompCtx_;
    const TUnboxedValue Queue_;
    const size_t Begin_;
    const size_t End_;
    const ui64 Generation_;
};

class TQueueCreateWrapper: public TMutableComputationNode<TQueueCreateWrapper> {
    using TBaseComputation = TMutableComputationNode<TQueueCreateWrapper>;

public:
    TQueueCreateWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, TString name, TMaybe<ui64> capacity, ui64 initSize)
        : TBaseComputation(mutables)
        , DependentNodes_(std::move(dependentNodes))
        , Name_(std::move(name))
        , Capacity_(capacity)
        , InitSize_(initSize)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return NUdf::TUnboxedValuePod(new TQueueResource(&ctx.HolderFactory.GetMemInfo(), Name_, Capacity_, InitSize_));
    }

private:
    void RegisterDependencies() const final {
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&TQueueCreateWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector DependentNodes_;
    const TString Name_;
    const TMaybe<ui64> Capacity_;
    const ui64 InitSize_;
};

class TQueuePushWrapper: public TMutableComputationNode<TQueuePushWrapper>, public TQueueResourceUser {
    using TBaseComputation = TMutableComputationNode<TQueuePushWrapper>;

public:
    TQueuePushWrapper(TComputationMutables& mutables, const TResourceType* resourceType, IComputationNode* resource, IComputationNode* value)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Value_(value)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto resource = Resource_->GetValue(ctx);
        auto& buffer = CheckAndGetBuffer(resource);
        buffer.PushBack(Value_->GetValue(ctx));
        if (buffer.IsUnbounded()) {
            UpdateBufferStats(resource);
        }
        return resource.Release();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource_);
        DependsOn(Value_);
    }

    IComputationNode* const Value_;
};

class TQueuePopWrapper: public TMutableComputationNode<TQueuePopWrapper>, public TQueueResourceUser {
    using TBaseComputation = TMutableComputationNode<TQueuePopWrapper>;

public:
    TQueuePopWrapper(TComputationMutables& mutables, const TResourceType* resourceType, IComputationNode* resource)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto resource = Resource_->GetValue(ctx);
        CheckAndGetBuffer(resource).PopFront();
        return resource.Release();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource_);
    }
};

class TQueuePeekWrapper: public TMutableComputationNode<TQueuePeekWrapper>, public TQueueResourceUser {
    using TBaseComputation = TMutableComputationNode<TQueuePeekWrapper>;

public:
    TQueuePeekWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, const TResourceType* resourceType, IComputationNode* resource, IComputationNode* index)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Index_(index)
        , DependentNodes_(std::move(dependentNodes))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto resource = Resource_->GetValue(ctx);
        auto index = Index_->GetValue(ctx);
        const auto& valRef = CheckAndGetBuffer(resource).Get(index.Get<ui64>());
        return !valRef ? NUdf::TUnboxedValuePod() : valRef.MakeOptional();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource_);
        DependsOn(Index_);
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&TQueuePeekWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Index_;
    const TComputationNodePtrVector DependentNodes_;
};

class TQueueRangeWrapper: public TMutableComputationNode<TQueueRangeWrapper>, public TQueueResourceUser {
    using TBaseComputation = TMutableComputationNode<TQueueRangeWrapper>;

public:
    TQueueRangeWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, const TResourceType* resourceType, IComputationNode* resource,
                       IComputationNode* begin, IComputationNode* end)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Begin_(begin)
        , End_(end)
        , DependentNodes_(std::move(dependentNodes))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto queue = Resource_->GetValue(ctx);

        auto begin = Begin_->GetValue(ctx).Get<ui64>();
        auto end = End_->GetValue(ctx).Get<ui64>();

        return ctx.HolderFactory.Create<TQueueRange</*AlwaysExist=*/false>>(ctx, queue, begin, end, Tag_, Resource_);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource_);
        DependsOn(Begin_);
        DependsOn(End_);
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&TQueueRangeWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Begin_;
    IComputationNode* const End_;
    const TComputationNodePtrVector DependentNodes_;
};

class TPreserveStreamValue: public TComputationValue<TPreserveStreamValue>, public TQueueResourceUser {
public:
    using TBase = TComputationValue<TPreserveStreamValue>;

    TPreserveStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream, NUdf::TUnboxedValue&& queue, TStringBuf tag, IComputationNode* resource, ui64 outpace)
        : TBase(memInfo)
        , TQueueResourceUser(std::move(tag), resource)
        , Stream_(std::move(stream))
        , Queue_(std::move(queue))
        , OutpaceGoal_(outpace)
        , Buffer_(CheckAndGetBuffer(Queue_))
        , FrontIndex_(Buffer_.Size())
    {
    }

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value) override {
        switch (State_) {
            case EPreserveState::Done:
                return NUdf::EFetchStatus::Finish;
            case EPreserveState::Feed:
            case EPreserveState::Yield:
                break;
            default:
                Y_ABORT_UNLESS(Outpace_ > 0);
                Buffer_.PopFront();
                --Outpace_;
        }
        for (NUdf::TUnboxedValue item; State_ != EPreserveState::Emit && Outpace_ <= OutpaceGoal_;) {
            switch (Stream_.Fetch(item)) {
                case NUdf::EFetchStatus::Yield:
                    State_ = EPreserveState::Yield;
                    return NUdf::EFetchStatus::Yield;
                case NUdf::EFetchStatus::Finish:
                    State_ = EPreserveState::Emit;
                    break;
                case NUdf::EFetchStatus::Ok:
                    Buffer_.PushBack(std::move(item));
                    if (Buffer_.IsUnbounded()) {
                        UpdateBufferStats(Queue_);
                    }
                    ++Outpace_;
                    if (Outpace_ > OutpaceGoal_) {
                        State_ = EPreserveState::GoOn;
                    } else {
                        State_ = EPreserveState::Feed;
                    }
            }
        }
        if (!Outpace_) {
            Buffer_.Clean();
            State_ = EPreserveState::Done;
            return NUdf::EFetchStatus::Finish;
        }
        value = Buffer_.Get(FrontIndex_);
        return NUdf::EFetchStatus::Ok;
    }

    enum class EPreserveState {
        Feed,
        GoOn,
        Yield,
        Emit,
        Done
    };
    const NUdf::TUnboxedValue Stream_;
    const NUdf::TUnboxedValue Queue_;
    const ui64 OutpaceGoal_;
    TSafeCircularBuffer<TUnboxedValue>& Buffer_;
    const size_t FrontIndex_;

    EPreserveState State_ = EPreserveState::Feed;
    ui64 Outpace_ = 0;
};

class TPreserveStreamWrapper: public TMutableComputationNode<TPreserveStreamWrapper>, public TQueueResourceUser {
    using TBaseComputation = TMutableComputationNode<TPreserveStreamWrapper>;

public:
    TPreserveStreamWrapper(TComputationMutables& mutables, IComputationNode* stream, const TResourceType* resourceType, IComputationNode* resource, ui64 outpace)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Stream_(stream)
        , Outpace_(outpace)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TPreserveStreamValue>(Stream_->GetValue(ctx), Resource_->GetValue(ctx), Tag_, Resource_, Outpace_);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Resource_);
        DependsOn(Stream_);
    }

    IComputationNode* const Stream_;
    const ui64 Outpace_;
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
        , Stream_(std::move(stream))
        , Queue_(std::move(queue))
        , Buffer_(TQueueResourceUser::CheckAndGetBuffer(Queue_))
        , AggregatedBounds_(factory(Buffer_,
                                    std::bind(&TAggregateWindowValue::ConsumeStream, this, std::placeholders::_1),
                                    TQueueResourceUser::CheckAndGetFrameBoundsIndices(Queue_),
                                    ctx))
    {
    }

private:
    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value) override {
        switch (AggregatedBounds_.Next()) {
            case EConsumeStatus::Ok:
                value = AggregatedBounds_.GetCurrentElement();
                return NUdf::EFetchStatus::Ok;
            case EConsumeStatus::Wait:
                return NUdf::EFetchStatus::Yield;
            case EConsumeStatus::End:
                if (!Cleaned_) {
                    AggregatedBounds_.Clean();
                    Cleaned_ = true;
                }
                return NUdf::EFetchStatus::Finish;
        }
    }

    EConsumeStatus ConsumeStream(TUnboxedValue& value) {
        switch (Stream_.Fetch(value)) {
            case EFetchStatus::Ok:
                return EConsumeStatus::Ok;
            case EFetchStatus::Finish:
                return EConsumeStatus::End;
            case EFetchStatus::Yield:
                return EConsumeStatus::Wait;
        }
    }

    const NUdf::TUnboxedValue Stream_;
    const NUdf::TUnboxedValue Queue_;
    TSafeCircularBuffer<TUnboxedValue>& Buffer_;
    bool Cleaned_ = false;
    std::invoke_result_t<TFactory, TSafeCircularBuffer<TUnboxedValue>&, std::function<EConsumeStatus(TUnboxedValue&)>, TFrameBoundsIndices&, TComputationContext&> AggregatedBounds_;
};

template <typename TFactory, bool IsRangeSupported>
class WinFramesCollector: public TMutableComputationNode<WinFramesCollector<TFactory, IsRangeSupported>>, public TQueueResourceUser {
    using TBaseComputation = TMutableComputationNode<WinFramesCollector>;

public:
    WinFramesCollector(TComputationMutables& mutables,
                       IComputationNode* stream,
                       const TResourceType* resourceType,
                       IComputationNode* resource,
                       TFactory&& factory,
                       const std::vector<IComputationNode*>& dependentNodes)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Stream_(stream)
        , Factory_(std::move(factory))
        , DependentNodes_(dependentNodes)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TAggregateWindowValue<TFactory, IsRangeSupported>>(Stream_->GetValue(ctx), Resource_->GetValue(ctx), Tag_, Resource_, Factory_, ctx);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Resource_);
        this->DependsOn(Stream_);
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&WinFramesCollector::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Stream_;
    const TFactory Factory_;
    const std::vector<IComputationNode*> DependentNodes_;
};

template <bool IsRange, bool IsIncremental, bool ReturnSingleElement>
class TWinFrame: public TMutableComputationNode<TWinFrame<IsRange, IsIncremental, ReturnSingleElement>>, public TQueueResourceUser {
    using TBaseComputation = TMutableComputationNode<TWinFrame<IsRange, IsIncremental, ReturnSingleElement>>;

public:
    TWinFrame(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes, const TResourceType* resourceType, IComputationNode* resource,
              ui64 handle)
        : TBaseComputation(mutables)
        , TQueueResourceUser(resourceType->GetTag(), resource)
        , Handle_(handle)
        , DependentNodes_(std::move(dependentNodes))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto queue = Resource_->GetValue(ctx);

        auto windows = this->CheckAndGetFrameBoundsIndices(queue);
        auto frame = this->GetWindowFrame(Handle_, windows);
        if constexpr (ReturnSingleElement) {
            if (frame.Size() == 0) {
                return TUnboxedValuePod();
            } else {
                const auto& valRef = CheckAndGetBuffer(queue).Get(frame.Max() - 1);
                return valRef.MakeOptional();
            }
            return CheckAndGetBuffer(queue).Get(frame.Min());
        } else {
            return ctx.HolderFactory.Create<TQueueRange</*AlwaysExist=*/true>>(ctx, queue, frame.Min(), frame.Max(), Tag_, Resource_);
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
        this->DependsOn(Resource_);
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&TWinFrame::DependsOn, this, std::placeholders::_1));
    }

    const ui64 Handle_;
    const TComputationNodePtrVector DependentNodes_;
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
    auto bounds = ConvertBoundsToComparators<TUnboxedValue, TUnboxedValue, TComputationContext>(variantBounds, sortOrder, deserializerContext);

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
        auto bounds = ConvertBoundsToComparators<TUnboxedValue, TUnboxedValue, TComputationContext, TNoopDeserializerContext>(variantBounds, ESortOrder::Unimportant, TNoopDeserializerContext{});
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
