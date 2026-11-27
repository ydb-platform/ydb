#include <ydb/core/kqp/runtime/common/kqp_compute_context.h>
#include <ydb/core/kqp/runtime/common/kqp_remote_embedder_stage.h>
#include <ydb/library/actors/core/coro_stack_pool.h>
#include <ydb/library/actors/core/scheduler_cookie.h>
#include <ydb/library/yql/dq/actors/compute/events/events.h>
#include <ydb/library/yql/public/udf/udf_async_waiter.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <util/system/context.h>

#include <library/cpp/threading/future/future.h>

#include <algorithm>
#include <exception>
#include <memory>
#include <mutex>
#include <utility>

namespace NKikimr::NMiniKQL {
namespace {

using namespace NYql::NDq;
using NActors::TActivationContext;
using NMonotonic::TMonotonic;
using NUdf::EFetchStatus;

NUdf::TUnboxedValue Materialize(const TType* type, NUdf::TUnboxedValue value, const THolderFactory& factory) {
    switch (type->GetKind()) {
        case TType::EKind::List:
        case TType::EKind::Dict: {
            TValuePacker packer(false, type);
            return packer.Unpack(packer.Pack(value), factory);
        }
        case TType::EKind::Optional:
            if (value) {
                auto item = Materialize(static_cast<const TOptionalType*>(type)->GetItemType(), value.GetOptionalValue(), factory);
                return item.Release().MakeOptional();
            }
            return value;
        case TType::EKind::Tagged:
            return Materialize(static_cast<const TTaggedType*>(type)->GetBaseType(), std::move(value), factory);
        case TType::EKind::Variant: {
            const auto index = value.GetVariantIndex();
            auto item = Materialize(static_cast<const TVariantType*>(type)->GetAlternativeType(index), value.GetVariantItem(), factory);
            return factory.CreateVariantHolder(item.Release(), index);
        }
        case TType::EKind::Struct:
        case TType::EKind::Tuple: {
            const auto* structure = type->IsStruct() ? static_cast<const TStructType*>(type) : nullptr;
            const auto* tuple = type->IsTuple() ? static_cast<const TTupleType*>(type) : nullptr;
            const auto count = structure ? structure->GetMembersCount() : tuple->GetElementsCount();
            NUdf::TUnboxedValue* fields;
            auto row = NUdf::TUnboxedValue(factory.CreateDirectArrayHolder(count, fields));
            for (ui32 i = 0; i < count; ++i) {
                const auto* fieldType = structure ? structure->GetMemberType(i) : tuple->GetElementType(i);
                fields[i] = Materialize(fieldType, value.GetElement(i), factory);
            }
            return row;
        }
        default:
            return value;
    }
}

class TEmbedderStageStream final : public TComputationValue<TEmbedderStageStream>,
    private NUdf::IAsyncUdfWaiter, private ITrampoLine {
    struct TWakeup {
        std::mutex Mutex;
        std::function<void()> Callback;
    };

    struct TAbandoned {};

public:
    TEmbedderStageStream(TMemoryUsageInfo* memInfo, TComputationContext& ctx,
        TKqpComputeContextBase& computeCtx, IComputationNode* input, const TType* itemType)
        : TComputationValue<TEmbedderStageStream>(memInfo)
        , Ctx_(ctx)
        , ComputeCtx_(computeCtx)
        , Input_(input)
        , ItemType_(itemType)
        , Wakeup_(std::make_shared<TWakeup>())
        , Stack_(NActors::TStackMemPool::GetMemPool(
            NActors::TStackMemPool::TPageBucket::Bytes(8 * 1024 * 1024))->Allocate())
        , Fiber_(TContClosure{this, {Stack_->Begin(), Stack_->End()}, "KqpRemoteEmbedder"})
    {
        Wakeup_->Callback = ComputeCtx_.GetWakeupCallback();
    }

    ~TEmbedderStageStream() override {
        {
            std::lock_guard lock(Wakeup_->Mutex);
            Wakeup_->Callback = {};
        }
        if (Started_ && !Finished_) {
            Abandoned_ = true;
            Resume();
        }
    }

private:
    EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        const auto status = FetchNext();
        if (status == EFetchStatus::Ok) {
            result = std::move(Row_);
        }
        return status;
    }

    EFetchStatus WideFetch(NUdf::TUnboxedValue* values, ui32 count) override {
        const auto status = FetchNext();
        if (status == EFetchStatus::Ok) {
            Y_ABORT_UNLESS(count == WideRow_.size());
            std::move(WideRow_.begin(), WideRow_.end(), values);
        }
        return status;
    }

    EFetchStatus FetchNext() {
        Resume();
        if (Error_) {
            std::rethrow_exception(Error_);
        }
        if (Waiting_) {
            ComputeCtx_.FlushRemoteEmbedderOutputs();
            throw TKqpRemoteEmbedderPending{};
        }
        return Status_;
    }

    void Resume() {
        Y_ABORT_UNLESS(!Caller_ && !Finished_);
        TExceptionSafeContext caller;
        Caller_ = &caller;
        auto* previous = std::exchange(Current, static_cast<NUdf::IAsyncUdfWaiter*>(this));
        Started_ = true;
        caller.SwitchTo(&Fiber_);
        Current = previous;
        Caller_ = nullptr;
    }

    void Suspend() {
        Fiber_.SwitchTo(Caller_);
        if (Abandoned_) {
            throw TAbandoned{};
        }
    }

    void DoRun() override {
        try {
            // Defer evaluation of the input itself as it may call a scalar UDF.
            auto input = Input_->GetValue(Ctx_);
            const auto* multi = ItemType_->IsMulti() ? static_cast<const TMultiType*>(ItemType_) : nullptr;
            if (multi) {
                WideRow_.resize(multi->GetElementsCount());
            }
            for (;;) {
                if (multi) {
                    Status_ = input.WideFetch(WideRow_.data(), WideRow_.size());
                    if (Status_ == EFetchStatus::Ok) {
                        for (ui32 i = 0; i < WideRow_.size(); ++i) {
                            WideRow_[i] = Materialize(multi->GetElementType(i), std::move(WideRow_[i]), Ctx_.HolderFactory);
                        }
                    }
                } else {
                    Status_ = input.Fetch(Row_);
                    if (Status_ == EFetchStatus::Ok) {
                        Row_ = Materialize(ItemType_, std::move(Row_), Ctx_.HolderFactory);
                    }
                }
                Suspend();
            }
        } catch (const TAbandoned&) {
        } catch (...) {
            Error_ = std::current_exception();
        }
        Finished_ = true;
        Fiber_.SwitchTo(Caller_);
        Y_ABORT("Resumed a finished remote embedder stage");
    }

    bool Wait(const NThreading::TFuture<void>& ready, TMonotonic deadline) override {
        if (ready.IsReady()) {
            return true;
        }
        ready.Subscribe([wakeup = std::weak_ptr(Wakeup_)](const auto&) {
            if (auto state = wakeup.lock()) {
                std::lock_guard lock(state->Mutex);
                if (state->Callback) {
                    state->Callback();
                }
            }
        });

        while (!ready.IsReady()) {
            const auto now = TMonotonic::Now();
            if (now >= deadline) {
                return false;
            }
            if (!Timer_.Get() || !Timer_.Get()->IsArmed()) {
                Timer_.Reset(NActors::ISchedulerCookie::Make2Way());
                TActivationContext::Schedule(deadline - now, std::make_unique<NActors::IEventHandle>(
                    ComputeCtx_.GetRemoteEmbedderWakeupActor(), NActors::TActorId{},
                    new TEvDqCompute::TEvResumeExecution(EResumeSource::CAWakeupCallback)), Timer_.Get());
            }
            Waiting_ = true;
            Suspend();
            Waiting_ = false;
        }
        return true;
    }

    TComputationContext& Ctx_;
    TKqpComputeContextBase& ComputeCtx_;
    IComputationNode* const Input_;
    const TType* const ItemType_;
    std::shared_ptr<TWakeup> Wakeup_;
    NActors::TSchedulerCookieHolder Timer_;
    std::unique_ptr<NActors::IStackMem> Stack_;
    TExceptionSafeContext Fiber_;
    TExceptionSafeContext* Caller_ = nullptr;
    std::exception_ptr Error_;
    EFetchStatus Status_ = EFetchStatus::Yield;
    NUdf::TUnboxedValue Row_;
    TUnboxedValueVector WideRow_;
    bool Started_ = false;
    bool Finished_ = false;
    bool Abandoned_ = false;
    bool Waiting_ = false;
};

class TEmbedderStageNode final : public TMutableComputationNode<TEmbedderStageNode> {
public:
    TEmbedderStageNode(TComputationMutables& mutables, TKqpComputeContextBase& computeCtx,
        IComputationNode* input, const TType* itemType)
        : TMutableComputationNode<TEmbedderStageNode>(mutables, EValueRepresentation::Boxed)
        , ComputeCtx_(computeCtx)
        , Input_(input)
        , ItemType_(itemType)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TEmbedderStageStream>(ctx, ComputeCtx_, Input_, ItemType_);
    }

private:
    bool IsSuitableForCache() const final { return false; }
    void RegisterDependencies() const final { DependsOn(Input_); }

    TKqpComputeContextBase& ComputeCtx_;
    IComputationNode* const Input_;
    const TType* const ItemType_;
};

} // namespace

IComputationNode* WrapKqpRemoteEmbedderStage(TCallable& callable,
    const TComputationNodeFactoryContext& ctx, TKqpComputeContextBase& computeCtx)
{
    MKQL_ENSURE(callable.GetInputsCount() == 1, "KqpRemoteEmbedderStage expects one argument");
    const auto* type = AS_TYPE(TStreamType, callable.GetInput(0).GetStaticType());
    computeCtx.EnableRemoteEmbedderStage();
    return new TEmbedderStageNode(ctx.Mutables, computeCtx, LocateNode(ctx.NodeLocator, callable, 0), type->GetItemType());
}

} // namespace NKikimr::NMiniKQL
