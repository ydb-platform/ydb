#include "dq_watermark_generator.h"

#include <ydb/library/yql/dq/runtime/streaming/dq_source_watermark_tracker.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_watermark.h>

#include <util/datetime/base.h>

namespace NKikimr::NMiniKQL {

namespace {

class TDqWatermarkGenerator : public TStatefulSourceComputationNode<TDqWatermarkGenerator/* , true */> {
    using TSelf = TDqWatermarkGenerator;
    using TBaseComputation = TStatefulSourceComputationNode<TSelf/* , true */>;
public:
    using TPartitionKey = ui64;

    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(
            TMemoryUsageInfo* memInfo,
            NUdf::TUnboxedValue&& stream,
            const TSelf* self,
            TComputationContext& ctx
        )
            : TBase(memInfo)
            , Stream_(std::move(stream))
            , Self_(self)
            , Ctx_(ctx)
        {
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            NUdf::TUnboxedValue value;
            const auto status = Stream_.Fetch(value);
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            auto newValue = value;
            Self_->ItemArg_->SetValue(Ctx_, std::move(newValue));

            const auto watermarkValue = Self_->WatermarkExtractor_->GetValue(Ctx_);
            if (watermarkValue) {
                const auto watermark = TInstant::MicroSeconds(watermarkValue.Get<ui64>());

                const auto partitionId = Self_->PartitionIdExtractor_->GetValue(Ctx_).Get<ui64>();
                const auto partitionKey = TPartitionKey{partitionId};

                const auto now = TInstant::Now();
                if (const auto newWatermark = Self_->WatermarkTracker_.NotifyNewPartitionTime(partitionKey, watermark, now)) {
                    Self_->Watermark_.WatermarkIn = newWatermark;
                }
            }

            result = value.Release();
            return NUdf::EFetchStatus::Ok;
        }

    private:
        const NUdf::TUnboxedValue Stream_;
        const TSelf* const Self_;
        TComputationContext& Ctx_;
    };

    TDqWatermarkGenerator(
        TComputationMutables& mutables,
        IComputationNode* input,
        IComputationExternalNode* itemArg,
        IComputationNode* watermarkExtractor,
        IComputationNode* partitionIdExtractor,
        std::vector<TPartitionKey> partitions,
        TDuration /* lateArrivalDelay */,
        TDuration granularity,
        TDuration idleTimeout,
        TWatermark& watermark
    )
        : TBaseComputation(mutables)
        , Input_(input)
        , ItemArg_(itemArg)
        , WatermarkExtractor_(watermarkExtractor)
        , PartitionIdExtractor_(partitionIdExtractor)
        , WatermarkTracker_(
            granularity,
            true,
            TDuration::Zero(),
            idleTimeout,
            "TDqWatermarkGenerator"
        )
        , Watermark_(watermark)
    {
        const auto now = TInstant::Now();
        for (const auto& partition : partitions) {
            WatermarkTracker_.RegisterPartition(partition, now);
        }
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& ctx) const override {
        NUdf::TUnboxedValue& valueRef = ValueRef(ctx);
        if (valueRef.IsInvalid()) {
            // Create new.
            valueRef = ctx.HolderFactory.Create<TStreamValue>(Input_->GetValue(ctx), this, ctx);
        } else if (valueRef.HasValue()) {
            MKQL_ENSURE(valueRef.IsBoxed(), "Expected boxed value");
            bool isStateToLoad = valueRef.HasListItems();
            if (isStateToLoad) {
                // Load from saved state.
                NUdf::TUnboxedValue stream = ctx.HolderFactory.Create<TStreamValue>(Input_->GetValue(ctx), this, ctx);
                stream.Load2(valueRef);
                valueRef = stream;
            }
        }

        return valueRef;
    }

private:
    void RegisterDependencies() const override {
        DependsOn(Input_);
        Own(ItemArg_);
        DependsOn(WatermarkExtractor_);
        DependsOn(PartitionIdExtractor_);
    }

private:
    IComputationNode* const Input_;
    IComputationExternalNode* const ItemArg_;
    IComputationNode* const WatermarkExtractor_;
    IComputationNode* const PartitionIdExtractor_;

    mutable NYql::NDq::TDqSourceWatermarkTracker<TPartitionKey> WatermarkTracker_;
    TWatermark& Watermark_;
};

} // anonymous namespace

IComputationNode* WrapDqWatermarkGenerator(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx,
    TWatermark& watermark
) {
    auto streamType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(streamType->IsStream(), "Expected stream");

    auto input = LocateNode(ctx.NodeLocator, callable, 0);
    auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    auto watermarkExtractor = LocateNode(ctx.NodeLocator, callable, 2);
    auto partitionIdExtractor = LocateNode(ctx.NodeLocator, callable, 3);
    auto watermarkSettings = AS_VALUE(TListLiteral, callable.GetInput(4));

    auto lateArrivalDelay = TDuration::Seconds(5);
    auto granularity = TDuration::Seconds(1);
    auto idleTimeout = TDuration::Seconds(5);
    for (ui32 i = 0; i + 2 <= watermarkSettings->GetItemsCount(); i += 2) {
        const auto  name = AS_VALUE(TDataLiteral, watermarkSettings->GetItems()[i + 0])->AsValue().AsStringRef();
        const auto value = AS_VALUE(TDataLiteral, watermarkSettings->GetItems()[i + 1])->AsValue().AsStringRef();

        if ("WatermarksGranularityUs" == std::string_view{name}) {
            granularity = TDuration::MicroSeconds(FromString<ui64>(value));
        } else if ("WatermarksLateArrivalDelayUs" == std::string_view{name}) {
            lateArrivalDelay = TDuration::MicroSeconds(FromString<ui64>(value));
        } else if ("WatermarksIdleTimeoutUs" == std::string_view{name}) {
            idleTimeout = TDuration::MicroSeconds(FromString<ui64>(value));
        }
    }

    return new TDqWatermarkGenerator(
        ctx.Mutables,
        input,
        itemArg,
        watermarkExtractor,
        partitionIdExtractor,
        {0},
        lateArrivalDelay,
        granularity,
        idleTimeout,
        watermark
    );
}

} // namespace NKikimr::NMiniKQL
