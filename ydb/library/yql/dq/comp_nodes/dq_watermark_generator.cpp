#include "dq_watermark_generator.h"

#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>
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

class TDqWatermarkGenerator : public TMutableComputationNode<TDqWatermarkGenerator> {
    using TBaseComputation = TMutableComputationNode<TDqWatermarkGenerator>;
public:
    using TPartitionKey = ui64;

    TDqWatermarkGenerator(
        TComputationMutables& mutables,
        IComputationNode* input,
        IComputationExternalNode* itemArg,
        IComputationNode* watermarkExtractor,
        IComputationNode* partitionIdExtractor,
        std::vector<TPartitionKey> partitions,
        TDuration lateArrivalDelay,
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
            lateArrivalDelay,
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

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& input, TComputationContext& ctx) {
        if (input.IsInvalid()) {
            input = Input_->GetValue(ctx).GetListIterator();
        }

        NUdf::TUnboxedValue value;
        if (!input.Next(value)) {
            return NUdf::TUnboxedValuePod::MakeFinish();
        }

        auto newValue = value;
        ItemArg_->SetValue(ctx, std::move(newValue));

        const auto watermark = TInstant::MicroSeconds(WatermarkExtractor_->GetValue(ctx).Get<ui64>());

        const auto partitionId = PartitionIdExtractor_->GetValue(ctx).Get<ui64>();
        const auto partitionKey = TPartitionKey{partitionId};

        const auto now = TInstant::Now();
        if (const auto newWatermark = WatermarkTracker_.NotifyNewPartitionTime(partitionKey, watermark, now)) {
            Watermark_.WatermarkIn = newWatermark;
        }

        return value.Release();
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

    NYql::NDq::TDqSourceWatermarkTracker<TPartitionKey> WatermarkTracker_;
    TWatermark& Watermark_;
};

} // anonymous namespace

IComputationNode* WrapDqWatermarkGenerator(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx,
    TWatermark& watermark
) {
    auto input = LocateNode(ctx.NodeLocator, callable, 0);
    auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    auto watermarkExtractor = LocateNode(ctx.NodeLocator, callable, 2);
    auto partitionIdExtractor = LocateNode(ctx.NodeLocator, callable, 3);
    auto watermarkSettings = AS_VALUE(TListLiteral, callable.GetInput(4)); // TODO

    auto lateArrivalDelay = TDuration::Zero();
    auto granularity = TDuration::Seconds(1);
    auto idleTimeout = TDuration::Max();
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
        partitions,
        lateArrivalDelay,
        granularity,
        idleTimeout,
        watermark
    );
}

} // namespace NKikimr::NMiniKQL
