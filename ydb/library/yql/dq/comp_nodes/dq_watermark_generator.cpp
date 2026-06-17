#include "dq_watermark_generator.h"

#include <ydb/library/yql/dq/runtime/streaming/dq_source_watermark_tracker.h>
#include <ydb/library/yql/dq/runtime/streaming/partition_key.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_watermark.h>

#include <util/datetime/base.h>
#include <util/string/split.h>

namespace NKikimr::NMiniKQL {

namespace {

const TStatKey WatermarkGenerator_EmptyTimeCount("WatermarkGenerator_EmptyTimeCount", true);

using NYql::NDq::TPartitionKey;
using TDqSourceWatermarkTracker = NYql::NDq::TDqSourceWatermarkTracker<TPartitionKey>;

class TDqWatermarkGenerator : public TStatefulSourceComputationNode<TDqWatermarkGenerator/* , true */> {
    using TSelf = TDqWatermarkGenerator;
    using TBaseComputation = TStatefulSourceComputationNode<TSelf/* , true */>;
public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(
            TMemoryUsageInfo* memInfo,
            NUdf::TUnboxedValue&& stream,
            const TSelf* self,
            std::vector<TPartitionKey> partitionKeys,
            TComputationContext& ctx
        )
            : TBase(memInfo)
            , Stream_(std::move(stream))
            , Self_(self)
            , Ctx_(ctx)
        {
            if (Self_->WatermarkTracker_) {
                const auto now = TInstant::Now();
                for (const auto& partitionKey : partitionKeys) {
                    Self_->WatermarkTracker_->RegisterPartition(partitionKey, now);
                }
            }
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            i64 emptyTimeCountStat = 0;

            Y_DEFER {
                MKQL_ADD_STAT(Ctx_.Stats, WatermarkGenerator_EmptyTimeCount, emptyTimeCountStat);
            };

            NUdf::TUnboxedValue value;
            const auto status = Stream_.Fetch(value);
            const auto now = TInstant::Now();
            if (Self_->WatermarkTracker_) {
                Self_->WatermarkTracker_->ProcessIdlenessCheck(now);
                if (const auto idleWatermark = Self_->WatermarkTracker_->HandleIdleness(now)) {
                    Self_->Watermark_.WatermarkIn = idleWatermark;
                }
                [[maybe_unused]] const auto nextIdleCheckAt = Self_->WatermarkTracker_->PrepareIdlenessCheck(now);
            }

            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            {
                auto newValue = value;
                Self_->ItemArg_->SetValue(Ctx_, std::move(newValue));
            }

            const auto watermarkValue = Self_->WatermarkExtractor_->GetValue(Ctx_);
            if (watermarkValue) {
                const auto watermark = TInstant::MicroSeconds(watermarkValue.Get<ui64>());

                const auto partitionKeyValue = Self_->PartitionKeyExtractor_->GetValue(Ctx_);

                const auto clusterValue = partitionKeyValue.GetElement(0);
                const auto cluster = std::string(clusterValue.AsStringRef());

                const auto partitionIdValue = partitionKeyValue.GetElement(1);
                const auto partitionId = partitionIdValue.Get<ui64>();

                const auto partitionKey = TPartitionKey{
                    .Cluster = cluster,
                    .PartitionId = partitionId,
                };

                if (Self_->WatermarkTracker_) {
                    if (const auto newWatermark = Self_->WatermarkTracker_->NotifyNewPartitionTime(partitionKey, watermark, now)) {
                        Self_->Watermark_.WatermarkIn = newWatermark;
                    }
                }
            } else {
                ++emptyTimeCountStat;
            }

            result = value;
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
        IComputationNode* partitionKeyExtractor,
        IComputationNode* partitionKeys,
        TDuration lateArrivalDelay,
        TDuration granularity,
        TDuration idleTimeout,
        std::vector<std::string> clusters,
        TWatermark& watermark,
        TDqSourceWatermarkTracker* watermarkTracker
    )
        : TBaseComputation(mutables)
        , Input_(input)
        , ItemArg_(itemArg)
        , WatermarkExtractor_(watermarkExtractor)
        , PartitionKeyExtractor_(partitionKeyExtractor)
        , Partitions_(partitionKeys)
        , LateArrivalDelay_(lateArrivalDelay)
        , Granularity_(granularity)
        , IdleTimeout_(idleTimeout)
        , Clusters_(std::move(clusters))
        , Watermark_(watermark)
        , WatermarkTracker_(watermarkTracker)
    {
    }

    NUdf::TUnboxedValue GetValue(TComputationContext& ctx) const override {
        NUdf::TUnboxedValue& valueRef = ValueRef(ctx);
        if (valueRef.IsInvalid()) {
            // Create new.
            valueRef = CreateStream(ctx);
        } else if (valueRef.HasValue()) {
            MKQL_ENSURE(valueRef.IsBoxed(), "Expected boxed value");
            bool isStateToLoad = valueRef.HasListItems();
            if (isStateToLoad) {
                // Load from saved state.
                NUdf::TUnboxedValue stream = CreateStream(ctx);
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
        DependsOn(PartitionKeyExtractor_);
        DependsOn(Partitions_);
    }

    NUdf::TUnboxedValue CreateStream(TComputationContext& ctx) const {
        auto partitions = ExtractPartitions(Partitions_->GetValue(ctx));

        return ctx.HolderFactory.Create<TStreamValue>(
            Input_->GetValue(ctx),
            this,
            std::move(partitions),
            ctx
        );
    }

    [[nodiscard]] std::vector<TPartitionKey> ExtractPartitions(NUdf::TUnboxedValue partitionsValue) const {
        std::vector<TPartitionKey> partitionKeys;

        for (const auto& cluster : Clusters_) {
            auto partitionsIterator = partitionsValue.GetListIterator();
            NUdf::TUnboxedValue partitionValue;
            while (partitionsIterator.Next(partitionValue)) {
                auto partitionId = partitionValue.Get<ui64>();
                partitionKeys.emplace_back(cluster, partitionId);
            }
        }
        return partitionKeys;
    }

private:
    IComputationNode* const Input_;
    IComputationExternalNode* const ItemArg_;
    IComputationNode* const WatermarkExtractor_;
    IComputationNode* const PartitionKeyExtractor_;
    IComputationNode* const Partitions_;

    TDuration LateArrivalDelay_;
    TDuration Granularity_;
    TDuration IdleTimeout_;
    std::vector<std::string> Clusters_;
    TWatermark& Watermark_;
    TDqSourceWatermarkTracker* WatermarkTracker_;
};

} // anonymous namespace

IComputationNode* WrapDqWatermarkGenerator(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx,
    TWatermark& watermark,
    TDqSourceWatermarkTracker* watermarkTracker
) {
    auto streamType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(streamType->IsStream(), "Expected stream");

    auto input = LocateNode(ctx.NodeLocator, callable, 0);
    auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    auto watermarkExtractor = LocateNode(ctx.NodeLocator, callable, 2);
    auto partitionKeyExtractor = LocateNode(ctx.NodeLocator, callable, 3);
    auto watermarkSettings = AS_VALUE(TListLiteral, callable.GetInput(4));
    auto partitionKeys = LocateNode(ctx.NodeLocator, callable, 5);

    auto lateArrivalDelay = TDuration::Seconds(5);
    auto granularity = TDuration::Seconds(1);
    auto idleTimeout = TDuration::Seconds(5);
    std::vector<std::string> clusters;
    for (ui32 i = 0; i + 2 <= watermarkSettings->GetItemsCount(); i += 2) {
        const auto  name = AS_VALUE(TDataLiteral, watermarkSettings->GetItems()[i + 0])->AsValue().AsStringRef();
        const auto value = AS_VALUE(TDataLiteral, watermarkSettings->GetItems()[i + 1])->AsValue().AsStringRef();

        if ("WatermarksGranularityUs" == std::string_view{name}) {
            granularity = TDuration::MicroSeconds(FromString<ui64>(value));
        } else if ("WatermarksLateArrivalDelayUs" == std::string_view{name}) {
            lateArrivalDelay = TDuration::MicroSeconds(FromString<ui64>(value));
        } else if ("WatermarksIdleTimeoutUs" == std::string_view{name}) {
            idleTimeout = TDuration::MicroSeconds(FromString<ui64>(value));
        } else if ("FederatedClusters" == std::string_view{name}) {
            TVector<TStringBuf> clusterList;
            Split(value, ",", clusterList);
            for (const auto& cluster : clusterList) {
                clusters.emplace_back(cluster);
            }
        }
    }
    if (clusters.empty()) {
        clusters.emplace_back();
    }

    if (watermarkTracker) {
        watermarkTracker->SetSettings(granularity, true, TDuration::Zero(), idleTimeout);
    }

    return new TDqWatermarkGenerator(
        ctx.Mutables,
        input,
        itemArg,
        watermarkExtractor,
        partitionKeyExtractor,
        partitionKeys,
        lateArrivalDelay,
        granularity,
        idleTimeout,
        clusters,
        watermark,
        watermarkTracker
    );
}

} // namespace NKikimr::NMiniKQL
