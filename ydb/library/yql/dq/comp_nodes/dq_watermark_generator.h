#pragma once

namespace NYql::NDq {
struct TPartitionKey;
template <typename TPartitionKey>
class TDqSourceWatermarkTracker;
} // namespace NYql::NDq

namespace NKikimr::NMiniKQL {

class IComputationNode;
class TCallable;
struct TComputationNodeFactoryContext;
struct TWatermark;

IComputationNode* WrapDqWatermarkGenerator(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx,
    TWatermark& watermark,
    NYql::NDq::TDqSourceWatermarkTracker<NYql::NDq::TPartitionKey>* watermarkTracker
);

} // namespace NKikimr::NMiniKQL
