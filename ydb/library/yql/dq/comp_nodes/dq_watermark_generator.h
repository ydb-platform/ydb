#pragma once

namespace NYql::NDq {
class TDqWatermarkGeneratorTracker;
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
    NYql::NDq::TDqWatermarkGeneratorTracker* watermarkTracker
);

} // namespace NKikimr::NMiniKQL
