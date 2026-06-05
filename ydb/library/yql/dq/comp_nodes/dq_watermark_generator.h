#pragma once

namespace NKikimr::NMiniKQL {

class IComputationNode;
class TCallable;
struct TComputationNodeFactoryContext;
struct TWatermark;

IComputationNode* WrapDqWatermarkGenerator(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx,
    TWatermark& watermark
);

} // namespace NKikimr::NMiniKQL
