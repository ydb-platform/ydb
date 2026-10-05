#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr::NMiniKQL {

class IComputationNode;
class TCallable;
class TKqpComputeContextBase;
struct TComputationNodeFactoryContext;

IComputationNode* WrapKqpStreamingAggregation(TCallable& callable, const TComputationNodeFactoryContext& ctx, const TKqpComputeContextBase& computeCtx);

namespace NPrivate {

TString QuoteStreamingAggregationIdentifier(const TStringBuf& name);

} // namespace NPrivate

} // namespace NKikimr::NMiniKQL
