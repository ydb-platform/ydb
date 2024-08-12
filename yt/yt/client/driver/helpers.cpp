#include "helpers.h"

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NDriver {

using namespace NObjectClient;
using namespace NTracing;

///////////////////////////////////////////////////////////////////////////////

bool operator==(const TEtag& lhs, const TEtag& rhs)
{
    return lhs.Id == rhs.Id && lhs.Revision == rhs.Revision;
}

TErrorOr<TEtag> ParseEtag(TStringBuf etagString)
{
    static const TErrorOr<TEtag> parseError(TError("Failed to parse etag"));

    TStringBuf idString;
    TStringBuf revisionString;
    if (!etagString.TrySplit(':', idString, revisionString)) {
        return parseError;
    }

    TEtag result;

    if (!TObjectId::FromString(idString, &result.Id)) {
        return parseError;
    }

    if (!TryFromString(revisionString, result.Revision)) {
        return parseError;
    }

    return result;
}

void FormatValue(TStringBuilderBase* builder, const TEtag& tag, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v:%v", tag.Id, tag.Revision);
}

////////////////////////////////////////////////////////////////////////////////

void PutMethodInfoInTraceContext(const TStringBuf& methodName)
{
    if (auto* traceContext = TryGetCurrentTraceContext()) {
        auto baggage = traceContext->UnpackOrCreateBaggage();
        AddTagToBaggage(baggage, EAggregateIOTag::ApiMethod, methodName);
        AddTagToBaggage(baggage, EAggregateIOTag::ProxyKind, "http");
        traceContext->PackBaggage(baggage);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
