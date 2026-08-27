#include "dns_resolver.h"

#include <yt/yt/core/yson/pull_parser_deserialize.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/misc/hash.h>

namespace NYT::NDns {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TDnsResolveOptions options, TStringBuf /*spec*/)
{
    builder->AppendFormat("{EnableIPv4: %v, EnableIPv6: %v}",
        options.EnableIPv4,
        options.EnableIPv6);
}

void Serialize(const TDnsResolveOptions& options, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("enable_ipv4").Value(options.EnableIPv4)
            .Item("enable_ipv6").Value(options.EnableIPv6)
        .EndMap();
}

void Deserialize(TDnsResolveOptions& options, INodePtr node)
{
    auto mapNode = node->AsMap();
    options = {
        .EnableIPv4 = mapNode->GetChildValueOrDefault<bool>("enable_ipv4", TDnsResolveOptions{}.EnableIPv4),
        .EnableIPv6 = mapNode->GetChildValueOrDefault<bool>("enable_ipv6", TDnsResolveOptions{}.EnableIPv6),
    };
    if (!options.EnableIPv4 && !options.EnableIPv6) {
        THROW_ERROR_EXCEPTION("At least one of \"enable_ipv4\" and \"enable_ipv6\" must be true");
    }
}

void Deserialize(TDnsResolveOptions& options, TYsonPullParserCursor* cursor)
{
    Deserialize(options, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NDns::TDnsResolveOptions>::operator()(NYT::NDns::TDnsResolveOptions options) const
{
    size_t result = 0;
    NYT::HashCombine(result, options.EnableIPv4);
    NYT::HashCombine(result, options.EnableIPv6);
    return result;
}

////////////////////////////////////////////////////////////////////////////////
