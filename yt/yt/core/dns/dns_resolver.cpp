#include "dns_resolver.h"

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TDnsResolveOptions options, TStringBuf /*spec*/)
{
    builder->AppendFormat("{EnableIPv4: %v, EnableIPv6: %v}",
        options.EnableIPv4,
        options.EnableIPv6);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns

