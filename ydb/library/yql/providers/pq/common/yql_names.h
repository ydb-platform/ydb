#pragma once

#include <util/generic/strbuf.h>

namespace NYql {

constexpr TStringBuf PartitionsCountProp = "PartitionsCount";
constexpr TStringBuf ConsumerSetting = "Consumer";
constexpr TStringBuf EndpointSetting = "Endpoint";
constexpr TStringBuf UseSslSetting = "UseSsl";
constexpr TStringBuf AddBearerToTokenSetting = "AddBearerToToken";

} // namespace NYql
