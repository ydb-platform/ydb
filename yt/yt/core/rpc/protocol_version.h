#pragma once

#include "private.h"

#include <util/generic/string.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TProtocolVersion
{
    int Major;
    int Minor;

    static TProtocolVersion FromString(TStringBuf protocolVersionString);
};

bool operator == (const TProtocolVersion& lhs, const TProtocolVersion& rhs);
bool operator != (const TProtocolVersion& lhs, const TProtocolVersion& rhs);

void FormatValue(TStringBuilderBase* builder, TProtocolVersion version, TStringBuf spec);
TString ToString(TProtocolVersion protocolVersion);

////////////////////////////////////////////////////////////////////////////////

constexpr TProtocolVersion GenericProtocolVersion{-1, -1};
constexpr TProtocolVersion DefaultProtocolVersion{0, 0};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
