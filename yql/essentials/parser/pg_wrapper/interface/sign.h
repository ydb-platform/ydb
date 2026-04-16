#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <compare>
#include <expected>

namespace NYql {

// Used only for type annotation / optimization purposes.
// Do not use in computation level!
//
// Expensive call:
// This call creates |State| and calls |GetSign| with it.
// Actually state must be created once per thread and cached.
std::expected<std::strong_ordering, TString> PgSign(TStringBuf value, ui32 typeId);

} // namespace NYql
