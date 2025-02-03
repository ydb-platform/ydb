#pragma once

#include <util/generic/strbuf.h>

namespace NYql {

constexpr TStringBuf LeftName = "Left!";
constexpr TStringBuf RightName = "Right!";
constexpr TStringBuf SyncName = "Sync!";
constexpr TStringBuf IfName = "If!";
constexpr TStringBuf ForName = "For!";
constexpr TStringBuf CommitName = "Commit!";
constexpr TStringBuf ReadName = "Read!";
constexpr TStringBuf WriteName = "Write!";
constexpr TStringBuf ConfigureName = "Configure!";
constexpr TStringBuf ConsName = "Cons!";

} // namespace NYql
