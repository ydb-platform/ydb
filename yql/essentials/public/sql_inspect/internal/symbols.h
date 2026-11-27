#pragma once

#include <yql/essentials/public/sql_inspect/symbols.h>

namespace NYql::NSqlInspect {

TSymbolsResult GetSymbolsImpl(const TString& sql, const TSymbolSettings& settings);

} // namespace NYql::NSqlInspect
