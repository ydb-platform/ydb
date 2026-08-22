#pragma once

#include <yql/essentials/core/yql_arrow_resolver.h>

namespace NKikimr::NMiniKQL {
class IFunctionRegistry;
} // namespace NKikimr::NMiniKQL

namespace NYql {

IArrowResolver::TPtr MakeSimpleArrowResolver(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry);

} // namespace NYql
