#pragma once

#include <ydb/library/yql/core/yql_arrow_resolver.h>

namespace NKikimr {
namespace NMiniKQL {
    class IFunctionRegistry;
}
}

namespace NYql {

IArrowResolver::TPtr MakeSimpleArrowResolver(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry);

} // namespace NYql
