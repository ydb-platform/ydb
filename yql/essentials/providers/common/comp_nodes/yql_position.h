#pragma once
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NKikimr::NMiniKQL {

NYql::TPosition ExtractPosition(TCallable& callable);

} // namespace NKikimr::NMiniKQL
