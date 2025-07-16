#pragma once

#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>

namespace NKikimr::NMiniKQL {

TComputationNodeFactory GetDqNodeFactory(TComputationNodeFactory customFactory = {});

} // namespace NKikimr::NMiniKQL
