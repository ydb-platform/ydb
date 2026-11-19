#pragma once

#include <yql/essentials/minikql/mkql_node.h>

namespace NYql::NDq {

bool RequiresFirstChannelInput(NKikimr::NMiniKQL::TRuntimeNode root, const NKikimr::NMiniKQL::TNode* input);

} // namespace NYql::NDq
