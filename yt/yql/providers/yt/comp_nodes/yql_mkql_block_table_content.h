#pragma once

#include <yql/essentials/providers/common/codec/yql_codec.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NYql {

NKikimr::NMiniKQL::IComputationNode* WrapYtBlockTableContent(
    NYql::NCommon::TCodecContext& codecCtx,
    NKikimr::NMiniKQL::TComputationMutables& mutables,
    NKikimr::NMiniKQL::TCallable& callable, TStringBuf pathPrefix);

} // NYql
