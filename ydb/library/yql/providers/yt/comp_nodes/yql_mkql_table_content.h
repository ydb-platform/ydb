#pragma once

#include <ydb/library/yql/providers/common/codec/yql_codec.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NYql {

NKikimr::NMiniKQL::IComputationNode* WrapYtTableContent(
    NYql::NCommon::TCodecContext& codecCtx,
    NKikimr::NMiniKQL::TComputationMutables& mutables,
    NKikimr::NMiniKQL::TCallable& callable, const TString& optLLVM, TStringBuf pathPrefix);

} // NYql
