#pragma once

#include <yql/essentials/core/yql_graph_transformer.h>

#include <util/generic/ptr.h>
#include <util/generic/set.h>

#include <unordered_set>

namespace NYql {

struct TTypeAnnotationContext;
struct TKikimrConfiguration;

namespace NNodes {

class TExprBase;

} // namespace NNodes

} // namespace NYql

namespace NKikimr::NKqp::NOpt {

std::unordered_set<std::string_view> GetNonDeterministicFunctions();

TAutoPtr<NYql::IGraphTransformer> CreateKqpTxPeepholeTransformer(
    NYql::TTypeAnnotationContext& typesCtx, 
    const TIntrusivePtr<NYql::TKikimrConfiguration>& config, 
    bool withFinalStageRules = true,
    TSet<TString> disabledOpts = {}
);

TAutoPtr<NYql::IGraphTransformer> CreateKqpTxsPeepholeTransformer(
    NYql::TTypeAnnotationContext& typesCtx, 
    const TIntrusivePtr<NYql::TKikimrConfiguration>& config
);

NYql::IGraphTransformer::TStatus PeepHoleOptimize(const NYql::NNodes::TExprBase& program, NYql::TExprNode::TPtr& newProgram, NYql::TExprContext& ctx,
    NYql::TTypeAnnotationContext& typesCtx, TIntrusivePtr<NYql::TKikimrConfiguration> config,
    bool allowNonDeterministicFunctions, bool withFinalStageRules, TSet<TString> disabledOpts);

} // namespace NKikimr::NKqp::NOpt
