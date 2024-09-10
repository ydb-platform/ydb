#pragma once

#include <ydb/core/kqp/host/kqp_translate.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/sql/settings/serializer/proto/translation_settings.pb.h>

namespace NYql {

TExprNode::TPtr FindTopLevelRead(const TExprNode::TPtr& queryGraph);

TExprNode::TPtr RewriteReadFromView(
    const TExprNode::TPtr& node,
    TExprContext& ctx,
    const TString& query,
    NKikimr::NKqp::TKqpTranslationSettingsBuilder& settingsBuilder,
    IModuleResolver::TPtr moduleResolver,
    const NYql::NProto::TTranslationSettings& capturedContext
);

}
