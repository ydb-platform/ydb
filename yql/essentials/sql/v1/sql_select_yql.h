#pragma once

#include "context.h"
#include "node.h"

#include <yql/essentials/sql/settings/translation_settings.h>

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>

namespace NSQLTranslationV1 {

NYql::TLangVersion YqlSelectLangVersion();

std::unexpected<ESQLError> YqlSelectUnsupported(TContext& ctx, TStringBuf message);

TNodeResult BuildYqlSelect(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_select_stmt& rule);

TNodeResult BuildYqlSelectSubExpr(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_select_subexpr& rule,
    EColumnRefState state);

TNodeResult BuildYqlExists(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_exists_expr& rule);

TNodeResult BuildYqlSelect(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_values_stmt& rule);

} // namespace NSQLTranslationV1
