#pragma once

#include "node.h"

#include <yql/essentials/sql/settings/translation_settings.h>

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>

#include <expected>

namespace NSQLTranslationV1 {

enum class EYqlSelectError {
    Error,
    Unsupported,
};

using TYqlSelectResult = std::expected<TNodePtr, EYqlSelectError>;

TYqlSelectResult BuildYqlSelect(
    TContext& ctx,
    NSQLTranslation::ESqlMode mode,
    const NSQLv1Generated::TRule_select_stmt& rule);

} // namespace NSQLTranslationV1
