#include "parse_tree.h"

#include <yql/essentials/sql/v1/complete/syntax/format.h>

#include <util/system/yassert.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

TMaybe<std::string> GetName(SQLv1::Bind_parameterContext* ctx) {
    if (ctx == nullptr) {
        return Nothing();
    }

    if (auto* x = ctx->an_id_or_type()) {
        return x->getText();
    } else if (auto* x = ctx->TOKEN_TRUE()) {
        return x->getText();
    } else if (auto* x = ctx->TOKEN_FALSE()) {
        return x->getText();
    } else {
        return Nothing();
    }
}

TPosition GetPosition(SQLv1::Bind_parameterContext* ctx) {
    if (ctx == nullptr) {
        return {};
    }

    antlr4::Token* token = ctx->getStart();
    if (!token) {
        return {};
    }

    return {
        .Line = static_cast<ui32>(token->getLine()),
        .Column = static_cast<ui32>(token->getCharPositionInLine()),
    };
}

} // namespace NSQLComplete
