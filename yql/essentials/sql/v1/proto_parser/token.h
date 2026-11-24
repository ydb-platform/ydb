#pragma once

#include "range.h"

#include <yql/essentials/parser/lexer_common/lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

struct TQueryTextOptions {
    bool AreCommentSkipped = false;
    bool IsWhitespaceNormalized = false;
    bool IsSurroundingTriviaIncluded = false;
};

TString QueryText(const NSQLTranslation::TParsedTokenList& tokens, const TTokenIndexRange& range, TQueryTextOptions options = {});

TString ContextRecreationQueryText(const NSQLTranslation::TParsedTokenList& tokens, const TVector<TTokenIndexRange>& ranges);

TToken Beginning(const TRule_select_stmt& rule);

TToken Beginning(const TRule_select_kind& rule);

} // namespace NSQLTranslationV1
