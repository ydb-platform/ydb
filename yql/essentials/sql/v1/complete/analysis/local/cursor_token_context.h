#pragma once

#include <yql/essentials/sql/v1/complete/core/input.h>
#include <yql/essentials/sql/v1/complete/text/word.h>

#include <yql/essentials/parser/lexer_common/lexer.h>

#include <util/generic/maybe.h>

namespace NSQLComplete {

    using NSQLTranslation::ILexer;
    using NSQLTranslation::TParsedToken;
    using NSQLTranslation::TParsedTokenList;

    struct TCursor {
        TMaybe<size_t> PrevTokenIndex = Nothing();
        size_t NextTokenIndex = PrevTokenIndex ? *PrevTokenIndex : 0;
        size_t Position = 0;
    };

    struct TRichParsedToken {
        const TParsedToken* Base = nullptr;
        size_t Index = 0;
        size_t Position = 0;

        bool IsLiteral() const;
        size_t End() const;
    };

    struct TCursorTokenContext {
        TParsedTokenList Tokens;
        TVector<size_t> TokenPositions;
        TCursor Cursor;

        TMaybe<TRichParsedToken> Enclosing() const;
        TMaybe<TRichParsedToken> MatchCursorPrefix(const TVector<TStringBuf>& pattern) const;
    };

    bool GetStatement(
        ILexer::TPtr& lexer,
        const TMaterializedInput& input,
        TCompletionInput& output,
        size_t& output_position);

    bool GetCursorTokenContext(
        ILexer::TPtr& lexer,
        TCompletionInput input,
        TCursorTokenContext& context);

} // namespace NSQLComplete
