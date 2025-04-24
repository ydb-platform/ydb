#pragma once

#include <yql/essentials/sql/v1/complete/core/input.h>
#include <yql/essentials/sql/v1/complete/text/word.h>

#include <yql/essentials/parser/lexer_common/lexer.h>

namespace NSQLComplete {

    using NSQLTranslation::TParsedTokenList;

    // `PrevTokenIndex` = `NextTokenIndex`, iff caret is enclosed
    struct TCaretTokenPosition {
        size_t PrevTokenIndex;
        size_t NextTokenIndex;
    };

    bool GetStatement(NSQLTranslation::ILexer::TPtr& lexer, TCompletionInput input, TCompletionInput& output);

    TCaretTokenPosition CaretTokenPosition(const TParsedTokenList& tokens, size_t cursorPosition);

    bool EndsWith(const TParsedTokenList& tokens, const TVector<TStringBuf>& pattern);

} // namespace NSQLComplete
