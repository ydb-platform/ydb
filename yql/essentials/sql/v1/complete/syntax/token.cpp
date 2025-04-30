#include "token.h"

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>

namespace NSQLComplete {

    bool GetStatement(NSQLTranslation::ILexer::TPtr& lexer, TCompletionInput input, TCompletionInput& output) {
        TVector<TString> statements;
        NYql::TIssues issues;
        if (!NSQLTranslationV1::SplitQueryToStatements(
                TString(input.Text) + ";", lexer,
                statements, issues, /* file = */ "",
                /* areBlankSkipped = */ false)) {
            return false;
        }

        size_t cursor = 0;
        for (const auto& statement : statements) {
            if (input.CursorPosition < cursor + statement.size()) {
                output = {
                    .Text = input.Text.SubStr(cursor, statement.size()),
                    .CursorPosition = input.CursorPosition - cursor,
                };
                return true;
            }
            cursor += statement.size();
        }

        output = input;
        return true;
    }

    TCaretTokenPosition CaretTokenPosition(const TParsedTokenList& tokens, size_t cursorPosition) {
        size_t cursor = 0;
        for (size_t i = 0; i < tokens.size(); ++i) {
            const auto& content = tokens[i].Content;
            cursor += content.size();
            if (cursorPosition < cursor) {
                return {i, i};
            } else if (cursorPosition == cursor && IsWordBoundary(content.back())) {
                return {i, i + 1};
            }
        }
        return {std::max(tokens.size(), static_cast<size_t>(1)) - 1, tokens.size()};
    }

    bool EndsWith(const TParsedTokenList& tokens, const TVector<TStringBuf>& pattern) {
        if (tokens.size() < pattern.size()) {
            return false;
        }
        for (yssize_t i = tokens.ysize() - 1, j = pattern.ysize() - 1; 0 <= j; --i, --j) {
            if (!pattern[j].empty() && tokens[i].Name != pattern[j]) {
                return false;
            }
        }
        return true;
    }

} // namespace NSQLComplete
