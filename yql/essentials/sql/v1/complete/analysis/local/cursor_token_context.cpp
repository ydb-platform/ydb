#include "cursor_token_context.h"

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>

namespace NSQLComplete {

    namespace {

        bool Tokenize(ILexer::TPtr& lexer, TCompletionInput input, TParsedTokenList& tokens) {
            NYql::TIssues issues;
            if (!NSQLTranslation::Tokenize(
                    *lexer, TString(input.Text), /* queryName = */ "",
                    tokens, issues, /* maxErrors = */ 1)) {
                return false;
            }
            return true;
        }

        TCursor GetCursor(const TParsedTokenList& tokens, size_t cursorPosition) {
            size_t current = 0;
            for (size_t i = 0; i < tokens.size() && current < cursorPosition; ++i) {
                const auto& content = tokens[i].Content;

                current += content.size();
                if (current < cursorPosition) {
                    continue;
                }

                TCursor cursor = {
                    .PrevTokenIndex = i,
                    .NextTokenIndex = i,
                    .Position = cursorPosition,
                };

                if (current == cursorPosition) {
                    cursor.NextTokenIndex += 1;
                }

                return cursor;
            }

            return {
                .PrevTokenIndex = Nothing(),
                .NextTokenIndex = 0,
                .Position = cursorPosition,
            };
        }

        TVector<size_t> GetTokenPositions(const TParsedTokenList& tokens) {
            TVector<size_t> positions;
            positions.reserve(tokens.size());
            size_t pos = 0;
            for (const auto& token : tokens) {
                positions.emplace_back(pos);
                pos += token.Content.size();
            }
            return positions;
        }

    } // namespace

    bool TRichParsedToken::IsLiteral() const {
        return Base->Name == "STRING_VALUE" ||
               Base->Name == "DIGIGTS" ||
               Base->Name == "INTEGER_VALUE" ||
               Base->Name == "REAL";
    }

    size_t TRichParsedToken::End() const {
        return Position + Base->Content.size();
    }

    TRichParsedToken TokenAt(const TCursorTokenContext& context, size_t index) {
        return {
            .Base = &context.Tokens.at(index),
            .Index = index,
            .Position = context.TokenPositions.at(index),
        };
    }

    TMaybe<TRichParsedToken> TCursorTokenContext::Enclosing() const {
        if (Tokens.size() == 1) {
            Y_ENSURE(Tokens[0].Name == "EOF");
            return Nothing();
        }

        if (Cursor.PrevTokenIndex.Empty()) {
            return Nothing();
        }

        auto token = TokenAt(*this, *Cursor.PrevTokenIndex);
        if (Cursor.PrevTokenIndex == Cursor.NextTokenIndex ||
            !IsWordBoundary(token.Base->Content.back())) {
            return token;
        }

        return Nothing();
    }

    TMaybe<TRichParsedToken> TCursorTokenContext::MatchCursorPrefix(const TVector<TStringBuf>& pattern) const {
        const auto prefix = std::span{Tokens.begin(), Cursor.NextTokenIndex};
        if (prefix.size() < pattern.size()) {
            return Nothing();
        }

        ssize_t i = static_cast<ssize_t>(prefix.size()) - 1;
        ssize_t j = static_cast<ssize_t>(pattern.size()) - 1;
        for (; 0 <= j; --i, --j) {
            if (!pattern[j].empty() && prefix[i].Name != pattern[j]) {
                return Nothing();
            }
        }
        return TokenAt(*this, prefix.size() - pattern.size());
    }

    bool GetStatement(
        ILexer::TPtr& lexer,
        const TMaterializedInput& input,
        TCompletionInput& output,
        size_t& output_position) {
        TVector<TString> statements;
        NYql::TIssues issues;
        if (!NSQLTranslationV1::SplitQueryToStatements(
                input.Text, lexer,
                statements, issues, /* file = */ "",
                /* areBlankSkipped = */ false)) {
            return false;
        }

        size_t& cursor = output_position;
        cursor = 0;
        for (const auto& statement : statements) {
            if (input.CursorPosition < cursor + statement.size()) {
                output = {
                    .Text = TStringBuf(input.Text).SubStr(cursor, statement.size()),
                    .CursorPosition = input.CursorPosition - cursor,
                };
                return true;
            }
            cursor += statement.size();
        }

        output.Text = TStringBuf(input.Text);
        output.CursorPosition = input.CursorPosition;
        return true;
    }

    bool GetCursorTokenContext(ILexer::TPtr& lexer, TCompletionInput input, TCursorTokenContext& context) {
        TParsedTokenList tokens;
        if (!Tokenize(lexer, input, tokens)) {
            return false;
        }

        TVector<size_t> positions = GetTokenPositions(tokens);
        TCursor cursor = GetCursor(tokens, input.CursorPosition);
        context = {
            .Tokens = std::move(tokens),
            .TokenPositions = std::move(positions),
            .Cursor = cursor,
        };
        return true;
    }

} // namespace NSQLComplete
