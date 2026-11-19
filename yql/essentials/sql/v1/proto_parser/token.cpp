#include "token.h"

#include "parse_tree.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NSQLTranslationV1 {

namespace {

using NSQLTranslation::TParsedToken;
using NSQLTranslation::TParsedTokenList;

struct TTokenSpan {
    size_t Begin;
    size_t End;
};

TTokenSpan TokenSpan(const TParsedTokenList& tokens, const TTokenIndexRange& range) {
    const size_t begin = range.Start;
    const size_t end = range.IsEmpty() ? begin : static_cast<size_t>(range.End) + 1;

    YQL_ENSURE(begin <= end);
    YQL_ENSURE(end <= tokens.size());
    return {.Begin = begin, .End = end};
}

bool IsWhitespace(const TParsedToken& token) {
    return token.Name == "WS";
}

bool IsTrivia(const TParsedToken& token) {
    return IsWhitespace(token) || token.Name == "COMMENT";
}

size_t SkipTokens(const TParsedTokenList& tokens, size_t index, auto predicate) {
    while (index < tokens.size() && predicate(tokens[index])) {
        ++index;
    }
    return index;
}

size_t TriviaBegin(const TParsedTokenList& tokens, size_t index) {
    while (index > 0 && IsTrivia(tokens[index - 1])) {
        --index;
    }
    return index;
}

size_t IncludeTrailingComments(const TParsedTokenList& tokens, size_t end) {
    auto next = SkipTokens(tokens, end, IsWhitespace);
    while (next < tokens.size() && tokens[next].Name == "COMMENT" && tokens[end - 1].Line == tokens[next].Line) {
        end = next + 1;
        next = SkipTokens(tokens, end, IsWhitespace);
    }
    return end;
}

TString RenderQueryText(const TParsedTokenList& tokens, TTokenSpan span, TQueryTextOptions options) {
    TString text;
    for (size_t index = span.Begin; index < span.End; ++index) {
        const auto& token = tokens[index];
        if (token.Name == "EOF") {
            continue;
        }

        if (options.AreCommentSkipped && token.Name == "COMMENT") {
            if (!options.IsWhitespaceNormalized) {
                text += ' ';
            }
            continue;
        }

        if (options.IsWhitespaceNormalized) {
            if (IsWhitespace(token)) {
                continue;
            }

            if (!text.empty()) {
                text += ' ';
            }
        }

        text += token.Content;
    }

    return text;
}

} // namespace

TToken Beginning(const TRule_select_stmt& rule) {
    const auto& parenthesis = rule.GetRule_select_stmt_core2()
                                  .GetRule_select_stmt_intersect1()
                                  .GetRule_select_kind_parenthesis1();
    return Beginning(Unpack(parenthesis).GetRule_select_kind1());
}

TToken Beginning(const TRule_select_kind& rule) {
    const auto& block = rule.GetBlock2();
    switch (block.GetAltCase()) {
        case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt1:
            return block.GetAlt1().GetRule_process_core1().GetToken1();
        case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt2:
            return block.GetAlt2().GetRule_reduce_core1().GetToken1();
        case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt3:
            return block.GetAlt3().GetRule_select_core1().GetToken2();
        case NSQLv1Generated::TRule_select_kind_TBlock2::kAlt4:
            return block.GetAlt4().GetRule_combine_core1().GetToken1();
        case NSQLv1Generated::TRule_select_kind_TBlock2::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
}

TString QueryText(const NSQLTranslation::TParsedTokenList& tokens, const TTokenIndexRange& range, TQueryTextOptions options) {
    const bool isEmpty = range.IsEmpty();
    if (isEmpty && (!options.IsSurroundingTriviaIncluded || range.Start == 0)) {
        return {};
    }

    auto span = TokenSpan(tokens, range);
    if (isEmpty && !IsTrivia(tokens[span.Begin - 1])) {
        return {};
    }

    if (options.IsSurroundingTriviaIncluded) {
        span.Begin = TriviaBegin(tokens, span.Begin);
        span.End = SkipTokens(tokens, span.End, IsTrivia);
    }

    return RenderQueryText(tokens, span, options);
}

TString ContextRecreationQueryText(const NSQLTranslation::TParsedTokenList& tokens, const TVector<TTokenIndexRange>& ranges) {
    TString query;
    for (const auto& range : ranges) {
        YQL_ENSURE(!range.IsEmpty());
        auto span = TokenSpan(tokens, range);

        span.Begin = TriviaBegin(tokens, span.Begin);
        if (span.Begin > 0 && tokens[span.Begin - 1].Name == "SEMICOLON") {
            span.Begin = IncludeTrailingComments(tokens, span.Begin);
        }
        span.Begin = SkipTokens(tokens, span.Begin, IsWhitespace);

        const auto next = SkipTokens(tokens, span.End, IsTrivia);
        const bool isTerminated = next < tokens.size() && tokens[next].Name == "SEMICOLON";
        if (isTerminated) {
            span.End = IncludeTrailingComments(tokens, next + 1);
        }

        query += RenderQueryText(tokens, span, {});
        if (!isTerminated) {
            query += ';';
        }
        query += '\n';
    }
    return query;
}

} // namespace NSQLTranslationV1
