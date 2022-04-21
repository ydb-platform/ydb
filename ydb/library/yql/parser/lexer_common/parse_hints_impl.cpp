#include "parse_hints_impl.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NSQLTranslation {

namespace NDetail {

enum EParseState {
    INITIAL,
    NAME,
    IN_PARENS,
    IN_QUOTED_VALUE,
    IN_VALUE,
    WANT_WS,
};

TVector<TSQLHint> ParseSqlHints(NYql::TPosition commentPos, const TStringBuf& comment) {
    TVector<TSQLHint> result;
    if (!comment.StartsWith("/*+") && !comment.StartsWith("--+")) {
        return result;
    }
    TSQLHint hint;
    NYql::TTextWalker commentWalker(commentPos);
    const size_t len = comment.size();
    EParseState state = EParseState::INITIAL;
    for (size_t i = 0; i < len; ++i) {
        auto c = comment[i];
        switch (state) {
            case EParseState::INITIAL: {
                if (std::isalpha(c)) {
                    hint.Pos = commentPos;
                    hint.Name.push_back(c);
                    state = EParseState::NAME;
                }
                break;
            }
            case EParseState::NAME: {
                if (std::isalnum(c)) {
                    hint.Name.push_back(c);
                } else if (c == '(') {
                    state = EParseState::IN_PARENS;
                } else {
                    hint = {};
                    state = std::isspace(c) ? EParseState::INITIAL : EParseState::WANT_WS;
                }
                break;
            }
            case EParseState::IN_PARENS: {
                if (c == ')') {
                    result.emplace_back();
                    std::swap(hint, result.back());
                    state = EParseState::WANT_WS;
                } else if (c == '\'') {
                    hint.Values.emplace_back();
                    state = EParseState::IN_QUOTED_VALUE;
                } else if (!std::isspace(c)) {
                    hint.Values.emplace_back();
                    hint.Values.back().push_back(c);
                    state = EParseState::IN_VALUE;
                }
                break;
            }
            case EParseState::IN_QUOTED_VALUE: {
                YQL_ENSURE(!hint.Values.empty());
                if (c == '\'') {
                    if (i + 1 < len && comment[i + 1] == '\'') {
                        ++i;
                        commentWalker.Advance(c);
                        hint.Values.back().push_back(c);
                    } else {
                        state = EParseState::IN_PARENS;
                    }
                } else {
                    hint.Values.back().push_back(c);
                }
                break;
            }
            case EParseState::IN_VALUE: {
                if (std::isspace(c)) {
                    state = EParseState::IN_PARENS;
                } else if (c == ')') {
                    result.emplace_back();
                    std::swap(hint, result.back());
                    state = EParseState::WANT_WS;
                } else {
                    hint.Values.back().push_back(c);
                }
                break;
            }
            case EParseState::WANT_WS: {
                if (std::isspace(c)) {
                    state = EParseState::INITIAL;
                }
                break;
            }
        }
        commentWalker.Advance(c);
    }
    return result;
}

}

}
