#include "lexer.h"

#include <util/string/ascii.h>

namespace NKikimr::NSql {

namespace {

bool IsIdentStart(char ch) {
    return IsAsciiAlpha(ch) || ch == '_';
}

bool IsIdentContinue(char ch) {
    return IsAsciiAlnum(ch) || ch == '_';
}

} // namespace

TLexer::TLexer(TStringBuf sql)
    : Input(sql)
{
}

bool TLexer::SkipBlockComment() {
    Position += 2;
    int depth = 1;
    while (Position + 1 < Input.size()) {
        if (Input[Position] == '/' && Input[Position + 1] == '*') {
            ++depth;
            Position += 2;
            continue;
        }
        if (Input[Position] == '*' && Input[Position + 1] == '/') {
            --depth;
            Position += 2;
            if (depth == 0) {
                return true;
            }
            continue;
        }
        ++Position;
    }
    Position = Input.size();
    return false;
}

void TLexer::SkipSpace() {
    for (;;) {
        while (Position < Input.size() && IsAsciiSpace(Input[Position])) {
            ++Position;
        }
        if (Position + 1 < Input.size() && Input[Position] == '-' && Input[Position + 1] == '-') {
            Position += 2;
            while (Position < Input.size() && Input[Position] != '\n') {
                ++Position;
            }
            continue;
        }
        if (Position + 1 < Input.size() && Input[Position] == '/' && Input[Position + 1] == '*') {
            if (!SkipBlockComment()) {
                CommentError = true;
            }
            continue;
        }
        break;
    }
}

TToken TLexer::Next() {
    SkipSpace();
    if (CommentError) {
        return {ETokenKind::Error, "comment", false};
    }
    if (Position >= Input.size()) {
        return {};
    }

    const char ch = Input[Position];
    if (ch == '*') {
        ++Position;
        return {ETokenKind::Star, "*", false};
    }
    if (ch == ',') {
        ++Position;
        return {ETokenKind::Comma, ",", false};
    }
    if (ch == ';') {
        ++Position;
        return {ETokenKind::Semicolon, ";", false};
    }
    if (ch == '.') {
        ++Position;
        return {ETokenKind::Dot, ".", false};
    }
    if (ch == '(') {
        ++Position;
        return {ETokenKind::LeftParen, "(", false};
    }
    if (ch == ')') {
        ++Position;
        return {ETokenKind::RightParen, ")", false};
    }
    if (ch == '\'' ) {
        ++Position;
        TString text;
        while (Position < Input.size()) {
            if (Input[Position] == '\'') {
                if (Position + 1 < Input.size() && Input[Position + 1] == '\'') {
                    text.push_back('\'');
                    Position += 2;
                    continue;
                }
                ++Position;
                return {ETokenKind::String, text, false};
            }
            text.push_back(Input[Position]);
            ++Position;
        }
        return {ETokenKind::Error, text, false};
    }
    if (ch == '=' || ch == '<' || ch == '>' || ch == '!') {
        TString text(1, ch);
        ++Position;
        if (Position < Input.size()) {
            const char second = Input[Position];
            if ((ch == '<' && (second == '>' || second == '=')) || (ch == '>' && second == '=') || (ch == '!' && second == '=')) {
                text.push_back(second);
                ++Position;
            }
        }
        return {ETokenKind::Operator, text, false};
    }
    if (IsAsciiDigit(ch)) {
        const size_t begin = Position;
        ++Position;
        while (Position < Input.size() && IsAsciiDigit(Input[Position])) {
            ++Position;
        }
        return {ETokenKind::Number, TString(Input.substr(begin, Position - begin)), false};
    }
    if (ch == '"') {
        ++Position;
        TString text;
        while (Position < Input.size()) {
            if (Input[Position] == '"') {
                if (Position + 1 < Input.size() && Input[Position + 1] == '"') {
                    text.push_back('"');
                    Position += 2;
                    continue;
                }
                ++Position;
                return {ETokenKind::Identifier, text, true};
            }
            text.push_back(Input[Position]);
            ++Position;
        }
        return {ETokenKind::Error, text, true};
    }
    if (IsIdentStart(ch)) {
        const size_t begin = Position;
        ++Position;
        while (Position < Input.size() && IsIdentContinue(Input[Position])) {
            ++Position;
        }
        TString text(Input.substr(begin, Position - begin));
        text.to_lower();
        return {ETokenKind::Identifier, text, false};
    }

    TString text(Input.substr(Position, 1));
    ++Position;
    return {ETokenKind::Error, text, false};
}

} // namespace NKikimr::NSql
