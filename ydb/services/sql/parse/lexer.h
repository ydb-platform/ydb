#pragma once

#include <util/generic/string.h>

namespace NKikimr::NSql {

enum class ETokenKind {
    End,
    Identifier,
    Star,
    Comma,
    Semicolon,
    Dot,
    LeftParen,
    RightParen,
    Operator,
    Number,
    Parameter,
    String,
    Error,
};

struct TToken {
    ETokenKind Kind = ETokenKind::End;
    TString Text;
    bool Quoted = false;
};

class TLexer {
public:
    explicit TLexer(TStringBuf sql);

    TToken Next();

private:
    void SkipSpace();
    bool SkipBlockComment();

    TStringBuf Input;
    size_t Position = 0;
    bool CommentError = false;
};

} // namespace NKikimr::NSql
