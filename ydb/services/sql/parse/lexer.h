#pragma once

#include <util/generic/string.h>

namespace NKikimr::NSql {

enum class ETokenKind {
    End,
    Identifier,
    Star,
    Comma,
    Semicolon,
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

    TStringBuf Input;
    size_t Position = 0;
};

} // namespace NKikimr::NSql
