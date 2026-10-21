#pragma once

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/charset/utf8.h>
#include <util/charset/wide.h>

// note: TUtfIterCode and TUtfIterBuf have more strict restrictions than
// standard 'input_iterator'.
// 1) When used as iterator they should be derefenced only once (dereference
//    has side effect - increment of iterator)
// 2) operator++ do not increment iterator
//
// This is sufficient for most simple std-algorithms, and for 'for-range'

// iterator over UTF-8 string. Each character represented as wchar32 code.
// example: for (wchar32 c: TUtfIterCode("str")) { ... }
struct TUtfIterCode : std::iterator<std::input_iterator_tag, TStringBuf> {
    const ui8* P;
    const ui8* End;

    TUtfIterCode(TStringBuf s) {
        P = (ui8*)s.data();
        End = (ui8*)s.data() + s.size();
    }

    const char* Ptr() {
        return (const char*)P;
    }

    explicit operator bool() {
        return P != End;
    }

    wchar32 GetNext() {
        wchar32 code = 0;
        size_t runeLen = 0;
        Y_ENSURE(SafeReadUTF8Char(code, runeLen, P, End) == RECODE_OK, "broken UTF-8 string failed");

        P += runeLen;
        return code;
    }

    // c++ for-range interface (it's not real iterator interface, just profanation to satisfy compiler)
    TUtfIterCode begin() const {
        return (*this);
    }
    TUtfIterCode end() const {
        return (*this);
    }
    void operator++() {
    }

    wchar32 operator*() {
        return GetNext();
    }

    bool operator!=(const TUtfIterCode&) const {
        //assume '!=' is used to compare current with end
        return P != End;
    }
};

// iterator over UTF-8 string. Each character represented as TStringBuf
// example: for (TStringBuf c: TUtfIterBuf("str")) { ... }
struct TUtfIterBuf : std::iterator<std::input_iterator_tag, TStringBuf> {
    const ui8* P;
    const ui8* End;

    TUtfIterBuf(TStringBuf s) {
        P = (ui8*)s.data();
        End = (ui8*)s.data() + s.size();
    }

    const char* Ptr() {
        return (const char*)P;
    }

    explicit operator bool() {
        return P != End;
    }

    TStringBuf GetNext() {
        size_t runeLen = 0;
        Y_ENSURE(GetUTF8CharLen(runeLen, P, End) == RECODE_OK, "broken UTF-8 string");

        TStringBuf r((const char*)P, runeLen);
        P += runeLen;
        return r;
    }

    // c++ for-range interface (it's not real iterator interface, just profanation to satisfy compiler)
    TUtfIterBuf begin() const {
        return (*this);
    }
    TUtfIterBuf end() const {
        return (*this);
    }
    void operator++() {
    }

    TStringBuf operator*() {
        return GetNext();
    }

    bool operator!=(const TUtfIterBuf&) const {
        //assume '!=' is used to compare current with end
        return P != End;
    }
};
