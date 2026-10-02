#pragma once

#include <util/charset/utf8.h>
#include <util/generic/string.h>

class TUtf8Char: public TStringBase<TUtf8Char, char> {
public:
    TUtf8Char()
        : Length(0)
    {
    }

    explicit TUtf8Char(wchar32 c) {
        WriteUTF8Char(c, Length, reinterpret_cast<unsigned char*>(&Data));
    }

    size_t length() const noexcept {
        return Length;
    }

    const char* data() const noexcept {
        return Data;
    }

private:
    static constexpr size_t UtfMax = 4;
    size_t Length;
    char Data[UtfMax];
};
