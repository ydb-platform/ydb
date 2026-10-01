#include "position.h"

#include <yql/essentials/tools/yql_language_server/lsp/message/exception.h>

#include <util/charset/utf8.h>
#include <util/charset/wide.h>

namespace NLsp {

size_t ToBytes(TPosition position, TStringBuf text) {
    const auto error = [&](size_t i) {
        return NLsp::TLspException::BadRequest()
               << "(spot " << i << ") bad utf-8 or position "
               << NYql::NJson::ToJsonString(std::move(position));
    };

    const unsigned char* cur = reinterpret_cast<const unsigned char*>(text.data());
    const unsigned char* const end = cur + text.size();

    for (ui64 line = 0; line < position.Line; ++line) {
        while (cur < end && *cur != '\n') {
            if (wchar32 rune; ReadUTF8CharAndAdvance(rune, cur, end) != RECODE_OK) {
                throw error(0);
            }
        }
        if (cur >= end) {
            throw error(1);
        }
        ++cur; // skip '\n'
    }

    for (ui64 ch = 0; ch < position.Character; ++ch) {
        if (cur >= end ||
            *cur == '\n' ||
            (cur + 1 < end && *cur == '\r' && cur[1] == '\n'))
        {
            throw error(2);
        }

        wchar32 rune;
        if (ReadUTF8CharAndAdvance(rune, cur, end) != RECODE_OK) {
            throw error(3);
        }
        if (rune >= 0x10000) {
            ++ch; // non-BMP code points take 2 UTF-16 code units
        }
    }

    return cur - reinterpret_cast<const unsigned char*>(text.data());
}

TPosition FromBytes(size_t bytes, TStringBuf text) {
    const auto error = [&](size_t i) {
        return NLsp::TLspException::BadRequest()
               << "(spot " << i << ") bad utf-8 or byte offset " << bytes;
    };

    if (bytes > text.size()) {
        throw error(0);
    }

    TPosition position;
    const unsigned char* cur = reinterpret_cast<const unsigned char*>(text.data());
    const unsigned char* const end = cur + text.size();
    const unsigned char* const target = cur + bytes;

    while (cur < target) {
        if (*cur == '\n') {
            ++position.Line;
            position.Character = 0;
            ++cur;
        } else if (cur + 1 < end && *cur == '\r' && cur[1] == '\n') {
            ++position.Line;
            position.Character = 0;
            cur += 2;
        } else {
            wchar32 rune;
            if (ReadUTF8CharAndAdvance(rune, cur, end) != RECODE_OK) {
                throw error(1);
            }
            if (cur > target) {
                throw error(2);
            }
            position.Character += rune >= 0x10000 ? 2 : 1;
        }
    }

    return position;
}

} // namespace NLsp
