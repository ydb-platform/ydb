#pragma once

#include <util/generic/fwd.h>
#include <util/system/types.h>
#include <util/generic/strbuf.h>


class IOutputStream;

namespace NYql {

enum class EUnescapeResult
{
    OK,
    INVALID_ESCAPE_SEQUENCE,
    INVALID_BINARY,
    INVALID_OCTAL,
    INVALID_HEXADECIMAL,
    INVALID_UNICODE,
    INVALID_END,
};

TStringBuf UnescapeResultToString(EUnescapeResult result);

void EscapeArbitraryAtom(TStringBuf atom, char quoteChar, IOutputStream* out);

EUnescapeResult UnescapeArbitraryAtom(
        TStringBuf atom, char endChar, IOutputStream* out, size_t* readBytes);

void EscapeBinaryAtom(TStringBuf atom, char quoteChar, IOutputStream* out);

EUnescapeResult UnescapeBinaryAtom(
        TStringBuf atom, char endChar, IOutputStream* out, size_t* readBytes);

} // namspace NYql
