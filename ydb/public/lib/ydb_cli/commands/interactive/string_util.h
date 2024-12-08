#pragma once

#include <util/charset/unidata.h>

namespace NYdb {
    namespace NConsoleClient {

        bool IsWordBoundary(char ch);

        size_t LastWordIndex(TStringBuf text);

        TStringBuf LastWord(TStringBuf text);

    } // namespace NConsoleClient
} // namespace NYdb
