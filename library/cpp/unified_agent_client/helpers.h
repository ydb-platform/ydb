#pragma once

#include "client.h"

#include <util/charset/utf8.h>

namespace NUnifiedAgent::NPrivate {
    bool IsUtf8(const THashMap<TString, TString>& meta);

    struct ResultReplacingNonUTF {
        bool IsTruncated{false};
        size_t BrokenCount{0};
        TString Data;
    };

    ResultReplacingNonUTF ReplaceNonUTF(TStringBuf message, char signBrokenSymbol = '?', size_t maxSize = std::numeric_limits<size_t>::max());
}
