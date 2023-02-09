#include "helpers.h"
#include <util/string/ascii.h>

namespace NUnifiedAgent::NPrivate {
    bool IsUtf8(const THashMap<TString, TString>& meta) {
        for (const auto& p : meta) {
            if (!IsUtf(p.first) || !IsUtf(p.second)) {
                return false;
            }
        }
        return true;
    }

    ResultReplacingNonUTF ReplaceNonUTF(TStringBuf message, char signBrokenSymbol, size_t maxSize) {
        ResultReplacingNonUTF result;
        if (maxSize == 0) {
            result.IsTruncated = !message.empty();
            return result;
        }
        if (message.empty()) {
            return result;
        }

        auto currentPoint = reinterpret_cast<const unsigned char*>(&message[0]);
        auto endPoint = currentPoint + message.size();

        auto pushSignBroken = [&result, signBrokenSymbol]() {
            if (result.Data.empty() || result.Data.back() != signBrokenSymbol) {
                result.Data.push_back(signBrokenSymbol);
            }
            ++result.BrokenCount;
        };

        while (currentPoint < endPoint) {
            wchar32 rune = 0;
            size_t rune_len = 0;
            auto statusRead = SafeReadUTF8Char(rune, rune_len, currentPoint, endPoint);

            if (statusRead == RECODE_OK) {
                if (rune_len == 1 && !IsAsciiAlnum(*currentPoint) && !IsAsciiPunct(*currentPoint) && !IsAsciiSpace(*currentPoint)) {
                    ++currentPoint;
                    pushSignBroken();
                } else {
                    while (rune_len != 0) {
                        result.Data.push_back(*currentPoint);
                        ++currentPoint;
                        --rune_len;
                    }
                }
            } else if (statusRead == RECODE_BROKENSYMBOL) {
                ++currentPoint;
                pushSignBroken();
            } else {
                pushSignBroken();
                break;
            }

            if (result.Data.size() >= maxSize && currentPoint < endPoint) {
                result.IsTruncated = true;
                break;
            }
        }
        return result;
    }
}
