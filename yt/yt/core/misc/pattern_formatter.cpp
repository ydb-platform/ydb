#include "pattern_formatter.h"

#include <yt/yt/core/misc/error.h>

#include <util/stream/str.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static const char Dollar = '$';
static const char LeftParen = '(';
static const char RightParen = ')';

////////////////////////////////////////////////////////////////////////////////

void TPatternFormatter::AddProperty(const TString& name, const TString& value)
{
    PropertyMap[name] = value;
}

TString TPatternFormatter::Format(const TString& pattern)
{
    TString result;

    for (size_t pos = 0; pos < pattern.size(); ++pos) {
        if (pattern[pos] == Dollar && (pos + 1 < pattern.size() && pattern[pos + 1] == LeftParen)) {
            auto left = pos + 2;
            auto right = left;
            while (right < pattern.size() && pattern[right] != RightParen) {
                right += 1;
            }

            if (right < pattern.size()) {
                auto property = pattern.substr(left, right - left);

                auto it = PropertyMap.find(property);
                if (it != PropertyMap.end()) {
                    result.append(it->second);
                    pos = right;
                    continue;
                }
            }
        }

        result.append(pattern[pos]);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
