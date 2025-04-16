#include "normalize_name.h"

#include <yql/essentials/utils/yql_panic.h>

#include <util/string/builder.h>
#include <util/string/ascii.h>

namespace NYql {

    TMaybe<TIssue> NormalizeName(TPosition position, TString& name) {
        const ui32 inputLength = name.length();
        ui32 startCharPos = 0;
        ui32 totalSkipped = 0;
        bool atStart = true;
        bool justSkippedUnderscore = false;
        for (ui32 i = 0; i < inputLength; ++i) {
            const char c = name.at(i);
            if (c == '_') {
                if (!atStart) {
                    if (justSkippedUnderscore) {
                        return TIssue(position, TStringBuilder() << "\"" << name << "\" looks weird, has multiple consecutive underscores");
                    }
                    justSkippedUnderscore = true;
                    ++totalSkipped;
                    continue;
                } else {
                    ++startCharPos;
                }
            } else {
                atStart = false;
                justSkippedUnderscore = false;
            }
        }

        if (totalSkipped >= 5) {
            return TIssue(position, TStringBuilder() << "\"" << name << "\" looks weird, has multiple consecutive underscores");
        }

        ui32 outPos = startCharPos;
        for (ui32 i = startCharPos; i < inputLength; i++) {
            const char c = name.at(i);
            if (c == '_') {
                continue;
            } else {
                name[outPos] = AsciiToLower(c);
                ++outPos;
            }
        }

        name.resize(outPos);
        Y_ABORT_UNLESS(inputLength - outPos == totalSkipped);

        return Nothing();
    }

    TString NormalizeName(const TStringBuf& name) {
        TString result(name);
        TMaybe<TIssue> error = NormalizeName(TPosition(), result);
        YQL_ENSURE(error.Empty(), "" << error->GetMessage());
        return result;
    }

} // namespace NYql
