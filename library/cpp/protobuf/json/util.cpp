#include "util.h"

#include <util/string/ascii.h>

#include <functional>

namespace {
    void ToSnakeCaseImpl(TString* const name, std::function<bool(const char)> requiresUnderscore) {
        bool requiresChanges = false;
        size_t size = name->size();
        for (size_t i = 0; i < name->size(); i++) {
            if (IsAsciiUpper(name->at(i))) {
                requiresChanges = true;
                if (i > 0 && requiresUnderscore(name->at(i - 1))) {
                    size++;
                }
            }
        }

        if (!requiresChanges) {
            return;
        }

        if (size != name->size()) {
            TString result;
            result.reserve(size);
            for (size_t i = 0; i < name->size(); i++) {
                const char c = name->at(i);
                if (IsAsciiUpper(c)) {
                    if (i > 0 && requiresUnderscore(name->at(i - 1))) {
                        result += '_';
                    }
                    result += AsciiToLower(c);
                } else {
                    result += c;
                }
            }
            *name = std::move(result);
        } else {
            name->to_lower();
        }
    }
}

namespace NProtobufJson {
    void ToSnakeCase(TString* const name) {
        ToSnakeCaseImpl(name, [](const char prev) { return prev != '_'; });
    }

    void ToSnakeCaseDense(TString* const name) {
        ToSnakeCaseImpl(name, [](const char prev) { return prev != '_' && !IsAsciiUpper(prev); });
    }

    bool EqualsIgnoringCaseAndUnderscores(TStringBuf s1, TStringBuf s2) {
        size_t i1 = 0, i2 = 0;

        while (i1 < s1.size() && i2 < s2.size()) {
            if (s1[i1] == '_') {
                ++i1;
            } else if (s2[i2] == '_') {
                ++i2;
            } else if (AsciiToUpper(s1[i1]) != AsciiToUpper(s2[i2])) {
                return false;
            } else {
                ++i1, ++i2;
            }
        }

        while (i1 < s1.size() && s1[i1] == '_') {
            ++i1;
        }
        while (i2 < s2.size() && s2[i2] == '_') {
            ++i2;
        }

        return (i1 == s1.size() && i2 == s2.size());
    }
}
