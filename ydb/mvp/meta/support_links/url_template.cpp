#include "url_template.h"

#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

static bool IsUrlTemplateNameStart(char c) {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || c == '_';
}

static bool IsUrlTemplateNameChar(char c) {
    return IsUrlTemplateNameStart(c) || ('0' <= c && c <= '9');
}

TVector<TString> ExtractUrlTemplateParameters(TStringBuf urlTemplate) {
    TVector<TString> parameters;
    THashSet<TString> seenParameters;

    for (size_t i = 0; i < urlTemplate.size();) {
        const char c = urlTemplate[i];
        if (c == '{') {
            if (i + 1 < urlTemplate.size() && urlTemplate[i + 1] == '{') {
                i += 2;
                continue;
            }

            ++i;
            if (i == urlTemplate.size()) {
                ythrow yexception() << "missing '}' in url template";
            }

            const char first = urlTemplate[i];
            if (first == '}') {
                ythrow yexception() << "empty placeholders are not supported in url templates";
            }
            if ('0' <= first && first <= '9') {
                ythrow yexception() << "numeric placeholders are not supported in url templates";
            }
            if (!IsUrlTemplateNameStart(first)) {
                ythrow yexception() << "placeholder names in url templates must start with a letter or '_'";
            }

            const size_t nameStart = i;
            bool placeholderClosed = false;
            while (i < urlTemplate.size()) {
                const char current = urlTemplate[i];
                if (current == '}') {
                    TString name(urlTemplate.SubStr(nameStart, i - nameStart));
                    if (seenParameters.insert(name).second) {
                        parameters.push_back(std::move(name));
                    }
                    ++i;
                    placeholderClosed = true;
                    break;
                }
                if (current == ':') {
                    ythrow yexception() << "format specifiers are not supported in url templates";
                }
                if (!IsUrlTemplateNameChar(current)) {
                    ythrow yexception() << "placeholder names in url templates may contain only letters, digits, and '_'";
                }
                ++i;
            }

            if (!placeholderClosed) {
                ythrow yexception() << "missing '}' in url template";
            }

            continue;
        }

        if (c == '}') {
            if (i + 1 < urlTemplate.size() && urlTemplate[i + 1] == '}') {
                i += 2;
                continue;
            }
            ythrow yexception() << "unmatched '}' in url template";
        }

        ++i;
    }

    return parameters;
}

void ValidateUrlTemplateSyntax(TStringBuf urlTemplate) {
    ExtractUrlTemplateParameters(urlTemplate);
}

} // namespace NMVP::NSupportLinks
