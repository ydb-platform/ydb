#include "url_template.h"

#include <regex>

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {
namespace {

constexpr TStringBuf UrlTemplateNamePattern = R"([A-Za-z_][A-Za-z0-9_]*)";
static const std::regex UrlTemplateNameRegexp(TString(UrlTemplateNamePattern).c_str());

void ValidateUrlTemplateName(TStringBuf name) {
    if (!std::regex_match(name.begin(), name.end(), UrlTemplateNameRegexp)) {
        ythrow yexception()
            << "url template placeholders must use the form {name}, where name matches "
            << UrlTemplateNamePattern;
    }
}

void ValidateLiteralSegment(TStringBuf text) {
    if (text.Contains('}')) {
        ythrow yexception() << "unmatched '}' in url template";
    }
}

bool ScanUrlTemplateExpressions(TStringBuf urlTemplate, TStringBuf parameterNameToFind = {}, bool* hasExpressions = nullptr) {
    bool foundExpression = false;
    const bool findParameter = !parameterNameToFind.empty();
    TStringBuf tail = urlTemplate;
    while (!tail.empty()) {
        TStringBuf prefix;
        TStringBuf rest;
        if (!tail.TrySplit('{', prefix, rest)) {
            ValidateLiteralSegment(tail);
            break;
        }

        ValidateLiteralSegment(prefix);
        tail = rest;

        TStringBuf name;
        if (!tail.TrySplit('}', name, rest)) {
            ythrow yexception() << "missing '}' in url template";
        }

        ValidateUrlTemplateName(name);
        foundExpression = true;
        if (findParameter && name == parameterNameToFind) {
            if (hasExpressions != nullptr) {
                *hasExpressions = true;
            }
            return true;
        }

        tail = rest;
    }

    if (hasExpressions != nullptr) {
        *hasExpressions = foundExpression;
    }
    return false;
}

} // namespace

bool HasUrlTemplateExpressions(TStringBuf urlTemplate) {
    bool hasExpressions = false;
    ScanUrlTemplateExpressions(urlTemplate, {}, &hasExpressions);
    return hasExpressions;
}

bool HasUrlTemplateParameter(TStringBuf urlTemplate, TStringBuf parameterName) {
    return ScanUrlTemplateExpressions(urlTemplate, parameterName);
}

void ValidateUrlTemplateSyntax(TStringBuf urlTemplate) {
    ScanUrlTemplateExpressions(urlTemplate);
}

} // namespace NMVP::NSupportLinks
