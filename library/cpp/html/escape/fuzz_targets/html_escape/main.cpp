#include <library/cpp/html/escape/escape.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/system/yassert.h>

namespace {

TString ModelEscape(TStringBuf input, bool text) {
    TString out;
    for (char c : input) {
        switch (c) {
            case '"':
                if (!text) {
                    out += "&quot;";
                    break;
                }
                out += c;
                break;
            case '&':
                out += "&amp;";
                break;
            case '<':
                out += "&lt;";
                break;
            case '>':
                out += "&gt;";
                break;
            default:
                out += c;
                break;
        }
    }
    return out;
}

bool StartsWithAnyEntity(TStringBuf value) {
    return value.StartsWith("&amp;") ||
        value.StartsWith("&lt;") ||
        value.StartsWith("&gt;") ||
        value.StartsWith("&quot;");
}

void CheckEscapedEntities(TStringBuf escaped, bool text) {
    for (size_t i = 0; i < escaped.size(); ++i) {
        Y_ABORT_UNLESS(escaped[i] != '<');
        Y_ABORT_UNLESS(escaped[i] != '>');
        if (!text) {
            Y_ABORT_UNLESS(escaped[i] != '"');
        }
        if (escaped[i] == '&') {
            Y_ABORT_UNLESS(StartsWithAnyEntity(escaped.SubStr(i)));
        }
    }
}

void CheckHtmlEscape(TStringBuf input) {
    const TString owned(input);

    const TString text = NHtml::EscapeText(owned);
    const TString attr = NHtml::EscapeAttributeValue(owned);

    Y_ABORT_UNLESS(text == ModelEscape(input, true));
    Y_ABORT_UNLESS(attr == ModelEscape(input, false));
    Y_ABORT_UNLESS(text.size() >= input.size());
    Y_ABORT_UNLESS(attr.size() >= input.size());

    CheckEscapedEntities(text, true);
    CheckEscapedEntities(attr, false);

    if (input.find_first_of("&<>") == TStringBuf::npos) {
        Y_ABORT_UNLESS(text == input);
    }
    if (input.find_first_of("\"&<>") == TStringBuf::npos) {
        Y_ABORT_UNLESS(attr == input);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);
    CheckHtmlEscape(provider.ConsumeRandomLengthString(4096));
    return 0;
}
