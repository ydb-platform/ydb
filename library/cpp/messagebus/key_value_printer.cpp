#include "key_value_printer.h"

#include <util/stream/format.h>

TKeyValuePrinter::TKeyValuePrinter(const TString& sep)
    : Sep(sep)
{
}

TKeyValuePrinter::~TKeyValuePrinter() {
}

void TKeyValuePrinter::AddRowImpl(const TString& key, const TString& value, bool alignLeft) {
    Keys.push_back(key);
    Values.push_back(value);
    AlignLefts.push_back(alignLeft);
}

TString TKeyValuePrinter::PrintToString() const {
    if (Keys.empty()) {
        return TString();
    }

    size_t keyWidth = 0;
    size_t valueWidth = 0;

    for (size_t i = 0; i < Keys.size(); ++i) {
        keyWidth = Max(keyWidth, Keys.at(i).size());
        valueWidth = Max(valueWidth, Values.at(i).size());
    }

    TStringStream ss;

    for (size_t i = 0; i < Keys.size(); ++i) {
        ss << RightPad(Keys.at(i), keyWidth);
        ss << Sep;
        if (AlignLefts.at(i)) {
            ss << Values.at(i);
        } else {
            ss << LeftPad(Values.at(i), valueWidth);
        }
        ss << Endl;
    }

    return ss.Str();
}
