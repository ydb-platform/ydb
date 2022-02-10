#include "indent_text.h"

#include <util/stream/str.h>

TString IndentText(TStringBuf text, TStringBuf indent) {
    if (text.empty())
        return TString();

    TStringStream ss;
    ss.Reserve(text.size() + 20);

    char pc = 0;
    for (size_t i = 0; i < text.size(); ++i) {
        if (i == 0 || pc == '\n')
            ss << indent;

        char c = text.at(i);
        ss << c;
        pc = c;
    }
    if (pc != '\n')
        ss << '\n';

    return ss.Str();
}
