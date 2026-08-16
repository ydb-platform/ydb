#include "formatting.h"

#include <yql/essentials/tools/yql_language_server/lsp/support/position.h>

#include <yql/essentials/public/sql_format/sql_format.h>

namespace NLsp::NYql {

TMaybe<TTextEdit> TFormattingService::Formatting(TStringBuf text) {
    TString formatted;
    if (TString error; !NSQLFormat::SqlFormatSimple(TString(text), formatted, error)) {
        return Nothing();
    }

    if (formatted == text) {
        return Nothing();
    }

    return TTextEdit{
        .Range = {
            .Start = FromBytes(0, text),
            .End = FromBytes(text.size(), text),
        },
        .NewText = std::move(formatted),
    };
}

TFormattingService::TPtr MakeFormattingService() {
    return new TFormattingService();
}

} // namespace NLsp::NYql
