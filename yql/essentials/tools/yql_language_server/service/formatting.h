#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/formatting.h>

namespace NLsp::NYql {

class TFormattingService final: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TFormattingService>;

    TMaybe<TTextEdit> Formatting(TStringBuf text);
};

TFormattingService::TPtr MakeFormattingService();

} // namespace NLsp::NYql
