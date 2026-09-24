#pragma once

#include "text_document.h"

#include <yql/essentials/tools/yql_language_server/lsp/message/diagnostic.h>

namespace NLsp::NYql {

class IDiagnosticService: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDiagnosticService>;

    virtual TDocumentDiagnosticReport Analyze(
        TTextDocument::TPtr textDocument,
        TMaybe<TString> previousResultId) = 0;
};

IDiagnosticService::TPtr MakeDiagnosticService();

} // namespace NLsp::NYql
