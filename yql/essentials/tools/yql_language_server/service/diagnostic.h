#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/diagnostic.h>
#include <yql/essentials/tools/yql_language_server/lsp/support/synchronization.h>

namespace NLsp::NYql {

class IDiagnosticService: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDiagnosticService>;

    virtual TDocumentDiagnosticReport Analyze(
        TTextDocumentItemPtr textDocument,
        TMaybe<TString> previousResultId) = 0;
};

IDiagnosticService::TPtr MakeDiagnosticService();

} // namespace NLsp::NYql
