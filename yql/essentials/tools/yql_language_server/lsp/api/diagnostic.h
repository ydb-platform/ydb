#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/diagnostic.h>

namespace NLsp {

class IDiagnosticApi {
public:
    virtual ~IDiagnosticApi() = default;

    virtual TDocumentDiagnosticReport Diagnostic(TDocumentDiagnosticParams params) const = 0;
};

} // namespace NLsp
