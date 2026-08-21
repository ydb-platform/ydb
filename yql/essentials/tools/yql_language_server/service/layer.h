#pragma once

#include "completion.h"
#include "diagnostic.h"
#include "formatting.h"

#include <yql/essentials/tools/yql_language_server/lsp/support/synchronization.h>

namespace NLsp::NYql {

struct TServiceLayer {
    ITextDocuments::TPtr TextDocuments;
    TCompletionService::TPtr Completion;
    TFormattingService::TPtr Formatting;
    IDiagnosticService::TPtr Diagnostic;
};

TServiceLayer MakeServiceLayer();

} // namespace NLsp::NYql
