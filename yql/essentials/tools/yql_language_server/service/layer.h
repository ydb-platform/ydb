#pragma once

#include "completion.h"
#include "diagnostic.h"
#include "formatting.h"
#include "text_document.h"

namespace NLsp::NYql {

struct TServiceLayer {
    TTextDocuments::TPtr TextDocuments;
    TCompletionService::TPtr Completion;
    TFormattingService::TPtr Formatting;
    IDiagnosticService::TPtr Diagnostic;
};

TServiceLayer MakeServiceLayer();

} // namespace NLsp::NYql
