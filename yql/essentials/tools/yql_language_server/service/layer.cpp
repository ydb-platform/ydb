#include "layer.h"

namespace NLsp::NYql {

TServiceLayer MakeServiceLayer() {
    return {
        .TextDocuments = MakeTextDocuments(),
        .Completion = MakeCompletionService(),
        .Formatting = MakeFormattingService(),
        .Diagnostic = MakeDiagnosticService(),
    };
}

} // namespace NLsp::NYql
