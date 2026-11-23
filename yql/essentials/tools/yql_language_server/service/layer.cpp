#include "layer.h"

namespace NLsp::NYql {

TServiceLayer MakeServiceLayer() {
    return {
        .TextDocuments = MakeTextDocuments(NSQLPureAST::MakeParser()),
        .Completion = MakeCompletionService(),
        .Formatting = MakeFormattingService(),
        .Diagnostic = MakeDiagnosticService(),
    };
}

} // namespace NLsp::NYql
