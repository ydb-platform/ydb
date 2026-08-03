#include "layer.h"

namespace NLsp::NYql {

TServiceLayer MakeServiceLayer() {
    return {
        .TextDocuments = MakeTextDocuments(),
        .Completion = MakeCompletionService(),
        .Formatting = MakeFormattingService(),
    };
}

} // namespace NLsp::NYql
