#include "layer.h"

namespace NLsp::NYql {

TServiceLayer MakeServiceLayer() {
    return {
        .TextDocuments = MakeTextDocuments(),
        .Completion = MakeCompletionService(),
    };
}

} // namespace NLsp::NYql
