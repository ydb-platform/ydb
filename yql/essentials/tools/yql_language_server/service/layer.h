#pragma once

#include "completion.h"
#include "formatting.h"

#include <yql/essentials/tools/yql_language_server/lsp/support/synchronization.h>

namespace NLsp::NYql {

struct TServiceLayer {
    ITextDocuments::TPtr TextDocuments;
    TCompletionService::TPtr Completion;
    TFormattingService::TPtr Formatting;
};

TServiceLayer MakeServiceLayer();

} // namespace NLsp::NYql
