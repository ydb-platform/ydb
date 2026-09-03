#pragma once

#include "radix.h"

#include <yql/essentials/tools/yql_language_server/lsp/message/completion.h>

#include <yql/essentials/sql/v1/ide/completion/sql_complete.h>

namespace NLsp::NYql {

class TCompletionService final: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TCompletionService>;

    explicit TCompletionService(NSQLComplete::ISqlCompletionEngine::TPtr engine);

    TCompletionList Completion(TStringBuf text, const TCompletionParams& params) const;

private:
    TString SortText(size_t index, size_t length) const;

    static ECompletionItemKind ToMessage(NSQLComplete::ECandidateKind kind);

    TCompletionItem ToMessage(
        NSQLComplete::TCandidate candidate, size_t index, size_t sortTextLen) const;

    TCompletionList ToMessage(TVector<NSQLComplete::TCandidate> candidates) const;

    TRadix Radix_;
    NSQLComplete::ISqlCompletionEngine::TPtr Engine_;
};

TCompletionService::TPtr MakeCompletionService();

} // namespace NLsp::NYql
