#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/completion.h>

#include <yql/essentials/sql/v1/ide/completion/sql_complete.h>

namespace NLsp::NYql {

class TCompletionService final: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TCompletionService>;

    explicit TCompletionService(NSQLComplete::ISqlCompletionEngine::TPtr engine);

    TCompletionList Completion(TStringBuf text, const TCompletionParams& params) const;

private:
    static ECompletionItemKind ToMessage(NSQLComplete::ECandidateKind kind);
    static TCompletionItem ToMessage(NSQLComplete::TCandidate candidate);
    static TCompletionList ToMessage(TVector<NSQLComplete::TCandidate> candidates);

    NSQLComplete::ISqlCompletionEngine::TPtr Engine_;
};

TCompletionService::TPtr MakeCompletionService();

} // namespace NLsp::NYql
