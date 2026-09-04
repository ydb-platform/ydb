#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/synchronization.h>

namespace NLsp {

using TTextDocumentItemPtr = std::shared_ptr<const TTextDocumentItem>;

class ITextDocuments: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ITextDocuments>;

    virtual void Open(TDidOpenTextDocumentParams params) = 0;
    virtual void Change(TDidChangeTextDocumentParams params) = 0;
    virtual void Close(const TDidCloseTextDocumentParams& params) = 0;

    /// @return non-null
    virtual TTextDocumentItemPtr Find(const TTextDocumentIdentifier& id) const = 0;
};

ITextDocuments::TPtr MakeTextDocuments();

} // namespace NLsp
