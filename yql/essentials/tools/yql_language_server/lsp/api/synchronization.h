#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/message/synchronization.h>

#include <library/cpp/threading/future/future.h>

namespace NLsp {

class ISynchronizationApi {
public:
    virtual ~ISynchronizationApi() = default;

    virtual void DidOpen(TDidOpenTextDocumentParams params) = 0;

    virtual void DidChange(TDidChangeTextDocumentParams params) = 0;

    virtual void DidClose(TDidCloseTextDocumentParams params) = 0;
};

} // namespace NLsp
