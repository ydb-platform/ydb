#pragma once

#include "api.h"

#include <yql/essentials/tools/yql_language_server/lsp/consumer/base.h>
#include <yql/essentials/tools/yql_language_server/lsp/json_rpc/message.h>

namespace NLsp {

class TBaseLspApi: public ILspApi {
public:
    explicit TBaseLspApi(NJsonRpc::TJsonRpcOutbox::TPtr out);

    void Stop() override;

    TInitializeResult Initialize(TInitializeParams params) override;
    void Initialized(TInitializedParams params) override;
    void SetTrace(TSetTraceParams params) override;

    void DidOpen(TDidOpenTextDocumentParams params) override;
    void DidChange(TDidChangeTextDocumentParams params) override;
    void DidClose(TDidCloseTextDocumentParams params) override;

    TCompletionList Completion(const TCompletionParams& params) const override;

    TDocumentDiagnosticReport Diagnostic(TDocumentDiagnosticParams params) const override;

    TVector<TTextEdit> Formatting(const TDocumentFormattingParams& params) const override;

protected:
    void Send(NJsonRpc::TJsonRpcResponse response) const;

private:
    void Receive(NJsonRpc::TJsonRpcRequest request) final;

    void Reply(NJsonRpc::TJsonRpcRequest request, NJson::TJsonValue x);

    NJsonRpc::TJsonRpcOutbox::TPtr Out_;
};

} // namespace NLsp
