#include "api.h"

#include <yql/essentials/tools/yql_language_server/lsp/api/base.h>

namespace NLsp::NYql {

namespace {

class TLspApi final: public TBaseLspApi {
public:
    TLspApi(NJsonRpc::TJsonRpcOutbox::TPtr out, TServiceLayer service)
        : TBaseLspApi(std::move(out))
        , Service_(std::move(service))
    {
    }

    TInitializeResult Initialize(TInitializeParams params) override {
        Y_UNUSED(params);

        return {
            .Capabilities = {
                .TextDocumentSync = TTextDocumentSyncOptions{
                    .OpenClose = true,
                    .Change = ETextDocumentSyncKind::Full,
                },
                .CompletionProvider = TCompletionOptions{
                    .TriggerCharacters = TVector<TString>{"`", ".", ":", "/", "", " "},
                },
                .DocumentFormattingProvider = TDocumentFormattingOptions{},
                .DiagnosticProvider = TDiagnosticOptions{
                    .Identifier = "YQL",
                },
            },
            .ServerInfo = TServerInfo{
                .Name = "yql",
                .Version = "0.0.1",
            },
        };
    }

    void Initialized(TInitializedParams params) override {
        Y_UNUSED(params);
    }

    void SetTrace(TSetTraceParams params) override {
        Y_UNUSED(params);
    }

    void DidOpen(TDidOpenTextDocumentParams params) override {
        Service_.TextDocuments->Open(std::move(params));
    }

    void DidChange(TDidChangeTextDocumentParams params) override {
        Service_.TextDocuments->Change(std::move(params));
    }

    void DidClose(TDidCloseTextDocumentParams params) override {
        Service_.TextDocuments->Close(params);
    }

    TCompletionList Completion(const TCompletionParams& params) const override {
        auto document = Service_.TextDocuments->Find(params.TextDocument);
        return Service_.Completion->Completion(document->Text, params);
    }

    TDocumentDiagnosticReport Diagnostic(TDocumentDiagnosticParams params) const override {
        auto document = Service_.TextDocuments->Find(params.TextDocument);
        return Service_.Diagnostic->Analyze(std::move(document), std::move(params.PreviousResultId));
    }

    TVector<TTextEdit> Formatting(const TDocumentFormattingParams& params) const override {
        auto document = Service_.TextDocuments->Find(params.TextDocument);
        if (auto edit = Service_.Formatting->Formatting(document->Text)) {
            return {std::move(*edit)};
        }
        return {};
    }

private:
    TServiceLayer Service_;
};

} // namespace

NJsonRpc::TJsonRpcListenerFactory MakeYqlLspApi(TServiceLayer layer) {
    return [layer = std::move(layer)](NJsonRpc::TJsonRpcOutbox::TPtr out) mutable {
        return new TLspApi(std::move(out), std::move(layer));
    };
}

} // namespace NLsp::NYql
