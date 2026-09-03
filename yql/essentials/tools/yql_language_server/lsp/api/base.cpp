#include "base.h"

#include <yql/essentials/tools/yql_language_server/lsp/message/exception.h>
#include <yql/essentials/tools/yql_language_server/lsp/message/method.h>

namespace NLsp {

namespace {

template <typename T>
T Parse(TMaybe<NJson::TJsonValue> params) {
    if (!params) {
        throw TLspException(NJsonRpc::TJsonRpcError::CodeInvalidRequest) << "missing params";
    }

    auto parsed = NYql::NJson::FromJson<T>(std::move(*params));
    if (!parsed) {
        throw TLspException(NJsonRpc::TJsonRpcError::CodeInvalidParams) << parsed.error();
    }

    return std::move(*parsed);
}

} // namespace

TBaseLspApi::TBaseLspApi(NJsonRpc::TJsonRpcOutbox::TPtr out)
    : Out_(std::move(out))
{
}

void TBaseLspApi::Stop() {
    Out_->Stop();
    Out_.Reset();
}

TInitializeResult TBaseLspApi::Initialize(TInitializeParams params) {
    Y_UNUSED(params);
    throw TLspException::MethodNotFound(Method.Initialize);
}

void TBaseLspApi::Initialized(TInitializedParams params) {
    Y_UNUSED(params);
    throw TLspException::MethodNotFound(Method.Initialized);
}

void TBaseLspApi::SetTrace(TSetTraceParams params) {
    Y_UNUSED(params);
    throw TLspException::MethodNotFound(Method.SetTrace);
}

void TBaseLspApi::DidOpen(TDidOpenTextDocumentParams params) {
    Y_UNUSED(params);
    throw TLspException::MethodNotFound(Method.TextDocument.DidOpen);
}

void TBaseLspApi::DidChange(TDidChangeTextDocumentParams params) {
    Y_UNUSED(params);
    throw TLspException::MethodNotFound(Method.TextDocument.DidChange);
}

void TBaseLspApi::DidClose(TDidCloseTextDocumentParams params) {
    Y_UNUSED(params);
    throw TLspException::MethodNotFound(Method.TextDocument.DidClose);
}

TCompletionList TBaseLspApi::Completion(const TCompletionParams& params) const {
    Y_UNUSED(params);
    throw TLspException::MethodNotFound(Method.TextDocument.Completion);
}

TDocumentDiagnosticReport TBaseLspApi::Diagnostic(TDocumentDiagnosticParams params) const {
    Y_UNUSED(params);
    throw TLspException::MethodNotFound(Method.TextDocument.Diagnostic);
}

TVector<TTextEdit> TBaseLspApi::Formatting(const TDocumentFormattingParams& params) const {
    Y_UNUSED(params);
    throw TLspException::MethodNotFound(Method.TextDocument.Formatting);
}

void TBaseLspApi::Send(NJsonRpc::TJsonRpcResponse response) const {
    Out_->Receive(std::move(response));
}

void TBaseLspApi::Receive(NJsonRpc::TJsonRpcRequest request) {
    const auto execute = [&](auto x) -> void {
        Reply(std::move(request), NYql::NJson::ToJson(std::move(x)));
    };

    const auto& m = request.Method;

    if (m == Method.SetTrace) {
        auto params = Parse<TSetTraceParams>(std::move(request.Params));
        SetTrace(std::move(params));
    }

    else if (m == Method.Initialize) {
        auto params = Parse<TInitializeParams>(std::move(request.Params));
        execute(Initialize(std::move(params)));
    }

    else if (m == Method.Initialized) {
        auto params = Parse<TInitializedParams>(std::move(request.Params));
        Initialized(std::move(params));
    }

    else if (m == Method.TextDocument.DidOpen) {
        auto params = Parse<TDidOpenTextDocumentParams>(std::move(request.Params));
        DidOpen(std::move(params));
    }

    else if (m == Method.TextDocument.DidChange) {
        auto params = Parse<TDidChangeTextDocumentParams>(std::move(request.Params));
        DidChange(std::move(params));
    }

    else if (m == Method.TextDocument.DidClose) {
        auto params = Parse<TDidCloseTextDocumentParams>(std::move(request.Params));
        DidClose(std::move(params));
    }

    else if (m == Method.TextDocument.Completion) {
        auto params = Parse<TCompletionParams>(std::move(request.Params));
        execute(Completion(params));
    }

    else if (m == Method.TextDocument.Diagnostic) {
        auto params = Parse<TDocumentDiagnosticParams>(std::move(request.Params));
        execute(Diagnostic(params));
    }

    else if (m == Method.TextDocument.Formatting) {
        auto params = Parse<TDocumentFormattingParams>(std::move(request.Params));
        execute(Formatting(params));
    }

    else {
        throw TLspException::MethodNotFound(m);
    }
}

void TBaseLspApi::Reply(NJsonRpc::TJsonRpcRequest request, NJson::TJsonValue x) {
    if (!request.Id) {
        throw TLspException(NJsonRpc::TJsonRpcError::CodeInvalidRequest)
            << "method '" << request.Method << "' request id is missing";
    }

    Send({.Result = std::move(x), .Id = std::move(*request.Id)});
}

} // namespace NLsp
