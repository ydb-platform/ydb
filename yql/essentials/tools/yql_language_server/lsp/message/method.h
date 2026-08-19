#pragma once

#include <yql/essentials/tools/yql_language_server/lsp/json_rpc/message.h>

namespace NLsp {

constexpr struct {
    TStringBuf SetTrace = "$/setTrace";
    TStringBuf Initialize = "initialize";
    TStringBuf Initialized = "initialized";
    struct {
        TStringBuf DidOpen = "textDocument/didOpen";
        TStringBuf DidChange = "textDocument/didChange";
        TStringBuf DidClose = "textDocument/didClose";
        TStringBuf Completion = "textDocument/completion";
        TStringBuf Diagnostic = "textDocument/diagnostic";
        TStringBuf Formatting = "textDocument/formatting";
    } TextDocument;
} Method;

bool IsReadonlyMethod(TStringBuf method);

} // namespace NLsp
