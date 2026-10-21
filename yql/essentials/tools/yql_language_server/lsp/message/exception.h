#pragma once

#include "text_document.h"

#include <yql/essentials/tools/yql_language_server/lsp/json_rpc/exception.h>

namespace NLsp {

class TLspException: public NJsonRpc::TJsonRpcException {
public:
    explicit TLspException(NJsonRpc::TJsonRpcError::TCode code);

    static TLspException BadRequest();
    static TLspException MethodNotFound(TStringBuf name);
    static TLspException UnknownUri(const TDocumentUri& uri);
    static TLspException Conflict(TTextDocumentVersion incoming, TTextDocumentVersion existing);
    static TLspException Unsupported();
};

} // namespace NLsp
