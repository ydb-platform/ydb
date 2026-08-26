#include "exception.h"

namespace NLsp {

TLspException::TLspException(NJsonRpc::TJsonRpcError::TCode code)
    : NJsonRpc::TJsonRpcException(code)
{
}

TLspException TLspException::BadRequest() {
    return TLspException(NJsonRpc::TJsonRpcError::CodeInvalidParams)
           << "bad request: ";
}

TLspException TLspException::MethodNotFound(TStringBuf name) {
    return TLspException(NJsonRpc::TJsonRpcError::CodeMethodNotFound)
           << "method not found: '" << name << "'";
}

TLspException TLspException::UnknownUri(const TDocumentUri& uri) {
    return TLspException::BadRequest()
           << "unknown uri '" << uri << "', "
           << "maybe forgot to open";
}

TLspException TLspException::Conflict(TTextDocumentVersion incoming, TTextDocumentVersion existing) {
    return TLspException::BadRequest()
           << "stale update with document version "
           << incoming << " vs existing " << existing;
}

TLspException TLspException::Unsupported() {
    return TLspException(NJsonRpc::TJsonRpcError::CodeInternalError)
           << "unsupported: ";
}

} // namespace NLsp
