#include "exception.h"

namespace NLsp::NJsonRpc {

TJsonRpcException::TJsonRpcException(TJsonRpcError::TCode code)
    : Code_(code)
{
}

TJsonRpcError TJsonRpcException::ToProtocol() const {
    return {
        .Code = Code_,
        .Message = what(),
    };
}

TJsonRpcException TJsonRpcException::Unknown(std::exception_ptr e) try {
    std::rethrow_exception(e);
} catch (...) {
    return Unknown() << CurrentExceptionMessage();
}

TJsonRpcException TJsonRpcException::Unknown() {
    return TJsonRpcException(NJsonRpc::TJsonRpcError::CodeInternalError)
           << "unknown error: ";
}

} // namespace NLsp::NJsonRpc
