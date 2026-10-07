#pragma once

#include "message.h"

#include <util/generic/yexception.h>

namespace NLsp::NJsonRpc {

class TJsonRpcException: public yexception {
public:
    explicit TJsonRpcException(TJsonRpcError::TCode code);

    TJsonRpcError ToProtocol() const;

    static TJsonRpcException Unknown(std::exception_ptr e);
    static TJsonRpcException Unknown();

private:
    TJsonRpcError::TCode Code_;
};

} // namespace NLsp::NJsonRpc
