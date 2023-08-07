#pragma once

#include <util/generic/fwd.h>
#include <library/cpp/grpc/client/grpc_client_low.h>

namespace NTestHelpers {

TString CreateQuerySession(const NGrpc::TGRpcClientConfig& clientConfig);

NGrpc::IStreamRequestCtrl::TPtr CheckAttach(NGrpc::TGRpcClientLow& clientLow, const NGrpc::TGRpcClientConfig& clientConfig,
    const TString& id, int code, bool& allDoneOk);

void CheckAttach(const NGrpc::TGRpcClientConfig& clientConfig, const TString& id, int expected, bool& allDoneOk);
void CheckDelete(const NGrpc::TGRpcClientConfig& clientConfig, const TString& id, int expected, bool& allDoneOk);

}
