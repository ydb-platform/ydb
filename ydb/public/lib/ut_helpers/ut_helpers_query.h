#pragma once

#include <util/generic/fwd.h>
#include <ydb/library/grpc/client/grpc_client_low.h>

namespace NTestHelpers {

TString CreateQuerySession(const NYdbGrpc::TGRpcClientConfig& clientConfig);

NYdbGrpc::IStreamRequestCtrl::TPtr CheckAttach(NYdbGrpc::TGRpcClientLow& clientLow, const NYdbGrpc::TGRpcClientConfig& clientConfig,
    const TString& id, int code, bool& allDoneOk);

void CheckAttach(const NYdbGrpc::TGRpcClientConfig& clientConfig, const TString& id, int expected, bool& allDoneOk);
void CheckDelete(const NYdbGrpc::TGRpcClientConfig& clientConfig, const TString& id, int expected, bool& allDoneOk);
void EnsureSessionClosed(NYdbGrpc::IStreamRequestCtrl::TPtr p, int expected, bool& allDone);

}
