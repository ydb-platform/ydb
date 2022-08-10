#pragma once

#include <ydb/library/yql/providers/common/token_accessor/grpc/token_accessor_pb.grpc.pb.h>
#include <library/cpp/grpc/client/grpc_client_low.h>

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <util/datetime/base.h>

namespace NYql {

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateTokenAccessorCredentialsProviderFactory(
    const TString& tokenAccessorEndpoint,
    bool useSsl,
    const TString& sslCaCert,
    const TString& serviceAccountId,
    const TString& serviceAccountIdSignature,
    const TDuration& refreshPeriod = TDuration::Hours(1),
    const TDuration& requestTimeout = TDuration::Seconds(10)
);

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateTokenAccessorCredentialsProviderFactory(
    std::shared_ptr<NGrpc::TGRpcClientLow> client,
    std::shared_ptr<NGrpc::TServiceConnection<TokenAccessorService>> connection,
    const TString& serviceAccountId,
    const TString& serviceAccountIdSignature,
    const TDuration& refreshPeriod = TDuration::Hours(1),
    const TDuration& requestTimeout = TDuration::Seconds(10)
);

}

