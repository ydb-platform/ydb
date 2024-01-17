#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

namespace NYdb {

class TYdbAuthenticator : public grpc::MetadataCredentialsPlugin {
public:
    TYdbAuthenticator(std::shared_ptr<ICredentialsProvider> credentialsProvider);

    grpc::Status GetMetadata(
        grpc::string_ref,
        grpc::string_ref,
        const grpc::AuthContext&,
        std::multimap<grpc::string, grpc::string>* metadata
    ) override;

    bool IsBlocking() const override;

private:
    std::shared_ptr<ICredentialsProvider> CredentialsProvider_;
};

} // namespace NYdb
