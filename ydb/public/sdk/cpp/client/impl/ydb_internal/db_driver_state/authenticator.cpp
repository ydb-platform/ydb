#define INCLUDE_YDB_INTERNAL_H
#include "authenticator.h"

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

namespace NYdb {

TYdbAuthenticator::TYdbAuthenticator(std::shared_ptr<ICredentialsProvider> credentialsProvider)
    : CredentialsProvider_(std::move(credentialsProvider))
{}

grpc::Status TYdbAuthenticator::GetMetadata(
    grpc::string_ref,
    grpc::string_ref,
    const grpc::AuthContext&,
    std::multimap<grpc::string, grpc::string>* metadata
) {
    try {
        metadata->insert(std::make_pair(YDB_AUTH_TICKET_HEADER, CredentialsProvider_->GetAuthInfo()));
    } catch (const std::exception& e) {
        return grpc::Status(
            grpc::StatusCode::UNAUTHENTICATED,
            "Can't get Authentication info from CredentialsProvider",
            e.what()
        );
    }
    return grpc::Status::OK;
}

bool TYdbAuthenticator::IsBlocking() const {
    return false;
}

} // namespace NYdb
