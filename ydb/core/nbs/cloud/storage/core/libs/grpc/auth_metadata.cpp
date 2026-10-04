#include "auth_metadata.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/verify.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

TMaybe<NProto::ERequestSource> ParseRequestSourceName(
    const TRequestSourceKinds& kinds,
    grpc::string_ref name)
{
    for (const auto& kind: kinds) {
        if (kind.Name == name) {
            return kind.Value;
        }
    }
    return {};
}

TMaybe<TString> TryGetRequestSourceName(
    const TRequestSourceKinds& kinds,
    NProto::ERequestSource source)
{
    for (const auto& kind: kinds) {
        if (kind.Value == source) {
            return kind.Name;
        }
    }
    return {};
}

const grpc::string AUTH_HEADER = "authorization";
const grpc::string AUTH_METHOD = "Bearer ";
const grpc::string AUTH_PROPERTY_SOURCE = "y-auth-source";

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TAuthMetadataProcessor::TAuthMetadataProcessor(
    const TRequestSourceKinds& kinds,
    NProto::ERequestSource source)
    : RequestSource(source)
{
    auto requestSourceName = TryGetRequestSourceName(kinds, RequestSource);
    STORAGE_VERIFY(
        !requestSourceName.Empty(),
        "Auth",
        NProto::ERequestSource_Name(source));
    RequestSourceName = std::move(*requestSourceName);
}

grpc::Status TAuthMetadataProcessor::Process(
    const InputMetadata& authMetadata,
    grpc::AuthContext* context,
    OutputMetadata* consumedAuthMetadata,
    OutputMetadata* responseMetadata)
{
    Y_UNUSED(authMetadata);
    Y_UNUSED(consumedAuthMetadata);
    Y_UNUSED(responseMetadata);

    if (context->FindPropertyValues(AUTH_PROPERTY_SOURCE).empty()) {
        context->AddProperty(AUTH_PROPERTY_SOURCE, RequestSourceName);
    }

    return grpc::Status::OK;
}

////////////////////////////////////////////////////////////////////////////////

TMaybe<NProto::ERequestSource> GetRequestSource(
    const grpc::AuthContext& context,
    const TRequestSourceKinds& kinds)
{
    auto values = context.FindPropertyValues(AUTH_PROPERTY_SOURCE);
    if (!values.empty()) {
        return ParseRequestSourceName(kinds, values[0]);
    }
    return {};
}

TString GetAuthToken(
    const std::multimap<grpc::string_ref, grpc::string_ref>& metadata)
{
    auto range = metadata.equal_range(AUTH_HEADER);
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second.starts_with(AUTH_METHOD)) {
            auto token = it->second.substr(AUTH_METHOD.size());
            return {token.begin(), token.end()};
        }
    }

    return {};
}

}   // namespace NYdb::NBS
