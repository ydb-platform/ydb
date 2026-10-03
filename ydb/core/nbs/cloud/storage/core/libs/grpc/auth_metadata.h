#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/protos/request_source.pb.h>

#include <contrib/libs/grpc/include/grpcpp/security/auth_metadata_processor.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct TRequestSourceKind
{
    grpc::string Name;
    NProto::ERequestSource Value;
};

using TRequestSourceKinds = TVector<TRequestSourceKind>;

////////////////////////////////////////////////////////////////////////////////

class TAuthMetadataProcessor final: public grpc::AuthMetadataProcessor
{
private:
    const NProto::ERequestSource RequestSource;
    TString RequestSourceName;

public:
    explicit TAuthMetadataProcessor(
        const TRequestSourceKinds& kinds,
        NProto::ERequestSource source);

    bool IsBlocking() const override
    {
        // unless we are doing something heavy/blocking
        // we could do it in the caller context.
        return false;
    }

    grpc::Status Process(
        const InputMetadata& authMetadata,
        grpc::AuthContext* context,
        OutputMetadata* consumedAuthMetadata,
        OutputMetadata* responseMetadata) override;
};

////////////////////////////////////////////////////////////////////////////////

TMaybe<NProto::ERequestSource> GetRequestSource(
    const grpc::AuthContext& context,
    const TRequestSourceKinds& kinds);

TString GetAuthToken(
    const std::multimap<grpc::string_ref, grpc::string_ref>& metadata);

}   // namespace NYdb::NBS
