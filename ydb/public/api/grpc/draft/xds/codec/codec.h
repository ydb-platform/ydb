#pragma once

#include <ydb/public/api/grpc/draft/xds/wire/discovery.pb.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>

#include <util/generic/string.h>

namespace NYdb::NEds {

inline constexpr char TypeUrl[] = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";

void Encode(const Ydb::Discovery::ListEndpointsResult& result, const TString& database,
    const TString& version, const TString& nonce, Ydb::Xds::DiscoveryResponse& response);

bool Decode(const Ydb::Xds::DiscoveryResponse& response, const TString& database,
    Ydb::Discovery::ListEndpointsResult& result, TString& error);

}
