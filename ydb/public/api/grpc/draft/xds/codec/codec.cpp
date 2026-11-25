#include "codec.h"

namespace NYdb::NEds {

namespace {

// A single typed extension lets each SDK reuse its ListEndpoints parser,
// preserving node IDs, TLS options, IPs and the bridge topology together.
constexpr char DiscoveryMetadata[] = "ydb.discovery";

template <typename TMessage>
bool UnpackDiscoveryMetadata(const Ydb::Xds::Metadata& metadata, TMessage& message) {
    const auto& extensions = metadata.typed_filter_metadata();
    const auto it = extensions.find(DiscoveryMetadata);
    return it != extensions.end() && it->second.UnpackTo(&message);
}

}

void Encode(const Ydb::Discovery::ListEndpointsResult& result, const TString& database,
        const TString& version, const TString& nonce, Ydb::Xds::DiscoveryResponse& response) {
    Ydb::Xds::ClusterLoadAssignment assignment;
    assignment.set_cluster_name(database);
    auto* group = assignment.add_endpoints();
    group->mutable_locality()->set_region("ydb");
    group->mutable_load_balancing_weight()->set_value(1);

    (*group->mutable_metadata()->mutable_typed_filter_metadata())[DiscoveryMetadata].PackFrom(result);
    for (const auto& info : result.endpoints()) {
        auto* lb = group->add_lb_endpoints();
        auto* socket = lb->mutable_endpoint()->mutable_address()->mutable_socket_address();
        socket->set_address(info.address());
        socket->set_port_value(info.port());
    }

    response.Clear();
    response.set_type_url(TypeUrl);
    response.set_version_info(version);
    response.set_nonce(nonce);
    auto* resource = response.add_resources();
    resource->set_type_url(TypeUrl);
    resource->set_value(assignment.SerializeAsString());
}

bool Decode(const Ydb::Xds::DiscoveryResponse& response, const TString& database,
        Ydb::Discovery::ListEndpointsResult& result, TString& error) {
    if (response.type_url() != TypeUrl || response.version_info().empty() || response.nonce().empty()
            || response.resources_size() != 1 || response.resources(0).type_url() != TypeUrl) {
        error = "Expected one versioned EDS resource for the requested database";
        return false;
    }

    Ydb::Xds::ClusterLoadAssignment assignment;
    if (!assignment.ParseFromString(response.resources(0).value()) || assignment.cluster_name() != database
            || assignment.endpoints_size() != 1) {
        error = "Invalid YDB cluster load assignment";
        return false;
    }

    const auto& group = assignment.endpoints(0);
    Ydb::Discovery::ListEndpointsResult decoded;
    if (!UnpackDiscoveryMetadata(group.metadata(), decoded) || decoded.endpoints_size() != group.lb_endpoints_size()) {
        error = "Invalid YDB discovery attributes";
        return false;
    }
    for (int i = 0; i < decoded.endpoints_size(); ++i) {
        const auto& info = decoded.endpoints(i);
        const auto& socket = group.lb_endpoints(i).endpoint().address().socket_address();
        if (socket.protocol() != 0 || socket.address().empty() || socket.port_value() == 0 || socket.port_value() > 65535) {
            error = "Invalid EDS TCP socket address";
            return false;
        }
        if (socket.address() != info.address() || socket.port_value() != info.port()) {
            error = "EDS socket address disagrees with YDB endpoint";
            return false;
        }
    }
    result.Swap(&decoded);
    error.clear();
    return true;
}

}
