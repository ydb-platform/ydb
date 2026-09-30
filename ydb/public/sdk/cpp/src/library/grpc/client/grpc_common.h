#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/grpc_common/constants.h>

#include <util/datetime/base.h>
#include <unordered_map>
#include <string>
#include <memory>
#include <new>

namespace NYdbGrpc {
inline namespace Dev {

struct TGRpcClientConfig {
    std::string Locator; // format host:port
    TDuration Timeout = TDuration::Max(); // request timeout
    ui64 MaxMessageSize = NYdb::NGrpc::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT; // Max request and response size
    ui64 MaxInboundMessageSize = 0; // overrides MaxMessageSize for incoming requests
    ui64 MaxOutboundMessageSize = 0; // overrides MaxMessageSize for outgoing requests
    ui32 MaxInFlight = 0;
    bool EnableSsl = false;
    grpc::SslCredentialsOptions SslCredentials;
    grpc_compression_algorithm CompressionAlgorithm = GRPC_COMPRESS_NONE;
    ui64 MemQuota = 0;
    bool BoundedResponseTransport = false;
    std::unordered_map<std::string, std::string> StringChannelParams;
    std::unordered_map<std::string, int> IntChannelParams;
    std::string LoadBalancingPolicy = { };
    std::string SslTargetNameOverride = { };
    bool UseXds = false;
    std::string UserAgentPrefix = { };

    TGRpcClientConfig() = default;
    TGRpcClientConfig(const TGRpcClientConfig&) = default;
    TGRpcClientConfig(TGRpcClientConfig&&) = default;
    TGRpcClientConfig& operator=(const TGRpcClientConfig&) = default;
    TGRpcClientConfig& operator=(TGRpcClientConfig&&) = default;

    TGRpcClientConfig(const std::string& locator, TDuration timeout = TDuration::Max(),
            ui64 maxMessageSize = NYdb::NGrpc::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT, ui32 maxInFlight = 0, const std::string& caCert = "", const std::string& clientCert = "",
            const std::string& clientPrivateKey = "", grpc_compression_algorithm compressionAlgorithm = GRPC_COMPRESS_NONE, bool enableSsl = false)
        : Locator(locator)
        , Timeout(timeout)
        , MaxMessageSize(maxMessageSize)
        , MaxInFlight(maxInFlight)
        , EnableSsl(enableSsl)
        , SslCredentials{.pem_root_certs = NYdb::TStringType{caCert},
                         .pem_private_key = NYdb::TStringType{clientPrivateKey},
                         .pem_cert_chain = NYdb::TStringType{clientCert}}
        , CompressionAlgorithm(compressionAlgorithm)
        , UseXds((Locator.starts_with("xds:///")))
    {}
};

bool ValidateTlsCredentials(const grpc::SslCredentialsOptions& sslCredentials, std::string& errorMessage);

inline std::shared_ptr<grpc::ChannelInterface> CreateChannelInterface(const TGRpcClientConfig& config, grpc_socket_mutator* mutator = nullptr){
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(config.MaxInboundMessageSize ? config.MaxInboundMessageSize : config.MaxMessageSize);
    args.SetMaxSendMessageSize(config.MaxOutboundMessageSize ? config.MaxOutboundMessageSize : config.MaxMessageSize);
    args.SetCompressionAlgorithm(config.CompressionAlgorithm);
    args.SetUserAgentPrefix(NYdb::TStringType{config.UserAgentPrefix});

    // ChannelArguments appends duplicate keys, and gRPC keeps the first value.
    // Omit protected overrides before installing the bounded transport settings.
    const auto isBoundedParameter = [&](const std::string& name) {
        return config.BoundedResponseTransport &&
            (name == GRPC_ARG_HTTP2_BDP_PROBE ||
             name == GRPC_ARG_HTTP2_STREAM_LOOKAHEAD_BYTES ||
             name == GRPC_ARG_MAX_METADATA_SIZE ||
             name == GRPC_ARG_ABSOLUTE_MAX_METADATA_SIZE ||
             name == GRPC_COMPRESSION_CHANNEL_ENABLED_ALGORITHMS_BITSET ||
             name == GRPC_ARG_ENABLE_PER_MESSAGE_DECOMPRESSION);
    };
    for (const auto& kvp: config.StringChannelParams) {
        if (!isBoundedParameter(kvp.first)) {
            args.SetString(NYdb::TStringType{kvp.first}, NYdb::TStringType{kvp.second});
        }
    }

    for (const auto& kvp: config.IntChannelParams) {
        if (!isBoundedParameter(kvp.first)) {
            args.SetInt(NYdb::TStringType{kvp.first}, kvp.second);
        }
    }

    if (config.BoundedResponseTransport) {
        args.SetInt(GRPC_ARG_HTTP2_BDP_PROBE, 0);
        args.SetInt(GRPC_ARG_HTTP2_STREAM_LOOKAHEAD_BYTES, 64 * 1024);
        args.SetInt(GRPC_ARG_MAX_METADATA_SIZE, 16 * 1024);
        args.SetInt(GRPC_ARG_ABSOLUTE_MAX_METADATA_SIZE, 16 * 1024);
        // The receive-size check precedes decompression in gRPC. Reject every
        // compressed encoding and prevent expansion before protobuf preflight.
        args.SetInt(GRPC_COMPRESSION_CHANNEL_ENABLED_ALGORITHMS_BITSET, 1 << GRPC_COMPRESS_NONE);
        args.SetInt(GRPC_ARG_ENABLE_PER_MESSAGE_DECOMPRESSION, 0);
    }

    if (config.MemQuota) {
        grpc::ResourceQuota quota;
        quota.Resize(config.MemQuota);
        args.SetResourceQuota(quota);
    }
    if (mutator) {
        args.SetSocketMutator(mutator);
    }
    if (!config.LoadBalancingPolicy.empty()) {
        args.SetLoadBalancingPolicyName(NYdb::TStringType{config.LoadBalancingPolicy});
    }
    if (!config.SslTargetNameOverride.empty()) {
        args.SetSslTargetNameOverride(NYdb::TStringType{config.SslTargetNameOverride});
    }
    std::shared_ptr<grpc::ChannelCredentials> channelCredentials = nullptr;
    if (config.EnableSsl || !config.SslCredentials.pem_root_certs.empty()) {
        channelCredentials = grpc::SslCredentials(config.SslCredentials);
    } else {
        channelCredentials = grpc::InsecureChannelCredentials();
    }
    if (config.UseXds) {
        channelCredentials = grpc::XdsCredentials(channelCredentials);
    }
    return grpc::CreateCustomChannel(grpc::string(config.Locator), channelCredentials, args);
}

}
}
