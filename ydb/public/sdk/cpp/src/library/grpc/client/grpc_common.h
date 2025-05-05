#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/type_switcher.h>
#include <ydb/public/sdk/cpp/src/library/grpc/common/constants.h>

#include <util/datetime/base.h>
#include <unordered_map>
#include <string>

namespace NYdbGrpc {
inline namespace Dev {

struct TGRpcClientConfig {
    std::string Locator; // format host:port
    TDuration Timeout = TDuration::Max(); // request timeout
    ui64 MaxMessageSize = DEFAULT_GRPC_MESSAGE_SIZE_LIMIT; // Max request and response size
    ui64 MaxInboundMessageSize = 0; // overrides MaxMessageSize for incoming requests
    ui64 MaxOutboundMessageSize = 0; // overrides MaxMessageSize for outgoing requests
    ui32 MaxInFlight = 0;
    bool EnableSsl = false;
    grpc::SslCredentialsOptions SslCredentials;
    grpc_compression_algorithm CompressionAlgoritm = GRPC_COMPRESS_NONE;
    ui64 MemQuota = 0;
    std::unordered_map<std::string, std::string> StringChannelParams;
    std::unordered_map<std::string, int> IntChannelParams;
    std::string LoadBalancingPolicy = { };
    std::string SslTargetNameOverride = { };

    TGRpcClientConfig() = default;
    TGRpcClientConfig(const TGRpcClientConfig&) = default;
    TGRpcClientConfig(TGRpcClientConfig&&) = default;
    TGRpcClientConfig& operator=(const TGRpcClientConfig&) = default;
    TGRpcClientConfig& operator=(TGRpcClientConfig&&) = default;

    TGRpcClientConfig(const std::string& locator, TDuration timeout = TDuration::Max(),
            ui64 maxMessageSize = DEFAULT_GRPC_MESSAGE_SIZE_LIMIT, ui32 maxInFlight = 0, const std::string& caCert = "", const std::string& clientCert = "",
            const std::string& clientPrivateKey = "", grpc_compression_algorithm compressionAlgorithm = GRPC_COMPRESS_NONE, bool enableSsl = false)
        : Locator(locator)
        , Timeout(timeout)
        , MaxMessageSize(maxMessageSize)
        , MaxInFlight(maxInFlight)
        , EnableSsl(enableSsl)
        , SslCredentials{.pem_root_certs = NYdb::TStringType{caCert},
                         .pem_private_key = NYdb::TStringType{clientPrivateKey},
                         .pem_cert_chain = NYdb::TStringType{clientCert}}
        , CompressionAlgoritm(compressionAlgorithm)
    {}
};

inline std::shared_ptr<grpc::ChannelInterface> CreateChannelInterface(const TGRpcClientConfig& config, grpc_socket_mutator* mutator = nullptr){
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(config.MaxInboundMessageSize ? config.MaxInboundMessageSize : config.MaxMessageSize);
    args.SetMaxSendMessageSize(config.MaxOutboundMessageSize ? config.MaxOutboundMessageSize : config.MaxMessageSize);
    args.SetCompressionAlgorithm(config.CompressionAlgoritm);

    for (const auto& kvp: config.StringChannelParams) {
        args.SetString(NYdb::TStringType{kvp.first}, NYdb::TStringType{kvp.second});
    }

    for (const auto& kvp: config.IntChannelParams) {
        args.SetInt(NYdb::TStringType{kvp.first}, kvp.second);
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
    if (config.EnableSsl || !config.SslCredentials.pem_root_certs.empty()) {
        return grpc::CreateCustomChannel(grpc::string(config.Locator), grpc::SslCredentials(config.SslCredentials), args);
    } else {
        return grpc::CreateCustomChannel(grpc::string(config.Locator), grpc::InsecureChannelCredentials(), args);
    }
}

}
}
