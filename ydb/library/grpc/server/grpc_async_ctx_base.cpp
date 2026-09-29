#include "grpc_async_ctx_base.h"

namespace NYdbGrpc {

TString TBaseAsyncContextCommon::GetPeer() const {
    // Decode URL-encoded square brackets
    auto ip = Context.peer();
    CGIUnescape(ip);
    return ip;
}

TString TBaseAsyncContextCommon::GetAuthority() const {
    const auto authority = Context.ExperimentalGetAuthority();
    return TString(authority.data(), authority.size());
}

TInstant TBaseAsyncContextCommon::Deadline() const {
    // The timeout transferred in "grpc-timeout" header [1] and calculated from the deadline
    // right before the request is getting to be send.
    // 1. https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md
    //
    // After this timeout calculated back to the deadline on the server side
    // using server grpc GPR_CLOCK_MONOTONIC time (raw_deadline() method).
    // deadline() method convert this to epoch related deadline GPR_CLOCK_REALTIME
    //

    std::chrono::system_clock::time_point t = Context.deadline();
    if (t == std::chrono::system_clock::time_point::max()) {
        return TInstant::Max();
    }
    auto us = std::chrono::time_point_cast<std::chrono::microseconds>(t);
    return TInstant::MicroSeconds(us.time_since_epoch().count());
}

TSet<TStringBuf> TBaseAsyncContextCommon::GetPeerMetaKeys() const {
    TSet<TStringBuf> keys;
    for (const auto& [key, _]: Context.client_metadata()) {
        keys.emplace(key.data(), key.size());
    }
    return keys;
}

TVector<TStringBuf> TBaseAsyncContextCommon::GetPeerMetaValues(TStringBuf key) const {
    const auto& clientMetadata = Context.client_metadata();
    const auto range = clientMetadata.equal_range(grpc::string_ref{key.data(), key.size()});
    if (range.first == range.second) {
        return {};
    }

    TVector<TStringBuf> values;
    values.reserve(std::distance(range.first, range.second));

    for (auto it = range.first; it != range.second; ++it) {
        values.emplace_back(it->second.data(), it->second.size());
    }
    return values;
}

TVector<TStringBuf> TBaseAsyncContextCommon::FindClientCert() const {
    auto authContext = Context.auth_context();

    TVector<TStringBuf> values;
    for (auto& value: authContext->FindPropertyValues(GRPC_X509_PEM_CERT_PROPERTY_NAME)) {
        values.emplace_back(value.data(), value.size());
    }
    return values;
}

grpc_compression_level TBaseAsyncContextCommon::GetCompressionLevel() const {
    return Context.compression_level();
}

void TBaseAsyncContextCommon::Shutdown() {
    // Shutdown may only be called after request has started successfully
    if (Context.c_call())
        Context.TryCancel();
}

} // namespace NYdbGrpc
