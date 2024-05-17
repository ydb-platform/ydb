#pragma once

#include "grpc_server.h"

#include <library/cpp/string_utils/quote/quote.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/system/yassert.h>
#include <util/generic/set.h>

#include <grpcpp/server.h>
#include <grpcpp/server_context.h>

#include <chrono>

namespace NYdbGrpc {

template<typename TService>
class TBaseAsyncContext: public ICancelableContext {
public:
    TBaseAsyncContext(typename TService::TCurrentGRpcService::AsyncService* service, grpc::ServerCompletionQueue* cq)
        : Service(service)
        , CQ(cq)
    {
    }

    TString GetPeerName() const {
        // Decode URL-encoded square brackets
        auto ip = Context.peer();
        CGIUnescape(ip);
        return ip;
    }

    TInstant Deadline() const {
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

    TSet<TStringBuf> GetPeerMetaKeys() const {
        TSet<TStringBuf> keys;
        for (const auto& [key, _]: Context.client_metadata()) {
            keys.emplace(key.data(), key.size());
        }
        return keys;
    }

    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const {
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

    TVector<TStringBuf> FindClientCert() const {
        auto authContext = Context.auth_context();

        TVector<TStringBuf> values;
        for (auto& value: authContext->FindPropertyValues(GRPC_X509_PEM_CERT_PROPERTY_NAME)) {
            values.emplace_back(value.data(), value.size());
        }
        return values;
    }

    grpc_compression_level GetCompressionLevel() const {
        return Context.compression_level();
    }

    void Shutdown() override {
        // Shutdown may only be called after request has started successfully
        if (Context.c_call())
            Context.TryCancel();
    }

protected:
    //! The means of communication with the gRPC runtime for an asynchronous
    //! server.
    typename TService::TCurrentGRpcService::AsyncService* const Service;
    //! The producer-consumer queue where for asynchronous server notifications.
    grpc::ServerCompletionQueue* const CQ;
    //! Context for the rpc, allowing to tweak aspects of it such as the use
    //! of compression, authentication, as well as to send metadata back to the
    //! client.
    grpc::ServerContext Context;
};

} // namespace NYdbGrpc
