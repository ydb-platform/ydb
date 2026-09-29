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

//! Type-independent part of the async server call context
class TBaseAsyncContextCommon: public ICancelableContext {
public:
    explicit TBaseAsyncContextCommon(grpc::ServerCompletionQueue* cq)
        : CQ(cq)
    {
    }

    TString GetPeer() const;
    TString GetAuthority() const;
    TInstant Deadline() const;
    TSet<TStringBuf> GetPeerMetaKeys() const;
    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const;
    TVector<TStringBuf> FindClientCert() const;
    grpc_compression_level GetCompressionLevel() const;

    void Shutdown() override;

protected:
    //! The producer-consumer queue where for asynchronous server notifications.
    grpc::ServerCompletionQueue* const CQ;
    //! Context for the rpc, allowing to tweak aspects of it such as the use
    //! of compression, authentication, as well as to send metadata back to the
    //! client.
    grpc::ServerContext Context;
};

template<typename TService>
class TBaseAsyncContext: public TBaseAsyncContextCommon {
public:
    TBaseAsyncContext(typename TService::TCurrentGRpcService::AsyncService* service, grpc::ServerCompletionQueue* cq)
        : TBaseAsyncContextCommon(cq)
        , Service(service)
    {
    }

protected:
    //! The means of communication with the gRPC runtime for an asynchronous
    //! server.
    typename TService::TCurrentGRpcService::AsyncService* const Service;
};

} // namespace NYdbGrpc
