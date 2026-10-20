#pragma once

#include "metrics.h"
#include "tracing.h"
#include "tvm.h"
#include "wire.h"

#include <ydb/services/udf_store/wasm/async_runtime/runtime.h>

#include <grpcpp/security/credentials.h>

namespace NActors {
class TActorSystem;
}

namespace NFq::NWasmServices {

namespace NAsync = NKikimr::NUdfStore::NWasm::NAsync;

enum class EProtocol {
    Http,
    Grpc
};

// Trusted, already resolved binding supplied by the owner, never by guest code.
// Metadata/ACL resolution belongs to the query engine; TVM ticket refresh belongs to the provider.
struct TBinding {
    EProtocol Protocol = EProtocol::Http;
    TString Endpoint;
    TVector<std::pair<TString, TString>> Headers;
    std::shared_ptr<ITvmTicketProvider> TvmProvider;
    TTvmCredentials TvmCredentials;
    TString CaFile;
    TString CaCertificate;
    std::shared_ptr<grpc::ChannelCredentials> GrpcCredentials;
};

struct TTransportLimits {
    ui64 MaxOperations = 128;
    ui64 MaxPayloadBytes = 1 << 20;
    ui64 MaxReservedBytes = 64 << 20;
};

struct TTransportStats {
    ui64 Operations = 0;
    ui64 ReservedBytes = 0;
    ui64 Dispatches = 0;   // executor work turns, useful for checking idle behavior
};

struct TTransportExecutorLimits {
    ui64 MaxOperations = 1024;
    ui64 MaxReservedBytes = 256ull << 20;
};

// One actor on the supplied user pool, shared by all transforms on the node.
// Shared HTTP socket and gRPC completion waiters only post notifications;
// no per-transform threads.
// Producers must stop before the actor system is destroyed.
class TTransportExecutor {
public:
    static std::shared_ptr<TTransportExecutor> Create(
        NActors::TActorSystem* system, ui32 userPool, TTransportExecutorLimits limits = {}, std::shared_ptr<TRequestMetrics> metrics = {});
    TTransportStats Stats() const;

private:
    struct TImpl;
    explicit TTransportExecutor(std::shared_ptr<TImpl> impl);
    std::shared_ptr<TImpl> Impl_;
    friend class TTransport;
    friend struct TTransportTestAccess;
};

// Completion must enqueue an owner event, never enter WASM. Destruction requests
// cancellation; the shared executor retains credits through physical I/O cleanup.
class TTransport final: public NAsync::ITransport {
public:
    TTransport(TVector<TBinding> bindings, std::shared_ptr<TTransportExecutor> executor, TTransportLimits limits = {});
    ~TTransport();
    // Called by the owner before starting an invocation; Start snapshots the
    // context into each operation. Existing calls retain their original parent.
    void SetTraceContext(TTraceContext context);

    TCancel Start(NAsync::THandle operation, NAsync::EOperationKind kind, TString request, TInstant deadline, TCompletion completion) override;
    TTransportStats Stats() const;

private:
    struct TImpl;
    std::shared_ptr<TImpl> Impl_;
};

TString MakeRequest(ui32 binding, TStringBuf payload, TStringBuf method = "POST", TStringBuf path = "/call", TStringBuf alias = {});
bool ParseResponse(TStringBuf bytes, TResponseHeader& header, TStringBuf& payload);

}   // namespace NFq::NWasmServices
