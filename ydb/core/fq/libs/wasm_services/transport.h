#pragma once

#include "wire.h"

#include <ydb/services/udf_store/wasm/async_runtime/runtime.h>

#include <grpcpp/security/credentials.h>

namespace NFq::NWasmServices {

namespace NAsync = NKikimr::NUdfStore::NWasm::NAsync;

enum class EProtocol { Http, Grpc };

// Trusted, already resolved binding supplied by the owner, never by guest code.
// Metadata/ACL resolution and refreshing credentials are not implemented here.
struct TBinding {
    EProtocol Protocol = EProtocol::Http;
    TString Endpoint;
    TString Method = "POST";
    TVector<std::pair<TString, TString>> Headers;
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
};

// One curl multi worker and one gRPC CQ worker, not a thread per request.
// Completion must enqueue an owner event, never enter WASM or destroy this
// transport on an I/O worker. Destruction cancels and drains physical I/O.
class TTransport final : public NAsync::ITransport {
public:
    explicit TTransport(TVector<TBinding> bindings, TTransportLimits limits = {});
    ~TTransport();

    TCancel Start(NAsync::THandle operation, NAsync::EOperationKind kind, TString request,
                  TInstant deadline, TCompletion completion) override;
    TTransportStats Stats() const;

private:
    struct TImpl;
    std::shared_ptr<TImpl> Impl_;
};

TString MakeRequest(ui32 binding, TStringBuf payload);
bool ParseResponse(TStringBuf bytes, TResponseHeader& header, TStringBuf& payload);

} // namespace NFq::NWasmServices
