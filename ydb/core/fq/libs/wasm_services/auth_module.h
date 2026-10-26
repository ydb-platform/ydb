#pragma once

#include "auth_cache.h"
#include "transport.h"

#include <ydb/services/udf_store/wasm/module_catalog.h>

namespace NFq::NWasmServices {

// Host callback resolves only references approved for the current connection
// and execution identity. The callback and cancellation must be nonblocking.
using TAuthSecretResolver =
    std::function<NAsync::ITransport::TCancel(const TString& reference, TInstant deadline, NAsync::ITransport::TCompletion completion)>;

struct TAuthModuleConfig {
    NKikimr::NUdfStore::NWasm::TWasmModuleArtifactPtr Module;
    TString ParametersJson;
    TVector<TString> SecretReferences;
    TString ConnectionIdentity;
    TBinding Bootstrap;
    TAuthSecretResolver ResolveSecret;
    TAuthCacheLimits CacheLimits;
    ui64 MaxRealmMemoryBytes = 64 << 20;
};

// One instance/cache per transform compartment. Current service transforms have
// one connection binding. Multiple calls of that binding share its refresh.
// Request can run on the transport executor; only Process/Close enter the guest
// and must run on the compartment owner's mailbox, serialized with application.
class TAuthModule final: public ICredentialProvider {
public:
    TAuthModule(NYdb::NWasm::IWebAssemblyCompartment& parent, TAuthModuleConfig config, std::shared_ptr<TTransportExecutor> executor,
        std::function<void()> wakeup);
    ~TAuthModule();
    std::shared_ptr<ICredentialRequest> Request(TInstant deadline, TCompletion completion) override;
    void Process();
    TInstant NextDeadline() const;
    void SetTraceContext(TTraceContext context);
    void Close();
    TAuthCache::TStats Stats() const;

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};
}   // namespace NFq::NWasmServices
