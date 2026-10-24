#include "auth_module.h"

#include <ydb/services/udf_store/wasm/bridge_resident.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>
#include <ydb/udfs/wasm/sdk/services/auth.h>

#include <library/cpp/json/json_reader.h>
#include <util/generic/yexception.h>

#include <atomic>
#include <cstring>
#include <mutex>

namespace NFq::NWasmServices {
namespace {
using namespace NKikimr::NUdfStore::NWasm;
namespace NAuth = NYdb::NWasm::NAuth;

class TAuthIo final: public NAsync::ITransport {
public:
    TAuthIo(std::shared_ptr<TTransport> network, TVector<TString> references, TAuthSecretResolver resolver)
        : Network_(std::move(network))
        , References_(std::move(references))
        , Resolver_(std::move(resolver))
    {
    }

    TCancel Start(NAsync::THandle operation, NAsync::EOperationKind kind, TString request, TInstant deadline, TCompletion completion) override {
        if (kind != NAsync::EOperationKind::Secret) {
            return Network_->Start(operation, kind, std::move(request), deadline, std::move(completion));
        }
        ui64 handle = 0;
        if (request.size() == sizeof(handle)) {
            std::memcpy(&handle, request.data(), sizeof(handle));
        }
        if (!handle || handle > References_.size() || !Resolver_ || deadline <= TInstant::Now()) {
            completion(NAsync::EOperationStatus::Failed, {});
            return {};
        }
        // Do not retain the runtime in provider callbacks; its completion uses
        // weak state and becomes inert after cancellation or compartment teardown.
        auto callback = std::make_shared<TCompletion>(std::move(completion));
        try {
            return Resolver_(References_[handle - 1], deadline, [callback](NAsync::EOperationStatus status, TString value) {
                if (value.empty() || value.size() > 4096) {
                    status = NAsync::EOperationStatus::Failed;
                    value.clear();
                }
                return (*callback)(status, std::move(value));
            });
        } catch (...) {
            // Resolver diagnostics may contain credentials. Never propagate them.
            (*callback)(NAsync::EOperationStatus::Failed, {});
            return {};
        }
    }

private:
    std::shared_ptr<TTransport> Network_;
    TVector<TString> References_;
    TAuthSecretResolver Resolver_;
};
}   // namespace

struct TAuthModule::TImpl {
    struct TJob {
        TInstant Deadline;
        TInstant Started;
        TAuthCache::TCompletion Completion;
        std::atomic<bool> Cancelled{ false };
        NAsync::THandle Call = 0;
    };

    struct TQueue {
        std::mutex Mutex;
        bool Closed = false;
        TVector<std::shared_ptr<TJob>> Jobs;
        std::function<void()> Wakeup;
    };

    std::shared_ptr<TQueue> Queue;
    std::unique_ptr<TAuthCache> Cache;
    std::shared_ptr<TTransport> Network;
    std::unique_ptr<NAsync::TRuntime> Runtime;
    TVector<std::shared_ptr<TJob>> Active;
    TString Arguments;
    TString Key;

    TImpl(NYdb::NWasm::IWebAssemblyCompartment& parent, TAuthModuleConfig config, std::shared_ptr<TTransportExecutor> executor,
        std::function<void()> wakeup)
        : Queue(std::make_shared<TQueue>())
    {
        Y_ENSURE(config.Module && config.Module->Manifest.Auth, "Invalid auth WASM module");
        Y_ENSURE(!config.Bootstrap.TvmProvider && !config.Bootstrap.AuthProvider && config.Bootstrap.Headers.empty(),
            "Auth bootstrap must not invoke another authorization provider");
        Y_ENSURE(
            config.ParametersJson.size() <= NAuth::MaxParametersBytes && config.SecretReferences.size() <= 16, "Auth parameters exceed limits");
        NJson::TJsonValue parameters;
        Y_ENSURE(NJson::ReadJsonTree(config.ParametersJson, &parameters, true) && parameters.IsMap(), "Invalid public auth parameters");
        Y_ENSURE(wakeup, "Missing auth owner wakeup");
        EnsureUdfHostIntrinsicsRegistered();
        NAsync::KeepAsyncHostIntrinsicsLinked();
        Queue->Wakeup = std::move(wakeup);
        NAuth::TRequest request;
        request.ParametersBytes = config.ParametersJson.size();
        request.SecretCount = config.SecretReferences.size();
        Arguments = TString(reinterpret_cast<const char*>(&request), sizeof(request)) + config.ParametersJson;
        for (ui64 i = 1; i <= config.SecretReferences.size(); ++i) {
            Arguments.append(reinterpret_cast<const char*>(&i), sizeof(i));
        }
        // The cache is private to this immutable binding and execution identity;
        // it cannot mix credentials from another connection or compartment.
        Key = config.ConnectionIdentity;
        Y_ENSURE(!Key.empty() && Key.size() <= 4096, "Invalid auth connection identity");
        auto query = std::make_unique<TQueryCompartmentHandle>();
        query->Generation = 1;
        query->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(query->Generation);
        // Libraries arrive from the same local/store artifact loader as ordinary
        // modules. Link them in manifest order; the first one provides the SDK.
        TVector<TNamedModuleBytecode> libraries;
        THashSet<TString> required;
        for (const auto& name : config.Module->Manifest.RequiredLibraries) {
            Y_ENSURE(!name.empty() && required.insert(name).second, "Invalid auth library dependency");
            const TNamedModuleBytecode* found = nullptr;
            for (const auto& library : config.Module->Libraries) {
                if (library.Name == name) {
                    Y_ENSURE(!found, "Duplicate auth library artifact");
                    found = &library;
                }
            }
            Y_ENSURE(found && found->Bytecode.ObjectCode, "Auth library dependency is not loaded");
            libraries.push_back(*found);
        }
        Y_ENSURE(libraries.size() == config.Module->Libraries.size(), "Unexpected auth library artifact");
        query->Compartment = CreateRegistryCompartment(libraries, &parent, config.MaxRealmMemoryBytes);
        AddPrecompiledModule(query->Compartment.get(), config.Module->ModuleBytecode, config.Module->ModuleName);
        query->Resident = std::make_unique<TCompartmentResidentCache>(query->Compartment.get());
        TTransportLimits networkLimits;
        networkLimits.MaxPayloadBytes = NAuth::MaxTicketBytes + sizeof(NAuth::TResult);
        Network = std::make_shared<TTransport>(TVector<TBinding>{ std::move(config.Bootstrap) }, std::move(executor), networkLimits);
        auto io = std::make_shared<TAuthIo>(Network, std::move(config.SecretReferences), std::move(config.ResolveSecret));
        NAsync::TLimits limits;
        limits.AllowSecrets = true;
        limits.MaxCalls = config.CacheLimits.MaxEntries;
        limits.MaxPayloadBytes = NAuth::MaxTicketBytes + sizeof(NAuth::TResult) + 4096;
        auto alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), false);
        Runtime = std::make_unique<NAsync::TRuntime>(std::move(query), std::move(alloc), io, limits, Queue->Wakeup);
        const std::weak_ptr<TQueue> weak = Queue;
        Cache = std::make_unique<TAuthCache>(
            [weak, maxJobs = config.CacheLimits.MaxEntries](const TString&, TInstant deadline, TAuthCache::TCompletion completion) {
                auto queue = weak.lock();
                auto job = std::make_shared<TJob>();
                job->Deadline = deadline;
                job->Completion = std::move(completion);
                bool accepted = false;
                if (queue) {
                    {
                        std::lock_guard lock(queue->Mutex);
                        std::erase_if(queue->Jobs, [](const auto& pending) {
                            return pending->Cancelled.load();
                        });
                        if (!queue->Closed && queue->Jobs.size() < maxJobs) {
                            queue->Jobs.push_back(job);
                            accepted = true;
                        }
                    }
                    if (accepted) {
                        queue->Wakeup();
                    }
                }
                if (!accepted) {
                    job->Completion({});
                }
                return [weak, job] {
                    job->Cancelled = true;
                    if (auto queue = weak.lock()) {
                        queue->Wakeup();
                    }
                };
            }, config.CacheLimits);
    }
};

TAuthModule::TAuthModule(NYdb::NWasm::IWebAssemblyCompartment& parent, TAuthModuleConfig config, std::shared_ptr<TTransportExecutor> executor,
    std::function<void()> wakeup)
    : Impl_(std::make_unique<TImpl>(parent, std::move(config), std::move(executor), std::move(wakeup)))
{
}

TAuthModule::~TAuthModule() {
    Close();
}

std::shared_ptr<ICredentialRequest> TAuthModule::Request(TInstant deadline, TCompletion completion) {
    auto result = Impl_->Cache->Request(Impl_->Key, deadline, std::move(completion));
    Impl_->Queue->Wakeup();
    return result;
}

void TAuthModule::Process() {
    if (!Impl_->Runtime) {
        return;
    }
    const auto now = TInstant::Now();
    Impl_->Cache->Expire(now);
    {
        std::lock_guard lock(Impl_->Queue->Mutex);
        for (auto& job : Impl_->Queue->Jobs) {
            Impl_->Active.push_back(std::move(job));
        }
        Impl_->Queue->Jobs.clear();
    }
    Impl_->Runtime->Expire(now);
    Impl_->Runtime->TakeReady();
    for (auto it = Impl_->Active.begin(); it != Impl_->Active.end();) {
        auto job = *it;
        bool done = job->Cancelled;
        TAuthRefreshResult result;
        try {
            if (!done && !job->Call) {
                job->Started = TInstant::Now();
                job->Call = Impl_->Runtime->Start(Impl_->Arguments, job->Deadline);
            }
            if (!done) {
                const auto reply = Impl_->Runtime->Poll(job->Call);
                done = reply.Status != NAsync::ECallStatus::Waiting && reply.Status != NAsync::ECallStatus::Runnable;
                if (reply.Status == NAsync::ECallStatus::Completed) {
                    NAuth::TResult header;
                    std::string_view ticket;
                    if (NAuth::ReadResult({ reply.Data.data(), reply.Data.size() }, header, ticket)) {
                        result.Credential = { ECredentialStatus::Ready, TString(ticket) };
                        result.ExpiresAt = job->Started + TDuration::MilliSeconds(header.LifetimeMs);
                    }
                }
            }
        } catch (...) {
            done = true;
        }
        if (done) {
            if (job->Call) {
                try {
                    Impl_->Runtime->Drop(job->Call);
                } catch (...) {
                }
            }
            it = Impl_->Active.erase(it);
            if (!job->Cancelled) {
                job->Completion(std::move(result));
            }
        } else {
            ++it;
        }
    }
}

TInstant TAuthModule::NextDeadline() const {
    return Impl_->Runtime ? std::min(Impl_->Cache->NextDeadline(), Impl_->Runtime->NextDeadline()) : TInstant::Max();
}

void TAuthModule::SetTraceContext(TTraceContext context) {
    Impl_->Network->SetTraceContext(std::move(context));
}

void TAuthModule::Close() {
    if (!Impl_) {
        return;
    }
    {
        std::lock_guard lock(Impl_->Queue->Mutex);
        Impl_->Queue->Closed = true;
        Impl_->Queue->Jobs.clear();
    }
    Impl_->Cache->Close();
    Impl_->Runtime.reset();
    Impl_->Active.clear();
}

TAuthCache::TStats TAuthModule::Stats() const {
    return Impl_->Cache->Stats();
}
}   // namespace NFq::NWasmServices
