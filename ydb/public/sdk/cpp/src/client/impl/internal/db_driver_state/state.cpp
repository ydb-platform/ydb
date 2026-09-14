#define INCLUDE_YDB_INTERNAL_H
#include "state.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/logger/log.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_common.h>

#include <library/cpp/string_utils/quote/quote.h>

#include <thread>
#include <unordered_map>

namespace {
    void Quote(std::string& url, const char* safe = "/") {
        TTempBuf tempBuf(CgiEscapeBufLen(url.size()));
        char* to = tempBuf.Data();

        url.assign(to, Quote(to, TStringBuf(url), safe));
    }

}

namespace NYdb::inline Dev {

using namespace std::chrono_literals;

constexpr int PESSIMIZATION_DISCOVERY_THRESHOLD = 50; // percent of endpoints pessimized by transport error to start recheck
constexpr TDuration ENDPOINT_UPDATE_PERIOD = TDuration::Minutes(1); // period to perform endpoints update in "normal" case
constexpr TDeadline::Duration DISCOVERY_RECHECK_PERIOD = 5s; // period to run periodic discovery task

TDbDriverState::TDbDriverState(
    const std::string& database,
    const std::string& discoveryEndpoint,
    EDiscoveryMode discoveryMode,
    const TSslCredentials& sslCredentials,
    std::shared_ptr<IInternalClient> client
)
    : Database(database)
    , DiscoveryEndpoint(discoveryEndpoint)
    , DiscoveryMode(discoveryMode)
    , SslCredentials(sslCredentials)
    , Client(std::move(client))
    , EndpointPool([this] {
        return Client->GetEndpoints(shared_from_this());
    }, Client.get())
    , StatCollector(
        database,
        Client->GetMetricRegistry(),
        Client->GetExternalMetricRegistry(),
        discoveryEndpoint
    )
    , Log(Client->GetLog())
    , DiscoveryCompletedPromise(NThreading::NewPromise<void>())
{
    EndpointPool.SetStatCollector(StatCollector);
    Log.SetFormatter(GetPrefixLogFormatter(GetDatabaseLogPrefix(Database)));
}

void TDbDriverState::InitCredentials(
    std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory
) {
    Credentials.Provider = credentialsProviderFactory->CreateProvider(weak_from_this());
#ifndef YDB_GRPC_UNSECURE_AUTH
    Credentials.CallCredentials = grpc::MetadataCredentialsFromPlugin(
        std::unique_ptr<grpc::MetadataCredentialsPlugin>(new TYdbAuthenticator(Credentials.Provider)));
#endif
}

NThreading::TFuture<void> TDbDriverState::GetCredentialsReady() const {
    return Credentials.Provider->GetAuthInfoAsync().IgnoreResult();
}

std::shared_ptr<ICredentialsProvider> TDbDriverState::GetCredentialsProvider() const {
    return Credentials.Provider;
}

#ifndef YDB_GRPC_UNSECURE_AUTH
std::shared_ptr<grpc::CallCredentials> TDbDriverState::GetCallCredentials() const {
    return Credentials.CallCredentials;
}
#endif

bool TDbDriverState::AreClientTlsCredentialsValid() const {
    std::call_once(ClientTlsValidationOnceFlag_, [this]() {
        ClientTlsValidationDetail_.clear();
        grpc::SslCredentialsOptions sslOptions{
            .pem_root_certs = NYdb::TStringType{SslCredentials.CaCert},
            .pem_private_key = NYdb::TStringType{SslCredentials.PrivateKey},
            .pem_cert_chain = NYdb::TStringType{SslCredentials.Cert}
        };
        ClientTlsCredentialsValid_ = NYdbGrpc::ValidateTlsCredentials(sslOptions, ClientTlsValidationDetail_);
    });
    return ClientTlsCredentialsValid_;
}

const std::string& TDbDriverState::GetClientTlsValidationDetail() const {
    return ClientTlsValidationDetail_;
}

void TDbDriverState::ForEachEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const {
    EndpointPool.ForEachEndpoint(cb, tag);
}

void TDbDriverState::ForEachLocalEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const {
    EndpointPool.ForEachLocalEndpoint(cb, tag);
}

void TDbDriverState::ForEachForeignEndpoint(const TEndpointElectorSafe::THandleCb& cb, const void* tag) const {
    EndpointPool.ForEachForeignEndpoint(cb, tag);
}

TBalancingPolicy::TImpl::EPolicyType TDbDriverState::GetBalancingPolicyType() const {
    return EndpointPool.GetBalancingPolicyType();
}

std::string TDbDriverState::GetEndpoint() const {
    return EndpointPool.GetEndpoint(TEndpointKey()).Endpoint;
}

NThreading::TFuture<void> TDbDriverState::DiscoveryCompleted() const {
    return DiscoveryCompletedPromise.GetFuture();
}
void TDbDriverState::SignalDiscoveryCompleted() {
    DiscoveryCompletedPromise.TrySetValue();
}

TPeriodicCb CreatePeriodicDiscoveryTask(TDbDriverState::TPtr driverState) {
    auto weak = std::weak_ptr<TDbDriverState>(driverState);
    return [weak](NYdb::NIssue::TIssues&&, EStatus status) {
        if (status != EStatus::SUCCESS) {
            return false;
        }

        TDbDriverState::TPtr strong = weak.lock();
        if (!strong) {
            return false;
        } else {

            bool pessThreshold = strong->EndpointPool.GetPessimizationRatio() > PESSIMIZATION_DISCOVERY_THRESHOLD;
            bool expiration = strong->EndpointPool.TimeSinceLastUpdate() > ENDPOINT_UPDATE_PERIOD;

            if (pessThreshold) {
                strong->StatCollector.IncDiscoveryDuePessimization();
            }
            if (expiration) {
                strong->StatCollector.IncDiscoveryDueExpiration();
            }

            if (pessThreshold || expiration) {
                auto asyncResult = strong->EndpointPool.UpdateAsync(strong);
                // true - we were first who run UpdateAsync
                if (asyncResult.second) {
                    auto cb = [strong](const NThreading::TFuture<TEndpointUpdateResult>& future) {
                        const auto& updateResult = future.GetValue();
#ifndef YDB_GRPC_BYPASS_CHANNEL_POOL
                        strong->Client->DeleteChannels(updateResult.Removed);
#endif
                        if (strong->DiscoveryMode == EDiscoveryMode::Sync) {
                            std::unique_lock guard(strong->LastDiscoveryStatusRWLock);
                            strong->LastDiscoveryStatus = updateResult.DiscoveryStatus;
                        }
                    };
                    asyncResult.first.Subscribe(std::move(cb));
                }
            }
        }
        return true;
    };
}

TDbDriverStateTracker::TDbDriverStateTracker(IInternalClient* client)
    : DiscoveryClient_(client)
{}

TDbDriverStatePtr TDbDriverStateTracker::GetDriverState(
    const std::string& database,
    const std::string& discoveryEndpoint,
    EDiscoveryMode discoveryMode,
    const TSslCredentials& sslCredentials,
    std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory
) {
    std::string clientIdentity;
    if (credentialsProviderFactory) {
        clientIdentity = credentialsProviderFactory->GetClientIdentity();
    }
    std::string quotedDatabase = database;
    Quote(quotedDatabase);
    const TStateKey key{quotedDatabase, discoveryEndpoint, clientIdentity, discoveryMode, sslCredentials};
    TDbDriverStatePtr strongState;
    {
        std::unique_lock lock(Lock_);
        Notify_.wait(lock, [&] {
            auto it = States_.find(key);
            return it == States_.end() || !it->second.Initializing;
        });
        if (auto it = States_.find(key); it != States_.end()) {
            if (auto state = it->second.State.lock()) {
                return state;
            }
        }
        std::erase_if(States_, [](const auto& entry) {
            return !entry.second.Initializing && entry.second.State.expired();
        });
        States_[key].Initializing = true;
    }

    try {
        strongState = std::make_shared<TDbDriverState>(
            quotedDatabase,
            discoveryEndpoint,
            discoveryMode,
            sslCredentials,
            DiscoveryClient_->shared_from_this());
        strongState->InitCredentials(credentialsProviderFactory
            ? std::move(credentialsProviderFactory)
            : CreateInsecureCredentialsProviderFactory());
        if (discoveryMode != EDiscoveryMode::Off) {
            DiscoveryClient_->AddPeriodicTask(CreatePeriodicDiscoveryTask(strongState), DISCOVERY_RECHECK_PERIOD);
        }
    } catch (...) {
        {
            std::unique_lock lock(Lock_);
            States_.erase(key);
        }
        Notify_.notify_all();
        throw;
    }
    {
        std::unique_lock lock(Lock_);
        States_[key] = {strongState, false};
    }
    Notify_.notify_all();

    if (strongState->DiscoveryMode != EDiscoveryMode::Off) {
        auto updateResult = strongState->EndpointPool.UpdateAsync(strongState);
        if (updateResult.second) {
            auto cb = [strongState](const NThreading::TFuture<TEndpointUpdateResult>&) {
                strongState->SignalDiscoveryCompleted();
            };
            updateResult.first.Subscribe(cb);
        }

        if (strongState->DiscoveryMode == EDiscoveryMode::Sync) {
            const auto& discoveryStatus = updateResult.first.GetValueSync().DiscoveryStatus;
            // Almost always true, except the situation when the current thread was
            // preempted just before UpdateAsync call and other one get
            // state from cache and call UpdateAsync before us.
            if (Y_LIKELY(updateResult.second)) {
                std::unique_lock guard(strongState->LastDiscoveryStatusRWLock);
                strongState->LastDiscoveryStatus = discoveryStatus;
            }
        }
    }

    return strongState;
}

void TDbDriverState::AddPeriodicTask(TPeriodicCb&& cb, TDeadline::Duration period) {
    Client->AddPeriodicTask(std::move(cb), period);
}

void TDbDriverState::PostToResponseQueue(TPostTaskCb&& f) {
    Client->PostToResponseQueue(std::move(f));
}

void TDbDriverStateTracker::SetMetricRegistry(NMonitoring::TMetricRegistry *sensorsRegistry) {
    std::vector<std::weak_ptr<TDbDriverState>> states;
    {
        std::shared_lock lock(Lock_);
        states.reserve(States_.size());
        for (auto& weak : States_) {
            states.push_back(weak.second.State);
        }
    }

    for (auto& weak : states) {
        if (auto strong = weak.lock()) {
            strong->StatCollector.SetMetricRegistry(sensorsRegistry);
            strong->EndpointPool.SetStatCollector(strong->StatCollector);
        }
    }
}

} // namespace NYdb
