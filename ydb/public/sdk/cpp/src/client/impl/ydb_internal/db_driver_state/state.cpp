#define INCLUDE_YDB_INTERNAL_H
#include "state.h"

#include <ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <src/client/impl/ydb_internal/logger/log.h>

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

namespace NYdb::inline V3 {

constexpr int PESSIMIZATION_DISCOVERY_THRESHOLD = 50; // percent of endpoints pessimized by transport error to start recheck
constexpr TDuration ENDPOINT_UPDATE_PERIOD = TDuration::Minutes(1); // period to perform endpoints update in "normal" case
constexpr TDuration DISCOVERY_RECHECK_PERIOD = TDuration::Seconds(5); // period to run periodic discovery task

TDbDriverState::TDbDriverState(
    const std::string& database,
    const std::string& discoveryEndpoint,
    EDiscoveryMode discoveryMode,
    const TSslCredentials& sslCredentials,
    IInternalClient* client
)
    : Database(database)
    , DiscoveryEndpoint(discoveryEndpoint)
    , DiscoveryMode(discoveryMode)
    , SslCredentials(sslCredentials)
    , Client(client)
    , EndpointPool([this, client]() mutable {
        // this callback will be called just after shared_ptr initialization
        // so this call is safe
        auto self = shared_from_this();
        return client->GetEndpoints(self);
    }, client)
    , StatCollector(database, client->GetMetricRegistry())
    , Log(Client->GetLog())
    , DiscoveryCompletedPromise(NThreading::NewPromise<void>())
{
    EndpointPool.SetStatCollector(StatCollector);
    Log.SetFormatter(GetPrefixLogFormatter(GetDatabaseLogPrefix(Database)));
}

void TDbDriverState::SetCredentialsProvider(std::shared_ptr<ICredentialsProvider> credentialsProvider) {
    CredentialsProvider = std::move(credentialsProvider);
#ifndef YDB_GRPC_UNSECURE_AUTH
    CallCredentials = grpc::MetadataCredentialsFromPlugin(
        std::unique_ptr<grpc::MetadataCredentialsPlugin>(new TYdbAuthenticator(CredentialsProvider)));
#endif
}

void TDbDriverState::AddCb(TCb&& cb, ENotifyType type) {
    std::lock_guard lock(NotifyCbsLock);
    NotifyCbs[static_cast<size_t>(type)].emplace_back(std::move(cb));
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

EBalancingPolicy TDbDriverState::GetBalancingPolicy() const {
    return EndpointPool.GetBalancingPolicy();
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
                auto asyncResult = strong->EndpointPool.UpdateAsync();
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
    std::string database,
    std::string discoveryEndpoint,
    EDiscoveryMode discoveryMode,
    const TSslCredentials& sslCredentials,
    std::shared_ptr<ICredentialsProviderFactory> credentialsProviderFactory
) {
    std::string clientIdentity;
    if (credentialsProviderFactory) {
        clientIdentity = credentialsProviderFactory->GetClientIdentity();
    }
    Quote(database);
    const TStateKey key{database, discoveryEndpoint, clientIdentity, discoveryMode, sslCredentials};
    {
        std::shared_lock lock(Lock_);
        auto state = States_.find(key);
        if (state != States_.end()) {
            auto strong = state->second.lock();
            if (strong) {
                return strong;
            }
            // If we can't promote to shared see bellow
        }
    }
    TDbDriverStatePtr strongState;
    for (;;) {
        std::unique_lock lock(Lock_);
        {
            auto state = States_.find(key);
            if (state != States_.end()) {
                auto strong = state->second.lock();
                if (strong) {
                    return strong;
                } else {
                    // We could find state record, but couldn't promote weak to shared
                    // this means weak ptr already expired but dtor hasn't been
                    // called yet. Likely other thread now is waiting on mutex to
                    // remove expired record from hashmap. So give him chance
                    // to do it after that we will be able to create new state
                    lock.unlock();
                    std::this_thread::yield();
                    continue;
                }
            }
        }
        {
            auto deleter = [this, key](TDbDriverState* p) {
                {
                    std::unique_lock lock(Lock_);
                    States_.erase(key);
                }
                delete p;
            };
            strongState = std::shared_ptr<TDbDriverState>(
                new TDbDriverState(
                    database,
                    discoveryEndpoint,
                    discoveryMode,
                    sslCredentials,
                    DiscoveryClient_),
                deleter);

            strongState->SetCredentialsProvider(
                credentialsProviderFactory
                    ? credentialsProviderFactory->CreateProvider(strongState)
                    : CreateInsecureCredentialsProviderFactory()->CreateProvider(strongState));

            if (discoveryMode != EDiscoveryMode::Off) {
                DiscoveryClient_->AddPeriodicTask(CreatePeriodicDiscoveryTask(strongState), DISCOVERY_RECHECK_PERIOD);
            }
            Y_ABORT_UNLESS(States_.emplace(key, strongState).second);
            break;
        }
    }

    if (strongState->DiscoveryMode != EDiscoveryMode::Off) {
        auto updateResult = strongState->EndpointPool.UpdateAsync();
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

void TDbDriverState::AddPeriodicTask(TPeriodicCb&& cb, TDuration period) {
    Client->AddPeriodicTask(std::move(cb), period);
}

NThreading::TFuture<void> TDbDriverStateTracker::SendNotification(
    TDbDriverState::ENotifyType type
) {
    std::vector<std::weak_ptr<TDbDriverState>> states;
    {
        std::shared_lock lock(Lock_);
        states.reserve(States_.size());
        for (auto& weak : States_) {
            states.push_back(weak.second);
        }
    }
    std::vector<NThreading::TFuture<void>> results;
    for (auto& state : states) {
        auto strong = state.lock();
        if (strong) {
            std::lock_guard lock(strong->NotifyCbsLock);
            for (auto& cb : strong->NotifyCbs[static_cast<size_t>(type)]) {
                if (cb) {
                    auto future = cb();
                    if (!future.HasException()) {
                        results.push_back(future);
                    }
                    //TODO: Add loger
                }
            }
        }
    }
    return NThreading::WaitExceptionOrAll(results);
}

void TDbDriverStateTracker::SetMetricRegistry(NMonitoring::TMetricRegistry *sensorsRegistry) {
    std::vector<std::weak_ptr<TDbDriverState>> states;
    {
        std::shared_lock lock(Lock_);
        states.reserve(States_.size());
        for (auto& weak : States_) {
            states.push_back(weak.second);
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
