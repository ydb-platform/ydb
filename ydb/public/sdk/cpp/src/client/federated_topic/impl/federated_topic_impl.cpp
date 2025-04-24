#include "federated_topic_impl.h"

#include "federated_read_session.h"
#include "federated_write_session.h"

namespace NYdb::inline Dev::NFederatedTopic {

std::shared_ptr<IFederatedReadSession>
TFederatedTopicClient::TImpl::CreateReadSession(const TFederatedReadSessionSettings& settings) {
    InitObserver();
    auto session = std::make_shared<TFederatedReadSession>(settings, Connections, ClientSettings, GetObserver(), ProvidedCodecs);
    session->Start();
    return std::move(session);
}

// std::shared_ptr<NTopic::ISimpleBlockingWriteSession>
// TFederatedTopicClient::TImpl::CreateSimpleBlockingWriteSession(const TFederatedWriteSessionSettings& settings) {
//     InitObserver();
//     auto session = std::make_shared<TSimpleBlockingFederatedWriteSession>(settings, Connections, ClientSettings, GetObserver());
//     session->Start();
//     return std::move(session);

// }

std::shared_ptr<NTopic::IWriteSession>
TFederatedTopicClient::TImpl::CreateWriteSession(const TFederatedWriteSessionSettings& settings) {
    // Split settings.MaxMemoryUsage_ by two.
    // One half goes to subsession. Other half goes to federated session internal buffer.
    const ui64 splitSize = (settings.MaxMemoryUsage_ + 1) / 2;
    TFederatedWriteSessionSettings splitSettings = settings;
    splitSettings.MaxMemoryUsage(splitSize);
    InitObserver();
    with_lock(Lock) {
        if (!splitSettings.EventHandlers_.HandlersExecutor_) {
            splitSettings.EventHandlers_.HandlersExecutor(ClientSettings.DefaultHandlersExecutor_);
        }
    }
    auto session = std::make_shared<TFederatedWriteSession>(
        splitSettings, Connections, ClientSettings, GetObserver(), ProvidedCodecs, GetSubsessionHandlersExecutor());
    session->Start();
    return std::move(session);
}

void TFederatedTopicClient::TImpl::InitObserver() {
    std::lock_guard guard(Lock);
    if (!Observer || Observer->IsStale()) {
        Observer = std::make_shared<TFederatedDbObserver>(Connections, ClientSettings);
        Observer->Start();
    }
}

NThreading::TFuture<std::vector<TFederatedTopicClient::TClusterInfo>> TFederatedTopicClient::TImpl::GetAllClusterInfo() {
    InitObserver();
    return Observer->WaitForFirstState().Apply(
            [weakObserver = std::weak_ptr(Observer)] (const auto& ) {
                std::vector<TClusterInfo> result;
                auto observer = weakObserver.lock();
                if (!observer) {
                    return result;
                }
                auto state = observer->GetState();
                result.reserve(state->DbInfos.size());
                for (const auto& db: state->DbInfos) {
                    auto& dbinfo = result.emplace_back();
                    switch (db->status()) {
#define TRANSLATE_STATUS(NAME) \
                    case TDbInfo::Status::DatabaseInfo_Status_##NAME: \
                        dbinfo.Status = TClusterInfo::EStatus::NAME; \
                        break
                    TRANSLATE_STATUS(STATUS_UNSPECIFIED);
                    TRANSLATE_STATUS(AVAILABLE);
                    TRANSLATE_STATUS(READ_ONLY);
                    TRANSLATE_STATUS(UNAVAILABLE);
                    default:
                        Y_ENSURE(false /* impossible status */);
                    }
#undef TRANSLATE_STATUS
                    dbinfo.Name = db->name();
                    dbinfo.Endpoint = db->endpoint();
                    dbinfo.Path = db->path();
                }
                return result;
            });
}

auto TFederatedTopicClient::TImpl::GetSubsessionHandlersExecutor() -> NTopic::IExecutor::TPtr {
    with_lock (Lock) {
        if (!SubsessionHandlersExecutor) {
            SubsessionHandlersExecutor = NTopic::CreateThreadPoolExecutor(1);
        }
        return SubsessionHandlersExecutor;
    }
}

}
