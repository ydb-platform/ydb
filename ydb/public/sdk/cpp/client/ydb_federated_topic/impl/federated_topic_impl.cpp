#include "federated_topic_impl.h"

#include "federated_read_session.h"
#include "federated_write_session.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.h>

namespace NYdb::NFederatedTopic {

std::shared_ptr<IFederatedReadSession>
TFederatedTopicClient::TImpl::CreateReadSession(const TFederatedReadSessionSettings& settings) {
    InitObserver();
    auto session = std::make_shared<TFederatedReadSession>(settings, Connections, ClientSettings, GetObserver());
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
    auto session = std::make_shared<TFederatedWriteSession>(splitSettings, Connections, ClientSettings, GetObserver());
    session->Start();
    return std::move(session);

}

void TFederatedTopicClient::TImpl::InitObserver() {
    with_lock(Lock) {
        if (!Observer || Observer->IsStale()) {
            Observer = std::make_shared<TFederatedDbObserver>(Connections, ClientSettings);
            Observer->Start();
        }
    }
}

}
