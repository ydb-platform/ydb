#include "federated_topic_impl.h"

#include "federated_read_session.h"
// #include "federated_write_session.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/read_session.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.h>

namespace NYdb::NFederatedTopic {

std::shared_ptr<IFederatedReadSession>
TFederatedTopicClient::TImpl::CreateFederatedReadSession(const TFederatedReadSessionSettings& settings) {
    InitObserver();
    auto session = std::make_shared<TFederatedReadSession>(settings, Connections, ClientSettings, GetObserver());
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
