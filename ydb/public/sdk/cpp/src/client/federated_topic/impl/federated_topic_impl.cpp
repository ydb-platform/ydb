#include "federated_topic_impl.h"

#include "federated_read_session.h"
#include "federated_write_session.h"

namespace NYdb::inline V3::NFederatedTopic {

NTopic::TTopicClientSettings FromFederated(const TFederatedTopicClientSettings& settings);

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

IOutputStream& operator<<(IOutputStream& out, NTopic::TTopicClientSettings const& settings) {
    out << "{"
        << " Database: " << settings.Database_
        << " DiscoveryEndpoint: " << settings.DiscoveryEndpoint_
        << " DiscoveryMode: " << (settings.DiscoveryMode_ ? (int)*settings.DiscoveryMode_ : -1)
        << " }";
    return out;
}

IOutputStream& operator<<(IOutputStream& out, NTopic::TDescribeTopicSettings const& settings) {
    out << "{"
        << " TraceId: " << settings.TraceId_
        << " ClientTimeout: " << settings.ClientTimeout_
        << " CancelAfter: " << settings.CancelAfter_
        << " ForgetAfter: " << settings.ForgetAfter_
        << " OperationTimeout: " << settings.OperationTimeout_
        << " IncludeLocation: " << settings.IncludeLocation_
        << " IncludeStats: " << settings.IncludeStats_
        << " RequestType: " << settings.RequestType_
        << " }";
    return out;
}

TAsyncDescribeTopicResult TFederatedTopicClient::TImpl::DescribeTopic(const std::string& path, const TDescribeTopicSettings& describeSettings) {
    InitObserver();
    return Observer->WaitForFirstState().Apply([path, describeSettings, self = this] (const auto &) {
            NTopic::TTopicClientSettings settings = FromFederated(self->ClientSettings);

            auto state = self->Observer->GetState();
            for (const auto& db: state->DbInfos) {
                if (db->status() != TDbInfo::Status::DatabaseInfo_Status_AVAILABLE &&
                    db->status() != TDbInfo::Status::DatabaseInfo_Status_READ_ONLY) {
                    continue;
                }
                settings
                    .Database(db->path())
                    .DiscoveryEndpoint(db->endpoint());
                Cerr << "Describe " << settings << " / " << describeSettings << " / " << path << Endl;
                auto subclient = make_shared<NTopic::TTopicClient::TImpl>(self->Connections, settings);
                return subclient->DescribeTopic(!path.empty() && path[0] == '/' ? path : db->path() + "/" + path, describeSettings);
            }
            throw yexception();
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
