#include "local_federated_topic_client.h"
#include "local_topic_client_helpers.h"
#include "local_topic_write_session.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYdb;
using namespace NYdb::NFederatedTopic;
using namespace NYdb::NTopic;

class TLocalFederatedTopicClient final : public TLocalTopicClientBase, public NYql::IFederatedTopicClient {
    using TBase = TLocalTopicClientBase;

public:
    using TBase::TBase;

    NThreading::TFuture<std::vector<TFederatedTopicClient::TClusterInfo>> GetAllTopicClusters() final {
        Y_VALIDATE(false, __func__ << " is not implemented");
    }

    std::shared_ptr<IWriteSession> CreateWriteSession(const TFederatedWriteSessionSettings& settings) final {
        return CreateLocalTopicWriteSession({
            .ActorSystem = ActorSystem,
            .Database = Database,
            .CredentialsProvider = CredentialsProvider,
        }, settings);
    }
};

} // anonymous namespace

NYql::IFederatedTopicClient::TPtr CreateLocalFederatedTopicClient(const TLocalTopicClientSettings& localSettings, const TFederatedTopicClientSettings& clientSettings) {
    return MakeIntrusive<TLocalFederatedTopicClient>(localSettings, clientSettings);
}

} // namespace NKikimr::NKqp
