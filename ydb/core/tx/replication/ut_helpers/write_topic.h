#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NKikimr::NReplication::NTestHelpers {

template <typename Env>
bool WriteTopic(const Env& env, const TString& topicPath, const TString& data) {
    NYdb::NTopic::TTopicClient client(env.GetDriver(), NYdb::NTopic::TTopicClientSettings()
        .DiscoveryEndpoint(env.GetEndpoint())
        .Database(env.GetDatabase())
    );

    auto session = client.CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
        .Path(topicPath)
        .ProducerId("producer")
        .MessageGroupId("producer")
    );

    const auto result = session->Write(data);
    session->Close();

    return result;
}

}
