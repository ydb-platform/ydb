#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NKikimr::NReplication::NTestHelpers {

namespace NImpl {
template <typename Env, typename Message>
bool WriteTopic(const Env& env, const TString& topicPath, Message&& data) {
    NYdb::NTopic::TTopicClient client(env.GetDriver(), NYdb::NTopic::TTopicClientSettings()
        .DiscoveryEndpoint(env.GetEndpoint())
        .Database(env.GetDatabase())
    );

    auto session = client.CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
        .Path(topicPath)
        .ProducerId("producer")
        .MessageGroupId("producer")
    );

    const auto result = session->Write(std::move(data));
    session->Close();

    return result;
}
} // namespace NImpl {

template <typename Env>
bool WriteTopic(const Env& env, const TString& topicPath, const std::string_view& data) {
    return NImpl::WriteTopic(env, topicPath, data);
}

template <typename Env>
bool WriteTopic(const Env& env, const TString& topicPath, NYdb::NTopic::TWriteMessage&& data) {
    return NImpl::WriteTopic(env, topicPath, std::move(data));
}
}
