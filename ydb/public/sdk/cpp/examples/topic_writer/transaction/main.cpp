#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

int main() {
    const std::string ENDPOINT = "HOST:PORT";
    const std::string DATABASE = "DATABASE";
    const std::string TOPIC = "PATH/TO/TOPIC";

    NYdb::TDriverConfig config;
    config.SetEndpoint(ENDPOINT);
    config.SetDatabase(DATABASE);
    NYdb::TDriver driver(config);

    NYdb::NQuery::TQueryClient queryClient(driver);
    auto getSessionResult = queryClient.GetSession().GetValueSync();
    NYdb::NStatusHelpers::ThrowOnError(getSessionResult);
    auto session = getSessionResult.GetSession();

    NYdb::NTopic::TTopicClient topicClient(driver);

    auto topicSession = topicClient.CreateSimpleBlockingWriteSession(
        NYdb::NTopic::TWriteSessionSettings()
            .Path(TOPIC)
            .DeduplicationEnabled(true)
    );

    auto beginTxResult = session.BeginTransaction(NYdb::NQuery::TTxSettings()).GetValueSync();
    NYdb::NStatusHelpers::ThrowOnError(beginTxResult);
    auto tx = beginTxResult.GetTransaction();

    NYdb::NTopic::TWriteMessage writeMessage("message");

    topicSession->Write(std::move(writeMessage), &tx);

    tx.Commit().GetValueSync();
}
