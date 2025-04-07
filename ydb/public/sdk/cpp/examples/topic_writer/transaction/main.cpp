#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

int main()
{
    const std::string ENDPOINT = "HOST:PORT";
    const std::string DATABASE = "DATABASE";
    const std::string TOPIC = "PATH/TO/TOPIC";

    NYdb::TDriverConfig config;
    config.SetEndpoint(ENDPOINT);
    config.SetDatabase(DATABASE);
    NYdb::TDriver driver(config);

    NYdb::NTable::TTableClient tableClient(driver);
    auto getTableSessionResult = tableClient.GetSession().GetValueSync();
    ThrowOnError(getTableSessionResult);
    auto tableSession = getTableSessionResult.GetSession();

    NYdb::NTopic::TTopicClient topicClient(driver);
    auto topicSessionSettings = NYdb::NTopic::TWriteSessionSettings()
        .Path(TOPIC)
        .DeduplicationEnabled(true);
    auto topicSession = topicClient.CreateSimpleBlockingWriteSession(topicSessionSettings);

    auto beginTransactionResult = tableSession.BeginTransaction().GetValueSync();
    ThrowOnError(beginTransactionResult);
    auto transaction = beginTransactionResult.GetTransaction();

    NYdb::NTopic::TWriteMessage writeMessage("message");

    topicSession->Write(std::move(writeMessage), &transaction);

    transaction.Commit().GetValueSync();
}
