#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

void ThrowOnError(const NYdb::TStatus& status)
{
    if (status.IsSuccess()) {
        return;
    }

    ythrow yexception() << status;
}

int main()
{
    const TString ENDPOINT = "HOST:PORT";
    const TString DATABASE = "DATABASE";
    const TString TOPIC = "PATH/TO/TOPIC";

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
