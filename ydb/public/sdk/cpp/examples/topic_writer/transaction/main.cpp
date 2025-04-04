#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

int main() {
    const std::string ENDPOINT = "HOST:PORT";
    const std::string DATABASE = "DATABASE";
    const std::string TOPIC = "PATH/TO/TOPIC";

    NYdb::TDriver driver(NYdb::TDriverConfig()
        .SetEndpoint(ENDPOINT)
        .SetDatabase(DATABASE)
    );

    NYdb::NQuery::TQueryClient queryClient(driver);

    auto getSessionResult = queryClient.GetSession().GetValueSync();
    NYdb::NStatusHelpers::ThrowOnError(getSessionResult);
    auto session = getSessionResult.GetSession();

    NYdb::NTopic::TTopicClient topicClient(driver);

    auto topicSession = topicClient.CreateSimpleBlockingWriteSession(NYdb::NTopic::TWriteSessionSettings()
        .Path(TOPIC)
        .DeduplicationEnabled(true)
    );

    auto beginTxResult = session.BeginTransaction(NYdb::NQuery::TTxSettings()).GetValueSync();
    NYdb::NStatusHelpers::ThrowOnError(beginTxResult);
    auto tx = beginTxResult.GetTransaction();

    NYdb::NTopic::TWriteMessage writeMessage("message");

    topicSession->Write(std::move(writeMessage), &tx);

    NYdb::NStatusHelpers::ThrowOnError(tx.Commit().GetValueSync());
}
