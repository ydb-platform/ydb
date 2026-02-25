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

    NYdb::NTopic::TProducerSettings producerSettings;
    producerSettings.Path(TOPIC);
    producerSettings.Codec(NYdb::NTopic::ECodec::RAW);
    producerSettings.ProducerIdPrefix("producer_basic");
    producerSettings.PartitionChooserStrategy(NYdb::NTopic::TProducerSettings::EPartitionChooserStrategy::Bound);
    producerSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
    producerSettings.MaxBlock(TDuration::Seconds(30));

    auto producer = topicClient.CreateProducer(producerSettings);

    NYdb::NTopic::TWriteMessage writeMessage("message");
    writeMessage.Key("key1");

    auto writeResult = producer->Write(std::move(writeMessage));
    Y_ASSERT(writeResult.IsSuccess());
}
