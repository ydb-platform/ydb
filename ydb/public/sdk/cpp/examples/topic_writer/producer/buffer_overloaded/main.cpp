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
    producerSettings.MaxMemoryUsage(1_KB);

    auto producer = topicClient.CreateProducer(producerSettings);

    auto messageData = std::string(1_KB, 'a');
    NYdb::NTopic::TWriteMessage writeMessage(messageData);
    writeMessage.Key("key1");

    auto writeResult = producer->Write(std::move(writeMessage));
    Y_ASSERT(writeResult.IsSuccess());

    for (int i = 0; i < 10; i++) {
        while (true) {
            auto writeResult = producer->Write(std::move(writeMessage));
            if (writeResult.IsSuccess()) {
                break;
            }

            if (writeResult.IsClosed()) {
                std::cerr << "Producer is closed in unexpected way" << std::endl;
                return 1;
            }

            if (writeResult.IsError()) {
                std::cerr << "Write failed with error: " << writeResult.ErrorMessage.value() << std::endl;
                return 1;
            }

            if (writeResult.IsTimeout()) {
                Y_ASSERT(producer->Flush().GetValueSync().IsSuccess());
            }
        }
    }
    return 0;
}
