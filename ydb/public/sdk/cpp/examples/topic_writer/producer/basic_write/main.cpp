#include <thread>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/generic/serialized_enum.h>

std::shared_ptr<NYdb::NTopic::IProducer> CreateProducer(const std::string& topic, NYdb::NTopic::TTopicClient& topicClient) {
    NYdb::NTopic::TProducerSettings producerSettings;
    producerSettings.Path(topic);
    producerSettings.Codec(NYdb::NTopic::ECodec::GZIP);
    producerSettings.ProducerIdPrefix("producer_basic");
    producerSettings.PartitionChooserStrategy(NYdb::NTopic::TProducerSettings::EPartitionChooserStrategy::Bound);
    producerSettings.SubSessionIdleTimeout(TDuration::Seconds(30));
    producerSettings.MaxBlock(TDuration::Seconds(30));
    producerSettings.MaxMemoryUsage(100_MB);
    return topicClient.CreateProducer(producerSettings);
}

template<typename T>
std::string GetResultStatus(const T& writeResult) {
    return std::string(NEnumSerializationRuntime::ToStringBuf(writeResult.Status));
}

std::string GetErrorMessage(const NYdb::NTopic::TFlushResult& result) {
    std::string errorMessage = "error occurred while flushing messages";
    errorMessage += ", flush status: " + GetResultStatus(result);
    errorMessage += ", last written sequence number: " + ToString(result.LastWrittenSeqNo);
    if (result.ClosedDescription) {
        errorMessage += ", producer is closed: ";
        errorMessage += result.ClosedDescription.value().DebugString();
    }
    return errorMessage;
}

std::string GetErrorMessage(const NYdb::NTopic::TWriteResult& result) {
    std::string errorMessage = "error occurred while writing message";
    errorMessage += ", write status: " + GetResultStatus(result);
    if (result.ErrorMessage) {
        errorMessage += ", reason: ";
        errorMessage += result.ErrorMessage.value();
    }
    if (result.ClosedDescription) {
        errorMessage += ", producer is closed: ";
        errorMessage += result.ClosedDescription.value().DebugString();
    }
    return errorMessage;
}

void WriteWithHandlingResult(std::shared_ptr<NYdb::NTopic::IProducer> producer, NYdb::NTopic::TWriteMessage&& writeMessage) {
    static constexpr size_t MAX_RETRIES = 10;

    for (size_t retries = 0; retries < MAX_RETRIES; retries++) {
        auto writeResult = producer->Write(std::move(writeMessage));
        if (writeResult.IsSuccess()) {
            // if write was successful, we can continue writing messages
            continue;
        }

        if (writeResult.IsError()) {
            // this means that some non retryable error occurred, for example, producer was closed due to user error
            // in this case we need to stop retrying and see the close description (to simplify the example, we just print it to standard error)
            std::cerr << GetErrorMessage(writeResult) << std::endl;
            return;
        }

        if (writeResult.IsTimeout()) {
            // when timeout occurs this means that producer's buffer is overloaded by memory (see MaxMemoryUsage setting)
            // so we need to wait for some time and try to write again later
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }
    }

    auto flushResult = producer->Flush().GetValueSync();
    if (flushResult.IsSuccess()) {
        // if flush was successful, we can return, because all messages were written to the server
        return;
    }

    if (flushResult.IsClosed()) {
        // if flush was not successful, this means that producer was closed due to non retryable error
        // in this case we should see the close description (to simplify the example, we just print it to standard error)
        std::cerr << GetErrorMessage(flushResult) << std::endl;
    }
}

int main() {
    const std::string ENDPOINT = "HOST:PORT";
    const std::string DATABASE = "DATABASE";
    const std::string TOPIC = "PATH/TO/TOPIC";

    NYdb::TDriverConfig config;
    config.SetEndpoint(ENDPOINT);
    config.SetDatabase(DATABASE);
    NYdb::TDriver driver(config);

    NYdb::NTopic::TTopicClient topicClient(driver);

    auto producer = CreateProducer(TOPIC, topicClient);
    auto messageData = std::string(1_KB, 'a');

    for (int i = 0; i < 10; i++) {
        NYdb::NTopic::TWriteMessage writeMessage(messageData);
        writeMessage.Key("key" + ToString(i));
        WriteWithHandlingResult(producer, std::move(writeMessage));
    }
    return 0;
}
