#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <variant>

namespace {

    using namespace std::chrono_literals;

    void PrintStatus(const NYdb::TStatus& status) {
        if (status.IsSuccess()) {
            std::cout << "OK" << std::endl;
        } else {
            std::cerr << status.GetIssues().ToString() << std::endl;
        }
    }

    void ListDirectory(const NYdb::TDriver& driver, const std::string& database) {
        NYdb::NScheme::TSchemeClient client(driver);
        const auto settings = NYdb::NScheme::TListDirectorySettings().ClientTimeout(TDuration::Seconds(2));
        for (;;) {
            PrintStatus(client.ListDirectory(database, settings).GetValueSync());
            std::this_thread::sleep_for(200ms);
        }
    }

    // One request per input line lets the fixture switch while the driver is idle.
    void ListOnDemand(const NYdb::TDriver& driver, const std::string& database) {
        NYdb::NScheme::TSchemeClient client(driver);
        const auto settings = NYdb::NScheme::TListDirectorySettings().ClientTimeout(TDuration::Seconds(3));
        std::string command;
        while (std::getline(std::cin, command)) {
            const auto result = client.ListDirectory(database, settings).GetValueSync();
            if (!result.IsSuccess()) {
                std::cout << "ERROR " << result.GetIssues().ToString() << std::endl;
                continue;
            }
            std::cout << "ENTRIES";
            for (const auto& child : result.GetChildren()) {
                std::cout << ' ' << child.Name;
            }
            std::cout << std::endl;
        }
    }

    void BulkUpsert(const NYdb::TDriver& driver, const std::string& table) {
        NYdb::NTable::TTableClient client(driver);
        const auto settings = NYdb::NTable::TBulkUpsertSettings().ClientTimeout(TDuration::Seconds(2));
        const auto rows = NYdb::TValueBuilder()
                              .BeginList()
                              .AddListItem()
                              .BeginStruct()
                              .AddMember("id")
                              .Uint64(1)
                              .EndStruct()
                              .EndList()
                              .Build();
        for (;;) {
            PrintStatus(client.BulkUpsert(table, NYdb::TValue(rows), settings).GetValueSync());
            std::this_thread::sleep_for(200ms);
        }
    }

    void WriteTopic(const NYdb::TDriver& driver, const std::string& topic) {
        using namespace NYdb::NTopic;
        TTopicClient client(driver);
        auto writer = client.CreateWriteSession(TWriteSessionSettings()
                                                    .Path(topic)
                                                    .ProducerId("probe")
                                                    .MessageGroupId("probe")
                                                    .Codec(ECodec::RAW));

        std::optional<TContinuationToken> token;
        bool awaitingAck = false;
        for (;;) {
            auto event = writer->GetEvent(true);
            if (!event) {
                continue;
            }

            if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&*event)) {
                token = std::move(ready->ContinuationToken);
            } else if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&*event)) {
                if (acks->Acks.size() != 1 || acks->Acks.front().State == TWriteSessionEvent::TWriteAck::EES_DISCARDED) {
                    throw std::runtime_error("Expected one acknowledged topic write");
                }
                awaitingAck = false;
                std::cout << "OK" << std::endl;
                std::this_thread::sleep_for(200ms);
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                throw std::runtime_error(closed->GetIssues().ToString());
            }

            if (token && !awaitingAck) {
                std::string payload = "probe";
                if (std::getenv("YDB_PROBE_ON_DEMAND") && !std::getline(std::cin, payload)) {
                    return;
                }
                writer->Write(std::move(*token), TWriteMessage(payload));
                token.reset();
                awaitingAck = true;
            }
        }
    }

    void ReadTopic(const NYdb::TDriver& driver, const std::string& topic) {
        using namespace NYdb::NTopic;
        TTopicClient client(driver);
        auto reader = client.CreateReadSession(TReadSessionSettings().ConsumerName("probe").AppendTopics(topic));

        for (;;) {
            auto event = reader->GetEvent(true);
            if (!event) {
                continue;
            }

            if (auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                start->Confirm();
            } else if (auto* stop = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                stop->Confirm();
            } else if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (const auto& message : data->GetMessages()) {
                    std::cout << "OK " << message.GetData() << std::endl;
                }
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                throw std::runtime_error(closed->GetIssues().ToString());
            }
        }
    }

} // namespace

int main() {
    const char* endpoint = std::getenv("YDB_ENDPOINT");
    const char* database = std::getenv("YDB_DATABASE");
    if (!endpoint || !database) {
        std::cerr << "Set YDB_ENDPOINT and YDB_DATABASE before starting the probe" << std::endl;
        return 2;
    }

    const char* configuredProbe = std::getenv("YDB_PROBE");
    const std::string probe = configuredProbe ? configuredProbe : "scheme";
    auto config = NYdb::TDriverConfig().SetEndpoint(endpoint).SetDatabase(database);
    if (const char* enabled = std::getenv("YDB_EDS_DISCOVERY"); enabled && std::string(enabled) == "1") {
        config.SetDiscoveryMode(NYdb::EDiscoveryMode::Eds);
    }
    const NYdb::TDriver driver(config);
    const char* configuredTopic = std::getenv("YDB_PROBE_TOPIC");
    const std::string topic = std::string(database) + "/" + (configuredTopic ? configuredTopic : "topic");

    if (probe == "scheme") {
        ListDirectory(driver, database);
    } else if (probe == "list-on-demand") {
        ListOnDemand(driver, database);
    } else if (probe == "bulk-upsert") {
        BulkUpsert(driver, std::string(database) + "/probe");
    } else if (probe == "topic-write") {
        WriteTopic(driver, topic);
    } else if (probe == "topic-read") {
        ReadTopic(driver, topic);
    } else {
        throw std::runtime_error("Unknown probe " + probe);
    }
}
