#include "common.h"

#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>

#include <contrib/libs/librdkafka/src-cpp/rdkafkacpp.h>
#include <contrib/libs/librdkafka/src/rdkafka.h>
#include <contrib/libs/librdkafka/src/rdkafka_mock.h>
#include <fmt/format.h>
#include <library/cpp/streams/zstd/zstd.h>
#include <util/stream/str.h>

#include <type_traits>

namespace NKikimr::NKqp {
namespace {

void AssertKafkaSuccess(RdKafka::ErrorCode status) {
    UNIT_ASSERT_C(status == RdKafka::ERR_NO_ERROR, RdKafka::err2str(status));
}

// A Kafka protocol server listening on TCP, shared by producer and SQL readers.
// No mocked gateway or read session is injected into the query execution path.
class TKafkaCluster {
public:
    TKafkaCluster() {
        char error[512];
        auto* config = rd_kafka_conf_new();
        rd_kafka_conf_set(config, "log_level", "0", error, sizeof(error));
        Owner = rd_kafka_new(RD_KAFKA_PRODUCER, config, error, sizeof(error));
        UNIT_ASSERT_C(Owner, error);
        Cluster = rd_kafka_mock_cluster_new(Owner, 3);
        UNIT_ASSERT(Cluster);
        const auto status = rd_kafka_mock_topic_create(Cluster, Topic, 2, 1);
        UNIT_ASSERT_C(status == RD_KAFKA_RESP_ERR_NO_ERROR, rd_kafka_err2str(status));
        Producer = CreateClient<RdKafka::Producer>();
    }

    ~TKafkaCluster() {
        Producer.reset();
        rd_kafka_mock_cluster_destroy(Cluster);
        rd_kafka_destroy(Owner);
    }

    TString Brokers() const {
        return rd_kafka_mock_cluster_bootstraps(Cluster);
    }

    void Write(TStringBuf prefix) {
        for (int partition = 0; partition < 2; ++partition) {
            const auto value = fmt::format("{}-{}", prefix, partition);
            AssertKafkaSuccess(Producer->produce(
                Topic, partition, RdKafka::Producer::RK_MSG_COPY, const_cast<char*>(value.data()), value.size(), nullptr, 0, 0, nullptr));
        }
        AssertKafkaSuccess(Producer->flush(10000));
    }

    void WaitCommitted(ui64 nextOffset) const {
        auto consumer = CreateClient<RdKafka::KafkaConsumer>();
        NTestUtils::WaitFor(TDuration::Seconds(30), "Kafka group commits after sink acknowledgement", [&](TString& error) {
            std::vector<std::unique_ptr<RdKafka::TopicPartition>> owned;
            std::vector<RdKafka::TopicPartition*> partitions;
            for (int id = 0; id < 2; ++id) {
                owned.emplace_back(RdKafka::TopicPartition::create(Topic, id));
                partitions.push_back(owned.back().get());
            }
            const auto status = consumer->committed(partitions, 1000);
            error = TString(RdKafka::err2str(status));
            bool ready = status == RdKafka::ERR_NO_ERROR;
            for (const auto* partition : partitions) {
                error += TStringBuilder() << "; partition " << partition->partition() << ": " << partition->offset();
                ready &= partition->err() == RdKafka::ERR_NO_ERROR && partition->offset() == static_cast<i64>(nextOffset);
            }
            return ready;
        });
        AssertKafkaSuccess(consumer->close());
    }

    static constexpr const char* Topic = "streaming-input";
    static constexpr const char* Group = "streaming-reader";

private:
    template <class T>
    std::unique_ptr<T> CreateClient() const {
        std::unique_ptr<RdKafka::Conf> config(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        std::string error;
        UNIT_ASSERT_C(config->set("bootstrap.servers", std::string(Brokers()), error) == RdKafka::Conf::CONF_OK, error);
        UNIT_ASSERT_C(config->set("enable.metrics.push", "false", error) == RdKafka::Conf::CONF_OK, error);
        if constexpr (std::is_same_v<T, RdKafka::KafkaConsumer>) {
            UNIT_ASSERT_C(config->set("group.id", Group, error) == RdKafka::Conf::CONF_OK, error);
            UNIT_ASSERT_C(config->set("enable.auto.commit", "false", error) == RdKafka::Conf::CONF_OK, error);
        }
        std::unique_ptr<T> client(T::create(config.get(), error));
        UNIT_ASSERT_C(client, error);
        return client;
    }

    rd_kafka_t* Owner = nullptr;
    rd_kafka_mock_cluster_t* Cluster = nullptr;
    std::unique_ptr<RdKafka::Producer> Producer;
};

class TKafkaStreamingFixture: public TStreamingTestFixture {
public:
    void CheckSourceStateHasNoOffsets(const TString& query) {
        const auto graphId = GetStreamingQueryCheckpointId(query);
        const auto results = ExecQuery(fmt::format(R"(
            SELECT blob, is_compressed, blob_seq_num
            FROM `.metadata/streaming/checkpoints/states` WHERE graph_id = "{}";
        )", graphId));
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        NYdb::TResultSetParser rows(results.front());
        bool found = false;
        while (rows.TryNextRow()) {
            // These tiny test states fit in one storage row.
            UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("blob_seq_num").GetOptionalUint64().value_or(0), 0);
            TString blob = rows.ColumnParser("blob").GetOptionalString().value();
            if (rows.ColumnParser("is_compressed").GetOptionalBool().value_or(false)) {
                TStringInput input(blob);
                TZstdDecompress decompressor(&input);
                blob = decompressor.ReadAll();
            }
            NYql::NDq::TComputeActorState state;
            UNIT_ASSERT(state.ParseFromString(blob));
            for (const auto& source : state.Sources) {
                found = true;
                for (const auto& data : source.Data) {
                    UNIT_ASSERT_C(data.Blob.empty(), "Kafka source must not persist offsets in YDB checkpoints");
                }
            }
        }
        UNIT_ASSERT_C(found, "No source checkpoint was inspected");
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(KafkaStreamingQueries) {
    Y_UNIT_TEST_TWIN_F(StopAndResumeFromKafkaGroup, NewQuery, TKafkaStreamingFixture) {
        TKafkaCluster kafka;
        const auto output = MakeExternalName("kafkaOutput");
        CreateScopedTopic(output);
        CreatePqSource("sink");
        ExecQuery(fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE kafkaSource WITH (
                SOURCE_TYPE = "Kafka", LOCATION = "{}", AUTH_METHOD = "NONE"
            );
        )", kafka.Brokers()));

        const auto create = [&](TStringBuf name) {
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{}` AS DO BEGIN
                    PRAGMA ydb.MaxTasksPerStage = "1";
                    PRAGMA kafka.Consumer = "{}";
                    INSERT INTO sink.`{}`
                    SELECT Data FROM kafkaSource.`{}` WITH (FORMAT = "raw");
                END DO;
            )", name, TKafkaCluster::Group, output, TKafkaCluster::Topic));
        };

        // No group or consumer is provisioned before starting the query.
        kafka.Write("before");
        create("kafkaIngest");
        ReadTopicMessages(output, { "before-0", "before-1" }, TInstant::Zero(), true);
        kafka.WaitCommitted(1);
        CheckSourceStateHasNoOffsets("kafkaIngest");

        ExecQuery("ALTER STREAMING QUERY kafkaIngest SET (RUN = FALSE)");
        WaitStreamingQueryStatus("kafkaIngest", "STOPPED");
        kafka.Write("after");
        EnsureTopicEndOffset(output, 2);

        if constexpr (NewQuery) {
            // A new query has no source checkpoint to restore. The same group
            // must still resume from Kafka's offsets, without replaying 'before'.
            create("kafkaResumed");
        } else {
            ExecQuery("ALTER STREAMING QUERY kafkaIngest SET (RUN = TRUE)");
        }
        ReadTopicMessages(output, { "before-0", "before-1", "after-0", "after-1" }, TInstant::Zero(), true);
        kafka.WaitCommitted(2);
        EnsureTopicEndOffset(output, 4);
        const TString active = NewQuery ? "kafkaResumed" : "kafkaIngest";
        CheckSourceStateHasNoOffsets(active);
        ExecQuery(TStringBuilder() << "ALTER STREAMING QUERY " << active << " SET (RUN = FALSE)");
        WaitStreamingQueryStatus(active, "STOPPED");
    }
}   // Y_UNIT_TEST_SUITE(KafkaStreamingQueries)

}   // namespace NKikimr::NKqp
