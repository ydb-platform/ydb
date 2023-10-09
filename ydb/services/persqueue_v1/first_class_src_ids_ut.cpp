#include <ydb/services/persqueue_v1/ut/test_utils.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/data_plane_helpers.h>
#include <ydb/services/persqueue_v1/actors/helpers.h>

namespace NKikimr::NPersQueueTests {
using namespace Tests;
using namespace NPersQueue;


Y_UNIT_TEST_SUITE(TFstClassSrcIdPQTest) {
    THolder<NYdb::TDriver> GetDriver(TTestServer& server) {
        NYdb::TDriverConfig driverCfg;
        driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort);
        return THolder<NYdb::TDriver>(new NYdb::TDriver(driverCfg));
    }

    std::pair<THolder<TTestServer>, THolder<NYdb::TDriver>> Setup(const TString& topic, bool useMapping) {
        auto settings = PQSettings(0);
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        settings.PQConfig.SetUseSrcIdMetaMappingInFirstClass(useMapping);
        auto server = MakeHolder<TTestServer>(settings);

        server->AnnoyingClient->CreateTopicNoLegacy(topic, 1);
        auto ydbDriver = GetDriver(*server);
        {
            auto writer = CreateSimpleWriter(*ydbDriver, topic, "123", 0, "raw");
            Cerr << "Created writer (initially).\n";
            for (auto i = 0; i < 10; ++i) {
                writer->Write("test-message", i + 1);
            }
            Cerr << "Writes done\n";
            writer->Close();
            Cerr << "Writer closed\n";
        }
        return {std::move(server), std::move(ydbDriver)};
    }

    Y_UNIT_TEST(TestTableCreated) {
        TString topic = "/Root/topic-f1";
        auto [server, ydbDriver] = Setup(topic, true); auto& server_ = server; auto& driver = ydbDriver;

        auto checkMetaTable = [&](ui64 expectedCount) {
            TStringBuilder query;
            query << "select * from `/Root/.metadata/TopicPartitionsMapping`";
            auto resultSet = server_->AnnoyingClient->RunYqlDataQuery(query);
            UNIT_ASSERT(resultSet.Defined());
            NYdb::TResultSetParser parser(*resultSet);

            UNIT_ASSERT_VALUES_EQUAL(resultSet->RowsCount(), expectedCount);
            return resultSet->RowsCount();
        };
        checkMetaTable(1);
        server->AnnoyingClient->SetNoConfigMode();
        ui64 currSeqNo = 10;
        auto alterAndCheck = [&](ui32 pCount) {
            server_->AnnoyingClient->AlterTopicNoLegacy(topic, pCount);
            auto writer = CreateSimpleWriter(*driver, topic, "123", 0, "raw");
            UNIT_ASSERT_VALUES_EQUAL(writer->GetInitSeqNo(), currSeqNo);
            writer->Write("test-data", ++currSeqNo);
            writer->Close();
            checkMetaTable(1);
        };
        alterAndCheck(5);
        alterAndCheck(10);
        alterAndCheck(15);
        ydbDriver->Stop(true);
    }

    Y_UNIT_TEST(NoMapping) {
        TString topic = "/Root/topic-f2";
        auto [server, ydbDriver] = Setup(topic, false); auto& server_ = server; auto& driver = ydbDriver;
        Cerr << "Setup done\n";
        auto alterAndCheck = [&](ui32 pCount) {
            server_->AnnoyingClient->AlterTopicNoLegacy(topic, pCount);
            auto writer = CreateSimpleWriter(*driver, topic, "12345");
            UNIT_ASSERT_VALUES_EQUAL(writer->GetInitSeqNo(), 0);
            auto res = writer->Write("test-data", writer->GetInitSeqNo() + 1);
            UNIT_ASSERT(res);
            writer->Close();
        };
        alterAndCheck(2);
        alterAndCheck(4);
        alterAndCheck(12);
        ydbDriver->Stop(true);
    }

    Y_UNIT_TEST(ProperPartitionSelected) {
        TString topic = "/Root/topic-f3";
        auto [server, ydbDriver] = Setup("/Root/otherTopic", false);
        //auto& server_ = server;
        auto& driver = ydbDriver;

        ui32 partCount = 15;
        TString srcId = "mySrcID";
        server->AnnoyingClient->CreateTopicNoLegacy(topic, partCount);

        auto pqClient = NYdb::NTopic::TTopicClient(*driver);
        auto topicSettings = NYdb::NTopic::TAlterTopicSettings();
        topicSettings.BeginAddConsumer("debug");
        auto alterRes = pqClient.AlterTopic(topic, topicSettings).GetValueSync();
        UNIT_ASSERT(alterRes.IsSuccess());

        auto partExpected = NKikimr::NDataStreams::V1::CalculateShardFromSrcId(srcId, partCount);
        Y_ABORT_UNLESS(partExpected < partCount);
        auto writer = CreateSimpleWriter(*driver, topic, srcId);
        auto res = writer->Write("test-data", writer->GetInitSeqNo() + 1);
        UNIT_ASSERT(res);
        writer->Close();

        NYdb::NPersQueue::TReadSessionSettings readerSettings;
        readerSettings.ConsumerName("debug").AppendTopics(topic);
        auto reader = CreateReader(*driver, readerSettings);
        auto mbEv = GetNextMessageSkipAssignment(reader);
        UNIT_ASSERT(mbEv.Defined());
        UNIT_ASSERT_VALUES_EQUAL(mbEv->GetPartitionStream()->GetPartitionGroupId(), partExpected + 1);
    }
}

} // namespace NKikimr::NPersQueueTests
