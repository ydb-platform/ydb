#include <ydb/services/lib/sharding/sharding.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <ydb/services/ydb/ydb_keys_ut.h>

#include <ydb/public/sdk/cpp/client/ydb_datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/api/grpc/draft/ydb_datastreams_v1.grpc.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/digest/md5/md5.h>

#include <util/system/tempfile.h>

#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <random>


using namespace NYdb;
using namespace NYdb::NTable;
using namespace NKikimr::NDataStreams::V1;
namespace YDS_V1 = Ydb::DataStreams::V1;
namespace NYDS_V1 = NYdb::NDataStreams::V1;
struct WithSslAndAuth : TKikimrTestSettings {
    static constexpr bool SSL = true;
    static constexpr bool AUTH = true;
};
using TKikimrWithGrpcAndRootSchemaSecure = NYdb::TBasicKikimrWithGrpcAndRootSchema<WithSslAndAuth>;

static constexpr const char NON_CHARGEABLE_USER[] = "superuser@builtin";
static constexpr const char NON_CHARGEABLE_USER_X[] = "superuser_x@builtin";
static constexpr const char NON_CHARGEABLE_USER_Y[] = "superuser_y@builtin";

static constexpr const char DEFAULT_CLOUD_ID[] = "somecloud";
static constexpr const char DEFAULT_FOLDER_ID[] = "somefolder";

template<class TKikimr, bool secure>
class TDatastreamsTestServer {
public:
    TDatastreamsTestServer(bool autopartitioningEnabled = false) {
        NKikimrConfig::TAppConfig appConfig;

        if (autopartitioningEnabled) {
            appConfig.MutableFeatureFlags()->SetEnableTopicSplitMerge(true);
            appConfig.MutableFeatureFlags()->SetEnablePQConfigTransactionsAtSchemeShard(true);
            appConfig.MutableFeatureFlags()->SetEnableTopicServiceTx(true);
        }

        appConfig.MutablePQConfig()->SetTopicsAreFirstClassCitizen(true);
        appConfig.MutablePQConfig()->SetEnabled(true);
        // NOTE(shmel1k@): KIKIMR-14221
        appConfig.MutablePQConfig()->SetCheckACL(false);
        appConfig.MutablePQConfig()->SetRequireCredentialsInNewProtocol(false);

        auto cst = appConfig.MutablePQConfig()->AddClientServiceType();
        cst->SetName("data-transfer");
        cst = appConfig.MutablePQConfig()->AddClientServiceType();
        cst->SetName("data-transfer2");


        appConfig.MutablePQConfig()->MutableQuotingConfig()->SetEnableQuoting(true);
        appConfig.MutablePQConfig()->MutableQuotingConfig()->SetQuotaWaitDurationMs(100);
        appConfig.MutablePQConfig()->MutableQuotingConfig()->SetPartitionReadQuotaIsTwiceWriteQuota(true);
        appConfig.MutablePQConfig()->MutableBillingMeteringConfig()->SetEnabled(true);
        appConfig.MutablePQConfig()->MutableBillingMeteringConfig()->SetFlushIntervalSec(1);
        appConfig.MutablePQConfig()->AddClientServiceType()->SetName("data-streams");
        appConfig.MutablePQConfig()->AddNonChargeableUser(NON_CHARGEABLE_USER);
        appConfig.MutablePQConfig()->AddNonChargeableUser(NON_CHARGEABLE_USER_X);
        appConfig.MutablePQConfig()->AddNonChargeableUser(NON_CHARGEABLE_USER_Y);

        appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(128);
        appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(512);
        appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(1_KB);

        auto limit = appConfig.MutablePQConfig()->AddValidRetentionLimits();
        limit->SetMinPeriodSeconds(0);
        limit->SetMaxPeriodSeconds(TDuration::Days(1).Seconds());
        limit->SetMinStorageMegabytes(0);
        limit->SetMaxStorageMegabytes(0);

        limit = appConfig.MutablePQConfig()->AddValidRetentionLimits();
        limit->SetMinPeriodSeconds(0);
        limit->SetMaxPeriodSeconds(TDuration::Days(7).Seconds());
        limit->SetMinStorageMegabytes(50_KB);
        limit->SetMaxStorageMegabytes(1_MB);

        MeteringFile = MakeHolder<TTempFileHandle>();
        appConfig.MutableMeteringConfig()->SetMeteringFilePath(MeteringFile->Name());

        if (secure) {
            appConfig.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        }
        KikimrServer = std::make_unique<TKikimr>(std::move(appConfig));
        ui16 grpc = KikimrServer->GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driverConfig = TDriverConfig().SetEndpoint(location).SetLog(CreateLogBackend("cerr", TLOG_DEBUG));
        if (secure) {
            driverConfig.UseSecureConnection(TString(NYdbSslTestData::CaCrt));
        } else {
            driverConfig.SetDatabase("/Root/");
        }

        Driver = std::make_unique<TDriver>(std::move(driverConfig));
        DataStreamsClient = std::make_unique<NYDS_V1::TDataStreamsClient>(*Driver,
             TCommonClientSettings()
                 .AuthToken("user@builtin"));

        {
            NYdb::NScheme::TSchemeClient schemeClient(*Driver);
            NYdb::NScheme::TPermissions permissions("user@builtin", {"ydb.generic.read", "ydb.generic.write"});

            auto result = schemeClient.ModifyPermissions("/Root",
                NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT(result.IsSuccess());
        }

        TClient client(*(KikimrServer->ServerSettings));
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                 client.AlterUserAttributes("/", "Root", {{"folder_id", DEFAULT_FOLDER_ID},
                                                                          {"cloud_id", DEFAULT_CLOUD_ID},
                                                                          {"database_id", "root"}}));
    }

public:
    std::unique_ptr<TKikimr> KikimrServer;
    std::unique_ptr<TDriver> Driver;
    std::unique_ptr<NYDS_V1::TDataStreamsClient> DataStreamsClient;
    std::unique_ptr<NYDS_V1::TDataStreamsClient> UnauthenticatedClient;
    THolder<TTempFileHandle> MeteringFile;
};

using TInsecureDatastreamsTestServer = TDatastreamsTestServer<TKikimrWithGrpcAndRootSchema, false>;
using TSecureDatastreamsTestServer = TDatastreamsTestServer<TKikimrWithGrpcAndRootSchemaSecure, true>;

ui32 CheckMeteringFile(TTempFileHandle* meteringFile, const TString& streamPath, const TString& schema,
                       std::function<void(const NJson::TJsonValue::TMapType& map)> tags_check,
                       std::function<void(const NJson::TJsonValue::TMapType& map)> labels_check,
                       std::function<void(const NJson::TJsonValue::TMapType& map)> usage_check) {
    if (meteringFile->IsOpen()) {
        meteringFile->Flush();
        meteringFile->Close();
    }
    auto input = TFileInput(TFile(meteringFile->Name(), RdOnly | OpenExisting));
    ui32 schemaFoundTimes{0};
    TString line;
    while(input.ReadLine(line)) {
        Cerr << "Got line from metering file data: '" << line << "'" << Endl;
        NJson::TJsonValue json;
        NJson::ReadJsonTree(line, &json, true);
        auto& map = json.GetMap();
        UNIT_ASSERT(map.contains("schema"));
        if (map.find("schema")->second.GetString() == schema) {
            ++schemaFoundTimes;
        } else {
            continue;
        }
        UNIT_ASSERT(map.contains("cloud_id"));
        UNIT_ASSERT(map.contains("folder_id"));
        UNIT_ASSERT(map.contains("resource_id"));
        UNIT_ASSERT(map.contains("source_id"));
        UNIT_ASSERT(map.contains("source_wt"));
        // Following 3 fields are mandatory:
        UNIT_ASSERT(map.contains("tags"));
        UNIT_ASSERT(map.contains("labels"));
        UNIT_ASSERT(map.contains("usage"));

        UNIT_ASSERT(map.find("cloud_id")->second.GetString() == DEFAULT_CLOUD_ID);
        UNIT_ASSERT(map.find("folder_id")->second.GetString() == DEFAULT_FOLDER_ID);
        UNIT_ASSERT(map.find("resource_id")->second.GetString() == streamPath);
        tags_check(map);
        labels_check(map);
        usage_check(map);
    }
    return schemaFoundTimes;
}


#define Y_UNIT_TEST_NAME this->Name_;


#define SET_YDS_LOCALS                               \
    auto& kikimr = testServer.KikimrServer->Server_; \
    Y_UNUSED(kikimr);                                \
    auto& driver = testServer.Driver;                \
    Y_UNUSED(driver);                                \


Y_UNIT_TEST_SUITE(DataStreams) {

    Y_UNIT_TEST(TestControlPlaneAndMeteringData) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        const TString streamName2 = TStringBuilder() << "tdir/stream_" << Y_UNIT_TEST_NAME;
        const TString streamName3 = TStringBuilder() << "tdir/table/feed_" << Y_UNIT_TEST_NAME;
        const TString tableName = "tdir/table";
        const TString feedName = TStringBuilder() << "feed_" << Y_UNIT_TEST_NAME;

        {
            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            auto result = pqClient.CreateTopic(streamName2).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            NYdb::NTable::TTableClient tableClient(*testServer.Driver);
            tableClient.RetryOperationSync([&](TSession session)
                {
                    NYdb::NTable::TTableBuilder builder;
                    builder.AddNonNullableColumn("key", NYdb::EPrimitiveType::String).SetPrimaryKeyColumn("key");

                    auto result = session.CreateTable("/Root/" + tableName, builder.Build()).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                    Cerr << result.GetIssues().ToString() << "\n";
                    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

                    auto result2 = session.AlterTable("/Root/" + tableName, NYdb::NTable::TAlterTableSettings()
                                    .AppendAddChangefeeds(NYdb::NTable::TChangefeedDescription(feedName,
                                                                                           NYdb::NTable::EChangefeedMode::Updates,
                                                                                           NYdb::NTable::EChangefeedFormat::Json))
                                                     ).ExtractValueSync();
                    Cerr << result2.GetIssues().ToString() << "\n";
                    UNIT_ASSERT_VALUES_EQUAL(result2.IsTransportError(), false);
                    UNIT_ASSERT_VALUES_EQUAL(result2.GetStatus(), EStatus::SUCCESS);
                    return result2;
                }
            );
        }

        // Trying to delete stream that doesn't exist yet

        {
            auto result = testServer.DataStreamsClient->DeleteStream("testfolder/" + streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings()
                    .ShardCount(3)
                    /*
                    .BeginConfigurePartitioningSettings()
                        .MinActivePartitions(3)
                        .MaxActivePartitions(7)
                        .BeginConfigureAutoPartitioningSettings()
                            .Strategy(NYdb::NDataStreams::V1::EAutoPartitioningStrategy::ScaleUpAndDown)
                            .StabilizationWindow(TDuration::Seconds(123))
                            .UpUtilizationPercent(97)
                            .DownUtilizationPercent(13)
                        .EndConfigureAutoPartitioningSettings()
                    .EndConfigurePartitioningSettings()
                    */
                ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& d = result.GetResult().stream_description();
            UNIT_ASSERT_VALUES_EQUAL(d.stream_status(), YDS_V1::StreamDescription::ACTIVE);
            UNIT_ASSERT_VALUES_EQUAL(d.stream_name(), streamName);
            UNIT_ASSERT_VALUES_EQUAL(d.stream_arn(), streamName);
            UNIT_ASSERT_VALUES_EQUAL(d.write_quota_kb_per_sec(), 1_KB);
            UNIT_ASSERT_VALUES_EQUAL(d.retention_period_hours(), 24);

            UNIT_ASSERT_VALUES_EQUAL(d.shards().size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(d.shards(0).sequence_number_range().starting_sequence_number(), "0");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(0).hash_key_range().starting_hash_key(), "0");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(0).hash_key_range().ending_hash_key(), "113427455640312821154458202477256070484");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(1).hash_key_range().starting_hash_key(), "113427455640312821154458202477256070485");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(1).hash_key_range().ending_hash_key(), "226854911280625642308916404954512140969");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(2).hash_key_range().starting_hash_key(), "226854911280625642308916404954512140970");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(2).hash_key_range().ending_hash_key(), "340282366920938463463374607431768211455");

            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().min_active_partitions(), 3);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().max_active_partitions(), 3);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().strategy(), ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().partition_write_speed().stabilization_window().seconds(), 300);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().partition_write_speed().up_utilization_percent(), 90);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().partition_write_speed().down_utilization_percent(), 30);
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStreamSummary(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description_summary().stream_status(),
                                     YDS_V1::StreamDescription::ACTIVE);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description_summary().stream_name(), streamName);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description_summary().stream_arn(), streamName);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description_summary().retention_period_hours(), 24);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description_summary().open_shard_count(), 3);
        }


        {
            auto result = testServer.DataStreamsClient->ListStreams(NYdb::NDataStreams::V1::TListStreamsSettings().Recurse(false)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_names().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_names(0), streamName);
        }

        // cannot decrease the number of shards
        {
            auto result = testServer.DataStreamsClient->UpdateShardCount(streamName,
                NYDS_V1::TUpdateShardCountSettings().TargetShardCount(2)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->UpdateShardCount(streamName,
                NYDS_V1::TUpdateShardCountSettings().TargetShardCount(15)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // list all streams, include cdc and recursive
        {
            auto result = testServer.DataStreamsClient->ListStreams(NYdb::NDataStreams::V1::TListStreamsSettings().Recurse(true)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_names().size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_names(0), streamName);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_names(1), streamName2);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_names(2), streamName3);
        }

        // should behave the same, returning 3 names
        {
            auto result = testServer.DataStreamsClient->ListStreams().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_names().size(), 3);
        }

        // now when stream is created delete should work fine
        {
            auto result = testServer.DataStreamsClient->DeleteStream(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Describe should fail after delete
        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream("testfolder/" + streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(TestReservedResourcesMetering) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10)
                                                .RetentionPeriodHours(20).StreamMode(NYdb::NDataStreams::V1::ESM_ON_DEMAND)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->UpdateStreamMode(streamName,
                NYDS_V1::TUpdateStreamModeSettings().StreamMode(NYdb::NDataStreams::V1::ESM_PROVISIONED)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            for (int i = 0; i < 5; ++i) {
                std::vector<NYDS_V1::TDataRecord> records;
                for (ui32 i = 1; i <= 30; ++i) {
                    TString data = Sprintf("%04u", i);
                    records.push_back({data, data, ""});
                }
                auto result = testServer.DataStreamsClient->PutRecords(streamName, records).ExtractValueSync();
                Cerr << result.GetResult().DebugString() << Endl;
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                sleep(1);
            }
        }

        sleep(1);

        auto putUnitsSchemaFound =
            CheckMeteringFile(testServer.MeteringFile.Get(), "/Root/" + streamName, "yds.events.puts.v1",
                          [](const NJson::TJsonValue::TMapType& map) {
                              UNIT_ASSERT(map.contains("tags"));
                              UNIT_ASSERT_VALUES_EQUAL(map.find("tags")->second.GetMap().size(), 0);
                          },
                          [streamName](const NJson::TJsonValue::TMapType& map) {
                              UNIT_ASSERT(map.contains("labels"));
                              auto& labels = map.find("labels")->second.GetMap();
                              UNIT_ASSERT_VALUES_EQUAL(
                                  labels.find("datastreams_stream_name")->second.GetString(), streamName);
                              UNIT_ASSERT_VALUES_EQUAL(
                                  labels.find("ydb_database")->second.GetString(), "root");
                          },
                          [](const NJson::TJsonValue::TMapType& map) {
                              UNIT_ASSERT(map.contains("usage"));
                              auto& usage = map.find("usage")->second.GetMap();
                              UNIT_ASSERT(usage.find("quantity")->second.GetInteger() >= 0);
                              UNIT_ASSERT_GT(usage.find("start")->second.GetUInteger(),
                                             TInstant::Now().Seconds() - 10);
                              UNIT_ASSERT_GT(usage.find("finish")->second.GetUInteger(),
                                             TInstant::Now().Seconds() - 9);
                              UNIT_ASSERT_VALUES_EQUAL(usage.find("unit")->second.GetString(), "put_events");
                          });
        UNIT_ASSERT_VALUES_EQUAL(putUnitsSchemaFound, 36);

        auto resourcesReservedSchemaFound =
            CheckMeteringFile(testServer.MeteringFile.Get(), "/Root/" + streamName, "yds.resources.reserved.v1",
                          [](const NJson::TJsonValue::TMapType& map) {
                              UNIT_ASSERT(map.contains("tags"));
                              auto& tags = map.find("tags")->second.GetMap();
                              UNIT_ASSERT(tags.contains("reserved_throughput_bps"));
                              UNIT_ASSERT(tags.contains("reserved_consumers_count"));
                              UNIT_ASSERT(tags.contains("reserved_storage_bytes"));
                          },
                          [streamName](const NJson::TJsonValue::TMapType& map) {
                              UNIT_ASSERT(map.contains("labels"));
                              auto& labels = map.find("labels")->second.GetMap();
                              UNIT_ASSERT_VALUES_EQUAL(
                                  labels.find("datastreams_stream_name")->second.GetString(), streamName);
                              UNIT_ASSERT_VALUES_EQUAL(
                                  labels.find("ydb_database")->second.GetString(), "root");
                          },
                          [](const NJson::TJsonValue::TMapType& map) {
                              UNIT_ASSERT(map.contains("usage"));
                              auto& usage = map.find("usage")->second.GetMap();
                              UNIT_ASSERT_GE(usage.find("quantity")->second.GetInteger(), 1);
                              UNIT_ASSERT_GT(usage.find("start")->second.GetUInteger(),
                                             TInstant::Now().Seconds() - 10);
                              UNIT_ASSERT_GT(usage.find("finish")->second.GetUInteger(),
                                             TInstant::Now().Seconds() - 9);
                              UNIT_ASSERT_VALUES_EQUAL(usage.find("unit")->second.GetString(), "second");
                          });
        UNIT_ASSERT_VALUES_EQUAL(resourcesReservedSchemaFound, 37);
    }

    Y_UNIT_TEST(TestReservedStorageMetering) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        const ui64 storageMb = 55_GB / 1_MB;
        const ui32 shardCount = 2;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings()
                                .ShardCount(shardCount)
                                .RetentionStorageMegabytes(storageMb)
                                .StreamMode(NYdb::NDataStreams::V1::ESM_PROVISIONED)
                        ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            for (int i = 0; i < 5; ++i) {
                std::vector<NYDS_V1::TDataRecord> records;
                for (ui32 i = 1; i <= 30; ++i) {
                    TString data = Sprintf("%04u", i);
                    records.push_back({data, data, ""});
                }
                auto result = testServer.DataStreamsClient->PutRecords(streamName, records).ExtractValueSync();
                Cerr << result.GetResult().DebugString() << Endl;
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                sleep(1);
            }
        }

        sleep(1);

        auto storageSchemaFound =
            CheckMeteringFile(testServer.MeteringFile.Get(), "/Root/" + streamName, "yds.storage.reserved.v1",
                            [](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("tags"));
                                UNIT_ASSERT_VALUES_EQUAL(map.find("tags")->second.GetMap().size(), 0);
                            },
                            [streamName](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("labels"));
                                auto& labels = map.find("labels")->second.GetMap();
                                UNIT_ASSERT_VALUES_EQUAL(labels.size(), 2);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("datastreams_stream_name")->second.GetString(), streamName);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("ydb_database")->second.GetString(), "root");
                            },
                            [](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("usage"));
                                auto& usage = map.find("usage")->second.GetMap();

                                auto start = usage.find("start")->second.GetUInteger();
                                auto finish = usage.find("finish")->second.GetUInteger();
                                UNIT_ASSERT_LE(start, finish);

                                auto now = TInstant::Now();
                                UNIT_ASSERT_GT(start, now.Seconds() - 10);
                                UNIT_ASSERT_GT(finish, now.Seconds() - 9);
                                UNIT_ASSERT_VALUES_EQUAL(usage.find("unit")->second.GetString(),
                                                         "mbyte*second");

                                UNIT_ASSERT_VALUES_EQUAL(usage.find("quantity")->second.GetUInteger(),
                                               storageMb * (finish - start));

                            });
        UNIT_ASSERT_VALUES_EQUAL(storageSchemaFound, 8);

        auto throughputSchemaFound =
            CheckMeteringFile(testServer.MeteringFile.Get(), "/Root/" + streamName,
                            "yds.throughput.reserved.v1",
                            [](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("tags"));
                                auto& tags = map.find("tags")->second.GetMap();
                                UNIT_ASSERT_VALUES_EQUAL(tags.size(), 2);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    tags.find("reserved_throughput_bps")->second.GetUInteger(), 1_MB);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    tags.find("reserved_consumers_count")->second.GetUInteger(), 0);

                            },
                            [streamName](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("labels"));
                                auto& labels = map.find("labels")->second.GetMap();
                                UNIT_ASSERT_VALUES_EQUAL(labels.size(), 2);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("datastreams_stream_name")->second.GetString(), streamName);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("ydb_database")->second.GetString(), "root");
                            },
                            [](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("usage"));
                                auto& usage = map.find("usage")->second.GetMap();

                                auto start = usage.find("start")->second.GetUInteger();
                                auto finish = usage.find("finish")->second.GetUInteger();
                                UNIT_ASSERT_LE(start, finish);

                                UNIT_ASSERT_VALUES_EQUAL(usage.find("quantity")->second.GetInteger(), finish - start);

                                auto now = TInstant::Now();
                                UNIT_ASSERT_GT(start, now.Seconds() - 10);
                                UNIT_ASSERT_GT(finish, now.Seconds() - 9);
                                UNIT_ASSERT_VALUES_EQUAL(usage.find("unit")->second.GetString(),
                                                         "second");
                            });
        UNIT_ASSERT_VALUES_EQUAL(throughputSchemaFound, 8);

        auto putUnitsSchemaFound =
            CheckMeteringFile(testServer.MeteringFile.Get(), "/Root/" + streamName, "yds.events.puts.v1",
                            [](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("tags"));
                                UNIT_ASSERT_VALUES_EQUAL(map.find("tags")->second.GetMap().size(), 0);
                            },
                            [streamName](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("labels"));
                                auto& labels = map.find("labels")->second.GetMap();
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("datastreams_stream_name")->second.GetString(), streamName);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("ydb_database")->second.GetString(), "root");
                            },
                            [](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("usage"));
                                auto& usage = map.find("usage")->second.GetMap();
                                UNIT_ASSERT_VALUES_EQUAL(usage.find("quantity")->second.GetInteger(), 1);
                                UNIT_ASSERT_GT(usage.find("start")->second.GetUInteger(),
                                               TInstant::Now().Seconds() - 10);
                                UNIT_ASSERT_GT(usage.find("finish")->second.GetUInteger(),
                                               TInstant::Now().Seconds() - 9);
                                UNIT_ASSERT_VALUES_EQUAL(usage.find("unit")->second.GetString(),
                                                         "put_events");
                            });
        UNIT_ASSERT_VALUES_EQUAL(putUnitsSchemaFound, 8);

        NYdb::NPersQueue::TPersQueueClient pqClient(*testServer.Driver);
        {
            auto res = pqClient.DropTopic(streamName);
            res.Wait();
            UNIT_ASSERT(res.GetValue().IsSuccess());
        }
    }


    Y_UNIT_TEST(TestReservedConsumersMetering) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        const ui64 storageMb = 55_GB / 1_MB;
        const ui32 shardCount = 1;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(shardCount)
                                                .RetentionStorageMegabytes(storageMb)
                                                .StreamMode(NYdb::NDataStreams::V1::ESM_PROVISIONED)
                    ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }


        NYdb::NPersQueue::TPersQueueClient pqClient(*testServer.Driver);
        std::vector<std::pair<TString, TString>> opts = {{"user1", ""}, {"user2", "data-transfer"}, {"user3", "data-streams"}, {"user4", "data-transfer2"}};
        for (const auto& p : opts) {
            NYdb::NPersQueue::TAddReadRuleSettings addReadRuleSettings;
            addReadRuleSettings.ReadRule(NYdb::NPersQueue::TReadRuleSettings().ServiceType(p.second).ConsumerName(p.first));
            auto res = pqClient.AddReadRule(streamName, addReadRuleSettings);
            res.Wait();
            UNIT_ASSERT(res.GetValue().IsSuccess());
        }

        {
            for (int i = 0; i < 5; ++i) {
                std::vector<NYDS_V1::TDataRecord> records;
                for (ui32 i = 1; i <= 30; ++i) {
                    TString data = Sprintf("%04u", i);
                    records.push_back({data, data, ""});
                }
                auto result = testServer.DataStreamsClient->PutRecords(streamName, records).ExtractValueSync();
                Cerr << result.GetResult().DebugString() << Endl;
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                sleep(1);
            }
        }

        std::vector<TString> consumers = {"user1", "user2"};
        for (auto& consumer : consumers) {
            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);

            auto session = pqClient.CreateReadSession(NYdb::NTopic::TReadSessionSettings()
                                                          .ConsumerName(consumer)
                                                          .AppendTopics(NYdb::NTopic::TTopicReadSettings().Path("/Root/" + streamName)));
            ui32 readCount = 0;
            while (readCount < 150) {
                auto event = session->GetEvent(true);

                if (auto* dataReceivedEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                    readCount += dataReceivedEvent->GetMessages().size();
                } else if (auto* createPartitionStreamEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                    createPartitionStreamEvent->Confirm();
                } else if (auto* destroyPartitionStreamEvent = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                    destroyPartitionStreamEvent->Confirm();
                } else if (auto* closeSessionEvent = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                    break;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(readCount, 150);
        }

        {
            auto res = pqClient.DropTopic(streamName);
            res.Wait();
            UNIT_ASSERT(res.GetValue().IsSuccess());
        }

        sleep(1);

        auto storageSchemaFound =
            CheckMeteringFile(testServer.MeteringFile.Get(), "/Root/" + streamName, "yds.storage.reserved.v1",
                            [](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("tags"));
                                UNIT_ASSERT_VALUES_EQUAL(map.find("tags")->second.GetMap().size(), 0);
                            },
                            [streamName](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("labels"));
                                auto& labels = map.find("labels")->second.GetMap();
                                UNIT_ASSERT_VALUES_EQUAL(labels.size(), 2);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("datastreams_stream_name")->second.GetString(), streamName);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("ydb_database")->second.GetString(), "root");
                            },
                            [/*storageMb*/](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("usage"));
                                auto& usage = map.find("usage")->second.GetMap();
                                UNIT_ASSERT_GT(usage.find("start")->second.GetUInteger(),
                                               TInstant::Now().Seconds() - 10);
                                UNIT_ASSERT_GT(usage.find("finish")->second.GetUInteger(),
                                               TInstant::Now().Seconds() - 9);
                                UNIT_ASSERT_VALUES_EQUAL(usage.find("unit")->second.GetString(),
                                                         "mbyte*second");
        //                        UNIT_ASSERT_VALUES_EQUAL(usage.find("quantity")->second.GetUInteger(),
        //                                       storageMb);

                            });
        UNIT_ASSERT_VALUES_EQUAL(storageSchemaFound, 9);


        ui32 s = 0;
        auto throughputSchemaFound =
            CheckMeteringFile(testServer.MeteringFile.Get(), "/Root/" + streamName,
                            "yds.throughput.reserved.v1",
                            [&s](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("tags"));
                                auto& tags = map.find("tags")->second.GetMap();
                                UNIT_ASSERT_VALUES_EQUAL(tags.size(), 2);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    tags.find("reserved_throughput_bps")->second.GetUInteger(), 1_MB);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    tags.find("reserved_consumers_count")->second.GetUInteger(), ui32(s >= 1) + ui32(s >= 3) );
                                ++s;

                            },
                            [streamName](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("labels"));
                                auto& labels = map.find("labels")->second.GetMap();
                                UNIT_ASSERT_VALUES_EQUAL(labels.size(), 2);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("datastreams_stream_name")->second.GetString(), streamName);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("ydb_database")->second.GetString(), "root");
                            },
                            [](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("usage"));
                                auto& usage = map.find("usage")->second.GetMap();

                                auto start = usage.find("start")->second.GetUInteger();
                                auto finish = usage.find("finish")->second.GetUInteger();
                                UNIT_ASSERT_LE(start, finish);

                                UNIT_ASSERT_VALUES_EQUAL(usage.find("quantity")->second.GetInteger(), finish - start);

                                auto now = TInstant::Now();
                                UNIT_ASSERT_GT(start, now.Seconds() - 10);
                                UNIT_ASSERT_GT(finish, now.Seconds() - 9);
                                UNIT_ASSERT_VALUES_EQUAL(usage.find("unit")->second.GetString(),
                                                         "second");
                            });
        UNIT_ASSERT_VALUES_EQUAL(throughputSchemaFound, 9);
    }


    Y_UNIT_TEST(TestNonChargeableUser) {
        TSecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        SET_YDS_LOCALS;
        const TString streamPath = "/Root/" + streamName;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamPath,
                NYDS_V1::TCreateStreamSettings().ShardCount(1)
                        .StreamMode(NYdb::NDataStreams::V1::ESM_PROVISIONED)
                ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        NYDS_V1::TDataStreamsClient client(*driver, TCommonClientSettings().AuthToken(NON_CHARGEABLE_USER));
        NYdb::NScheme::TSchemeClient schemeClient(*driver);
        {
            NYdb::NScheme::TPermissions permissions(NON_CHARGEABLE_USER,
                                                    {"ydb.generic.read", "ydb.generic.write"});
            auto result = schemeClient.ModifyPermissions(streamPath,
                NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        // for metering purposes
        {
            std::vector<NYDS_V1::TDataRecord> records;
            for (ui32 i = 1; i <= 30; ++i) {
                TString data = Sprintf("%04u", i);
                records.push_back({data, data, ""});
            }
            auto result = client.PutRecords(streamPath, records).ExtractValueSync();
            Cerr << result.GetResult().DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->UpdateStream(streamPath,
                 NYDS_V1::TUpdateStreamSettings().TargetShardCount(2)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DeleteStream(streamPath).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        sleep(1);

        auto putUnitsSchemaFound =
            CheckMeteringFile(testServer.MeteringFile.Get(), streamPath, "yds.events.puts.v1",
                          [](const NJson::TJsonValue::TMapType&) {},
                          [](const NJson::TJsonValue::TMapType&) {},
                          [](const NJson::TJsonValue::TMapType&) {});
        UNIT_ASSERT_VALUES_EQUAL(putUnitsSchemaFound, 0);

        auto resourcesReservedSchemaFound =
            CheckMeteringFile(testServer.MeteringFile.Get(), streamPath, "yds.resources.reserved.v1",
                            [](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("tags"));
                                auto& tags = map.find("tags")->second.GetMap();
                                UNIT_ASSERT(tags.contains("reserved_throughput_bps"));
                                UNIT_ASSERT(tags.contains("reserved_consumers_count"));
                                UNIT_ASSERT(tags.contains("reserved_storage_bytes"));
                            },
                            [streamName](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("labels"));
                                auto& labels = map.find("labels")->second.GetMap();
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("datastreams_stream_name")->second.GetString(), streamName);
                                UNIT_ASSERT_VALUES_EQUAL(
                                    labels.find("ydb_database")->second.GetString(), "root");
                            },
                            [](const NJson::TJsonValue::TMapType& map) {
                                UNIT_ASSERT(map.contains("usage"));
                                auto& usage = map.find("usage")->second.GetMap();
                                UNIT_ASSERT(usage.find("quantity")->second.GetInteger() >= 0);
                                UNIT_ASSERT_GT(usage.find("start")->second.GetUInteger(),
                                               TInstant::Now().Seconds() - 10);
                                UNIT_ASSERT_GT(usage.find("finish")->second.GetUInteger(),
                                               TInstant::Now().Seconds() - 9);
                                UNIT_ASSERT_VALUES_EQUAL(usage.find("unit")->second.GetString(), "second");
                            });
        UNIT_ASSERT_VALUES_EQUAL(resourcesReservedSchemaFound, 3);
    }

    Y_UNIT_TEST(TestStreamStorageRetention) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10)
                                                .RetentionStorageMegabytes(40_GB / 1_MB)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10).RetentionPeriodHours(0)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10).RetentionPeriodHours(25)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10)
                                                .RetentionPeriodHours(2)
                                                .RetentionStorageMegabytes(55_GB / 1_MB)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10)
                                                .RetentionPeriodHours(100)
                                                .RetentionStorageMegabytes(55_GB / 1_MB)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10)
                                                .RetentionPeriodHours(20)
                                                .RetentionStorageMegabytes(5_GB / 1_MB)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10)
                                                .RetentionStorageMegabytes(50_GB / 1_MB)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10).RetentionPeriodHours(25)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10).RetentionPeriodHours(2)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ALREADY_EXISTS);
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().stream_status(),
                                     YDS_V1::StreamDescription::ACTIVE);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().stream_name(), streamName);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().write_quota_kb_per_sec(), 1_KB);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().retention_period_hours(),
                                     TDuration::Days(7).Hours());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().storage_limit_mb(), 50_GB / 1_MB);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().stream_mode_details().stream_mode(),
                                     Ydb::DataStreams::V1::StreamMode::ON_DEMAND);


        }
    }

    Y_UNIT_TEST(ChangeBetweenRetentionModes) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;

        // CREATE STREAM IF NOT EXISTS
        auto result = testServer.DataStreamsClient->CreateStream(streamName,
            NYDS_V1::TCreateStreamSettings().ShardCount(10)
                                            .RetentionStorageMegabytes(50_GB / 1_MB)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        // UPDATE: Gb -> hours
        auto updResult = testServer.DataStreamsClient->UpdateStream(streamName,
            NYDS_V1::TUpdateStreamSettings().TargetShardCount(10)
                                            .RetentionPeriodHours(4)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(updResult.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(updResult.GetStatus(), EStatus::SUCCESS);

        auto describeResult = testServer.DataStreamsClient->DescribeStream(streamName).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(describeResult.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResult().stream_description().retention_period_hours(),
                                    TDuration::Hours(4).Hours());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResult().stream_description().storage_limit_mb(), 0);

        // UPDATE #2: hours -> Gb
        updResult = testServer.DataStreamsClient->UpdateStream(streamName,
            NYDS_V1::TUpdateStreamSettings().TargetShardCount(10)
                                            .RetentionStorageMegabytes(50_GB / 1_MB)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(updResult.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(updResult.GetStatus(), EStatus::SUCCESS);

        describeResult = testServer.DataStreamsClient->DescribeStream(streamName).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(describeResult.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResult().stream_description().retention_period_hours(),
                                    TDuration::Days(7).Hours());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResult().stream_description().storage_limit_mb(), 50_GB / 1_MB);

        // UPDATE #3: Gb -> hours
        updResult = testServer.DataStreamsClient->UpdateStream(streamName,
            NYDS_V1::TUpdateStreamSettings().TargetShardCount(10)
                                            .RetentionPeriodHours(4)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(updResult.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(updResult.GetStatus(), EStatus::SUCCESS);

        describeResult = testServer.DataStreamsClient->DescribeStream(streamName).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(describeResult.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResult().stream_description().retention_period_hours(),
                                    TDuration::Hours(4).Hours());
        UNIT_ASSERT_VALUES_EQUAL(describeResult.GetResult().stream_description().storage_limit_mb(), 0);
    }

    Y_UNIT_TEST(TestCreateExistingStream) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ALREADY_EXISTS);
        }

    }

    Y_UNIT_TEST(TestStreamPagination) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        for (ui32 folderIdx = 0; folderIdx < 4; folderIdx++) {
            for (ui32 streamIdx = 0; streamIdx < 5; streamIdx++) {
                TStringBuilder streamNameX = TStringBuilder() <<  folderIdx  << streamName << streamIdx;
                auto result = testServer.DataStreamsClient->CreateStream(
                    streamNameX,
                    NYDS_V1::TCreateStreamSettings().ShardCount(10)
                                                    ).ExtractValueSync();
                Cerr << result.GetIssues().ToString() << "\n";
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
        }

        TString startStream;
        THashSet<TString> streams;
        for (int i = 0; i < 3; i++) {
            auto result = testServer.DataStreamsClient->ListStreams(NYDS_V1::TListStreamsSettings().Limit(6).ExclusiveStartStreamName(startStream)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_names().size(), 6);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().has_more_streams(), true);
            streams.insert(result.GetResult().stream_names().begin(), result.GetResult().stream_names().end());
            startStream = result.GetResult().stream_names(5);
        }

        {
            auto result = testServer.DataStreamsClient->ListStreams(NYDS_V1::TListStreamsSettings().Limit(6).ExclusiveStartStreamName(startStream)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_names().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().has_more_streams(), false);
            streams.insert(result.GetResult().stream_names().begin(), result.GetResult().stream_names().end());
        }

        UNIT_ASSERT_VALUES_EQUAL(streams.size(), 20);
    }

    Y_UNIT_TEST(TestDeleteStream) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(3)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DeleteStream(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(TestDeleteStreamWithEnforceFlag) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(3)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->RegisterStreamConsumer(streamName, "user1",
                NYDS_V1::TRegisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DeleteStream(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->DeleteStream(streamName,
                NYDS_V1::TDeleteStreamSettings().EnforceConsumerDeletion(true)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }


    Y_UNIT_TEST(TestDeleteStreamWithEnforceFlagFalse) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(3)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->RegisterStreamConsumer(streamName, "user1",
                NYDS_V1::TRegisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DeleteStream(streamName,
                NYDS_V1::TDeleteStreamSettings().EnforceConsumerDeletion(false)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->DeregisterStreamConsumer(streamName, "user1",
                NYDS_V1::TDeregisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DeleteStream(streamName,
                NYDS_V1::TDeleteStreamSettings().EnforceConsumerDeletion(false)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(TestUpdateStream) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(10)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        for (ui32 i = 0; i < 2; ++i) {
            auto result = testServer.DataStreamsClient->UpdateStream(streamName,
                 NYDS_V1::TUpdateStreamSettings().RetentionPeriodHours(5).TargetShardCount(20).WriteQuotaKbPerSec(128)
            ).ExtractValueSync();
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().shards_size(), 20);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().retention_period_hours(), 5);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().write_quota_kb_per_sec(), 128);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().owner(), "user@builtin");
            UNIT_ASSERT(result.GetResult().stream_description().stream_creation_timestamp() > 0);
        }
    }

    Y_UNIT_TEST(TestUpdateStorage) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(1).RetentionStorageMegabytes(50_GB / 1_MB))
                .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->UpdateStream(streamName,
                 NYDS_V1::TUpdateStreamSettings().TargetShardCount(10)
                                                 .WriteQuotaKbPerSec(128)
                                                 .RetentionStorageMegabytes(55_GB / 1_MB)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().shards_size(), 10);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().retention_period_hours(),
                                     TDuration::Days(7).Hours());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().write_quota_kb_per_sec(), 128);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().owner(), "user@builtin");
            UNIT_ASSERT(result.GetResult().stream_description().stream_creation_timestamp() > 0);
        }
    }

    Y_UNIT_TEST(TestStreamTimeRetention) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName,
                NYDS_V1::TDescribeStreamSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().stream_description().retention_period_hours(), 24);
        }

        {
            auto result = testServer.DataStreamsClient->IncreaseStreamRetentionPeriod(streamName,
                NYDS_V1::TIncreaseStreamRetentionPeriodSettings().RetentionPeriodHours(50)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->DecreaseStreamRetentionPeriod(streamName,
                NYDS_V1::TDecreaseStreamRetentionPeriodSettings().RetentionPeriodHours(8)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->IncreaseStreamRetentionPeriod(streamName,
                NYDS_V1::TIncreaseStreamRetentionPeriodSettings().RetentionPeriodHours(4)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->IncreaseStreamRetentionPeriod(streamName,
                NYDS_V1::TIncreaseStreamRetentionPeriodSettings().RetentionPeriodHours(15)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            for (int i = 0; i < 5; ++i) {
                std::vector<NYDS_V1::TDataRecord> records;
                for (ui32 i = 1; i <= 30; ++i) {
                    TString data = Sprintf("%04u", i);
                    records.push_back({data, data, ""});
                }
                auto result = testServer.DataStreamsClient->PutRecords(streamName, records).ExtractValueSync();
                Cerr << result.GetResult().DebugString() << Endl;
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                sleep(1);
            }
        }

        sleep(1);

        CheckMeteringFile(testServer.MeteringFile.Get(), "/Root/" + streamName,
                          "yds.throughput.reserved.v1",
                          [](const NJson::TJsonValue::TMapType& map) {
                              UNIT_ASSERT(map.contains("labels"));
                              UNIT_ASSERT_VALUES_EQUAL(map.find("labels")->second.GetMap().size(), 2);
                          },
                          [](const NJson::TJsonValue::TMapType& map) {
                              UNIT_ASSERT(map.contains("tags"));
                              auto& tags = map.find("tags")->second.GetMap();
                              UNIT_ASSERT_VALUES_EQUAL(tags.size(), 2);
                              UNIT_ASSERT_VALUES_EQUAL(
                                  tags.find("reserved_throughput_bps")->second.GetInteger(), 1_MB);
                              UNIT_ASSERT_VALUES_EQUAL(
                                  tags.find("reserved_consumers_count")->second.GetInteger(), 0);

                          },
                          [](const NJson::TJsonValue::TMapType& map) {
                              UNIT_ASSERT(map.contains("usage"));
                              auto& usage = map.find("usage")->second.GetMap();
                              UNIT_ASSERT(usage.find("quantity")->second.GetInteger() >= 0);
                              UNIT_ASSERT(usage.find("start")->second.GetUInteger() >= 0);
                              UNIT_ASSERT(usage.find("finish")->second.GetUInteger() >= 0);
                              UNIT_ASSERT_VALUES_EQUAL(usage.find("unit")->second.GetString(), "second");
                          });
    }

    Y_UNIT_TEST(TestShardPagination) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream("/Root/" + streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(9)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // describe stream
        {
            TString exclusiveStartShardId;
            THashSet<TString> describedShards;
            for (int i = 0; i < 8; i += 2) {
                auto result = testServer.DataStreamsClient->DescribeStream(streamName,
                                                    NYDS_V1::TDescribeStreamSettings()
                                                        .Limit(2)
                                                        .ExclusiveStartShardId(exclusiveStartShardId)
                                                    ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                auto& description = result.GetResult().stream_description();
                UNIT_ASSERT_VALUES_EQUAL(description.shards().size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(description.has_more_shards(), true);
                for (const auto& shard : description.shards()) {
                    describedShards.insert(shard.shard_id());
                }
                exclusiveStartShardId = description.shards(1).shard_id();
            }

            {
                auto result = testServer.DataStreamsClient->DescribeStream(streamName,
                                                    NYDS_V1::TDescribeStreamSettings()
                                                            .Limit(2)
                                                            .ExclusiveStartShardId(exclusiveStartShardId)
                ).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                auto &description = result.GetResult().stream_description();
                UNIT_ASSERT_VALUES_EQUAL(description.shards().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(description.has_more_shards(), false);
                for (const auto& shard : description.shards()) {
                    describedShards.insert(shard.shard_id());
                }
            }

            // check for total number of shards
            UNIT_ASSERT_VALUES_EQUAL(describedShards.size(), 9);
        }

    }

    Y_UNIT_TEST(TestPutRecordsOfAnauthorizedUser) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        SET_YDS_LOCALS;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(5)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            Sleep(TDuration::Seconds(1));
        }

        NYDS_V1::TDataStreamsClient client(*driver, TCommonClientSettings().AuthToken(""));

        const TString dataStr = "9876543210";

        auto putRecordResult =
            client.PutRecord("/Root/" + streamName, {dataStr, dataStr, dataStr}).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(putRecordResult.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL_C(putRecordResult.GetStatus(), EStatus::SUCCESS, putRecordResult.GetIssues().ToString());
    }

    Y_UNIT_TEST(TestPutRecordsWithRead) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        SET_YDS_LOCALS;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(5)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        kikimr->GetRuntime()->SetLogPriority(NKikimrServices::PQ_READ_PROXY, NLog::EPriority::PRI_DEBUG);
        kikimr->GetRuntime()->SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NLog::EPriority::PRI_DEBUG);

        NYDS_V1::TDataStreamsClient client(*driver, TCommonClientSettings().AuthToken("user2@builtin"));

        TString dataStr = "9876543210";

        auto putRecordResult = client.PutRecord("/Root/" + streamName, {dataStr, dataStr, dataStr}).ExtractValueSync();
        Cerr << putRecordResult.GetResult().DebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(putRecordResult.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL_C(putRecordResult.GetStatus(), EStatus::SUCCESS, putRecordResult.GetIssues().ToString());

        {
            std::vector<NYDS_V1::TDataRecord> records;
            for (ui32 i = 1; i <= 30; ++i) {
                TString data = Sprintf("%04u", i);
                records.push_back({data, data, ""});
            }
            auto result = client.PutRecords(streamName, records).ExtractValueSync();
            Cerr << result.GetResult().DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        NYdb::NPersQueue::TPersQueueClient pqClient(*driver);

        {
            auto result = testServer.DataStreamsClient->RegisterStreamConsumer(streamName, "user1", NYDS_V1::TRegisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumer().consumer_name(), "user1");
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumer().consumer_status(),
                                     YDS_V1::ConsumerDescription_ConsumerStatus_ACTIVE);
        }

        auto session = pqClient.CreateReadSession(NYdb::NPersQueue::TReadSessionSettings()
                                                          .ConsumerName("user1")
                                                          .DisableClusterDiscovery(true)
                                                          .AppendTopics(NYdb::NPersQueue::TTopicReadSettings().Path("/Root/" + streamName)));
        ui32 readCount = 0;
        while (readCount < 31) {
            auto event = session->GetEvent(true);

            if (auto* dataReceivedEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (const auto& item : dataReceivedEvent->GetMessages()) {
                    Cerr << item.DebugString(true) << Endl;
                    UNIT_ASSERT_VALUES_EQUAL(item.GetData(), item.GetPartitionKey());
                    auto hashKey = item.GetExplicitHash().empty() ? HexBytesToDecimal(MD5::Calc(item.GetPartitionKey())) : BytesToDecimal(item.GetExplicitHash());
                    UNIT_ASSERT_VALUES_EQUAL(NKikimr::NDataStreams::V1::ShardFromDecimal(hashKey, 5), item.GetPartitionStream()->GetPartitionId());
                    UNIT_ASSERT(item.GetIp().empty());
                    if (item.GetData() == dataStr) {
                        UNIT_ASSERT_VALUES_EQUAL(item.GetExplicitHash(), dataStr);
                    }
                    readCount++;
                }
            } else if (auto* createPartitionStreamEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*event)) {
                createPartitionStreamEvent->Confirm();
            } else if (auto* destroyPartitionStreamEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent>(&*event)) {
                destroyPartitionStreamEvent->Confirm();
            } else if (auto* closeSessionEvent = std::get_if<NYdb::NPersQueue::TSessionClosedEvent>(&*event)) {
                break;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(readCount, 31);
    }

    Y_UNIT_TEST(TestPutRecordsCornerCases) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        const TString streamPath = "/Root/" + streamName;
        SET_YDS_LOCALS;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(5)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        kikimr->GetRuntime()->SetLogPriority(NKikimrServices::PQ_READ_PROXY, NLog::EPriority::PRI_DEBUG);
        NYDS_V1::TDataStreamsClient client(*driver, TCommonClientSettings().AuthToken("user2@builtin"));

        // Test for too long partition key
        TString longKey = TString(257, '1');
        TString shortEnoughKey = TString(256, '1');
        auto result = client.PutRecords(streamName,
                                        {{longKey,        longKey,        ""},
                                         {shortEnoughKey, shortEnoughKey, ""}}).ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

        result = client.PutRecords(streamName,
                                   {{shortEnoughKey, shortEnoughKey, ""},
                                    {shortEnoughKey, shortEnoughKey, ""}}).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // Test for too long data
        TString longData = TString(1_MB + 1, '1');
        TString shortEnoughData = TString(1_MB, '1');

        result = client.PutRecords(streamName,
                                   {{longData,        shortEnoughKey, ""},
                                    {shortEnoughData, shortEnoughKey, ""}}).ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

        result = client.PutRecords(streamName,
                                   {{shortEnoughData, shortEnoughKey, ""},
                                    {"",              shortEnoughKey, ""}}).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TString longExplicitHash = "340282366920938463463374607431768211456";
        TString shortEnoughExplicitHash = "340282366920938463463374607431768211455";
        TString badExplicitHash = "-439025493205215";

        result = client.PutRecords(streamName,
                                   {{"", shortEnoughKey, longExplicitHash},
                                    {"", shortEnoughKey, longExplicitHash}}).ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

        result = client.PutRecords(streamName,
                                   {{"", shortEnoughKey, badExplicitHash},
                                    {"", shortEnoughKey, badExplicitHash}}).ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

        result = client.PutRecords(streamName,
                                   {{"", shortEnoughKey, shortEnoughExplicitHash},
                                    {"", shortEnoughKey, shortEnoughExplicitHash}}).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = client.PutRecords(streamName,
                                   {{"", shortEnoughKey, "0"},
                                    {"", shortEnoughKey, "0"}}).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        kikimr->GetRuntime()->SetLogPriority(NKikimrServices::PQ_READ_PROXY, NLog::EPriority::PRI_INFO);
        kikimr->GetRuntime()->SetLogPriority(NKikimrServices::PERSQUEUE, NLog::EPriority::PRI_INFO);

        {
            std::vector<NYDS_V1::TDataRecord> records;
            TString data = TString(1_MB, 'a');
            records.push_back({data, "key", ""});
            records.push_back({data, "key", ""});
            records.push_back({data, "key", ""});
            records.push_back({data, "key", ""});

            Cerr << "First put records\n";
            auto result = client.PutRecords(streamPath, records).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().failed_record_count(), 0);
            Cerr << result.GetResult().DebugString() << Endl;

            Cerr << "Second put records (async)\n";
            auto secondWriteAsync = client.PutRecords(streamPath, records);

            Cerr << Now().Seconds() << "Third put records\n";
            result = client.PutRecords(streamPath, records).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().failed_record_count(), 4);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records(0).error_code(), "ProvisionedThroughputExceededException");

            result = secondWriteAsync.ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
            Cerr << result.GetResult().DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().failed_record_count(), 0);
            Cerr << result.GetResult().DebugString() << Endl;

        }

        NYdb::NPersQueue::TPersQueueClient pqClient(*driver);

        {
            NYdb::NPersQueue::TAddReadRuleSettings addReadRuleSettings;
            addReadRuleSettings.ReadRule(NYdb::NPersQueue::TReadRuleSettings().Version(1).ConsumerName("user1"));
            auto res = pqClient.AddReadRule(streamPath, addReadRuleSettings);
            res.Wait();
            UNIT_ASSERT(res.GetValue().IsSuccess());
        }
        auto session = pqClient.CreateReadSession(NYdb::NPersQueue::TReadSessionSettings()
                                                          .ConsumerName("user1")
                                                          .DisableClusterDiscovery(true)
                                                          .RetryPolicy(NYdb::NPersQueue::IRetryPolicy::GetNoRetryPolicy())
                                                          .AppendTopics(NYdb::NPersQueue::TTopicReadSettings().Path(streamPath)));
        ui32 readCount = 0;
        while (readCount < 16) {
            auto event = session->GetEvent(true);

            if (auto* dataReceivedEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (const auto& item : dataReceivedEvent->GetMessages()) {
                    Cout << "GOT MESSAGE: " <<  item.DebugString(false) << Endl;
                    readCount++;
                }
            } else if (auto* createPartitionStreamEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*event)) {
                createPartitionStreamEvent->Confirm();
            } else if (auto* destroyPartitionStreamEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent>(&*event)) {
                destroyPartitionStreamEvent->Confirm();
            } else if (auto* closeSessionEvent = std::get_if<NYdb::NPersQueue::TSessionClosedEvent>(&*event)) {
                break;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(readCount, 16);
    }

    Y_UNIT_TEST(TestPutRecords) {
        TSecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        SET_YDS_LOCALS;
        const TString streamPath = "/Root/" + streamName;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamPath,
                NYDS_V1::TCreateStreamSettings().ShardCount(5)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        NYDS_V1::TDataStreamsClient client(*driver, TCommonClientSettings().AuthToken("user2@builtin"));
        NYdb::NScheme::TSchemeClient schemeClient(*driver);
        {
            std::vector<NYDS_V1::TDataRecord> records;
            for (ui32 i = 1; i <= 30; ++i) {
                TString data = Sprintf("%04u", i);
                records.push_back({data, data, ""});
            }
            auto result = client.PutRecords(streamPath, records).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);

            {
                NYdb::NScheme::TPermissions permissions("user2@builtin", {"ydb.generic.read", "ydb.generic.write"});
                auto result = schemeClient.ModifyPermissions(streamPath,
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)).ExtractValueSync();
                UNIT_ASSERT(result.IsSuccess());
            }

            result = client.PutRecords(streamPath, records).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            Cerr << "PutRecordsResponse = " << result.GetResult().DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().failed_record_count(), 0);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records_size(), records.size());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().encryption_type(), YDS_V1::EncryptionType::NONE);

            TString dataStr = "9876543210";
            auto putRecordResult = client.PutRecord(streamPath, {dataStr, dataStr, ""}).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(putRecordResult.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(putRecordResult.GetStatus(), EStatus::SUCCESS, putRecordResult.GetIssues().ToString());
            Cerr << "PutRecord response = " << putRecordResult.GetResult().DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(putRecordResult.GetResult().shard_id(), "shard-000004");
            UNIT_ASSERT_VALUES_EQUAL(putRecordResult.GetResult().sequence_number(), "7");
            UNIT_ASSERT_VALUES_EQUAL(putRecordResult.GetResult().encryption_type(),
                                     YDS_V1::EncryptionType::NONE);

        }
    }

    Y_UNIT_TEST(TestPutEmptyMessage) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        SET_YDS_LOCALS;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(5)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        kikimr->GetRuntime()->SetLogPriority(NKikimrServices::PQ_READ_PROXY, NLog::EPriority::PRI_DEBUG);
        kikimr->GetRuntime()->SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NLog::EPriority::PRI_DEBUG);

        NYDS_V1::TDataStreamsClient client(*driver, TCommonClientSettings().AuthToken("user2@builtin"));


        auto putRecordResult = client.PutRecord("/Root/" + streamName, {"", "key", ""}).ExtractValueSync();
        Cerr << putRecordResult.GetResult().DebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(putRecordResult.IsTransportError(), false);
        UNIT_ASSERT_VALUES_EQUAL_C(putRecordResult.GetStatus(), EStatus::SUCCESS, putRecordResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(putRecordResult.GetResult().sequence_number(), "0");

        NYdb::NPersQueue::TPersQueueClient pqClient(*driver);

        {
            auto result = testServer.DataStreamsClient->RegisterStreamConsumer(streamName, "user1", NYDS_V1::TRegisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumer().consumer_name(), "user1");
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumer().consumer_status(),
                                     YDS_V1::ConsumerDescription_ConsumerStatus_ACTIVE);
        }

        auto session = pqClient.CreateReadSession(NYdb::NPersQueue::TReadSessionSettings()
                                                          .ConsumerName("user1")
                                                          .DisableClusterDiscovery(true)
                                                          .AppendTopics(NYdb::NPersQueue::TTopicReadSettings().Path("/Root/" + streamName)));
        while (true) {
            auto event = session->GetEvent(true);

            if (auto* dataReceivedEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (const auto& item : dataReceivedEvent->GetMessages()) {
                    Cerr << item.DebugString(true) << Endl;
                    UNIT_ASSERT_VALUES_EQUAL(item.GetData(), "");
                    UNIT_ASSERT_VALUES_EQUAL(item.GetPartitionKey(), "key");
                }
                UNIT_ASSERT_VALUES_EQUAL(dataReceivedEvent->GetMessages().size(), 1);
                break;
            } else if (auto* createPartitionStreamEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*event)) {
                createPartitionStreamEvent->Confirm();
            } else if (auto* destroyPartitionStreamEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent>(&*event)) {
                destroyPartitionStreamEvent->Confirm();
            } else if (auto* closeSessionEvent = std::get_if<NYdb::NPersQueue::TSessionClosedEvent>(&*event)) {
                UNIT_ASSERT(false);
                break;
            } else {
                Y_ABORT("not a data!");
            }
        }
    }

    Y_UNIT_TEST(TestListStreamConsumers) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        // Create stream -> OK
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(5)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // List stream consumers -> OK
        {
            auto result = testServer.DataStreamsClient->ListStreamConsumers(streamName,
                NYDS_V1::TListStreamConsumersSettings().MaxResults(100)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumers().size(), 0);
        }

        // List stream consumers more than allowed -> get BAD_REQUEST
        {
            auto result = testServer.DataStreamsClient->ListStreamConsumers(streamName,
                NYDS_V1::TListStreamConsumersSettings().MaxResults(10001)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        // List stream consumers less than allowed -> get BAD_REQUEST
        {
            auto result = testServer.DataStreamsClient->ListStreamConsumers(streamName,
                NYDS_V1::TListStreamConsumersSettings().MaxResults(0)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        // List not created stream -> get SCHEME_ERROR
        {
            auto result = testServer.DataStreamsClient->ListStreamConsumers(streamName + "_XXX",
                NYDS_V1::TListStreamConsumersSettings().MaxResults(100)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        }

        // Deregister unregistered consumer -> get NOT_FOUND
        {
            auto result = testServer.DataStreamsClient->DeregisterStreamConsumer(streamName, "user1",
                NYDS_V1::TDeregisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::NOT_FOUND);
        }

        // Register consumer 1 -> OK
        {
            auto result = testServer.DataStreamsClient->RegisterStreamConsumer(streamName, "user1",
                NYDS_V1::TRegisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumer().consumer_name(), "user1");
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumer().consumer_status(),
                                     YDS_V1::ConsumerDescription_ConsumerStatus_ACTIVE);
        }

        // Register consumer 2 -> OK
        {
            auto result = testServer.DataStreamsClient->RegisterStreamConsumer(streamName, "user2",
                NYDS_V1::TRegisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumer().consumer_name(), "user2");
        }

        // List stream consumers -> OK, get 2
        {
            auto result = testServer.DataStreamsClient->ListStreamConsumers(streamName,
                NYDS_V1::TListStreamConsumersSettings().MaxResults(100)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumers().size(), 2);
        }

        // Deregister consumer 2 -> OK
        {
            auto result = testServer.DataStreamsClient->DeregisterStreamConsumer(streamName, "user2",
                NYDS_V1::TDeregisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // List stream consumers -> OK, there's only 1 now
        {
            auto result = testServer.DataStreamsClient->ListStreamConsumers(streamName,
                NYDS_V1::TListStreamConsumersSettings().MaxResults(100)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumers().size(), 1);
        }

        // Register stream consumer -> OK
        {
            auto result = testServer.DataStreamsClient->RegisterStreamConsumer(streamName, "user2",
                NYDS_V1::TRegisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumer().consumer_name(), "user2");
        }

        // Add already added consumer -> get ALREADY_EXISTS
        {
            auto result = testServer.DataStreamsClient->RegisterStreamConsumer(streamName, "user2",
                NYDS_V1::TRegisterStreamConsumerSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ALREADY_EXISTS);
        }

        // List stream consumers with pages of size 1 -> OK
        {
            auto result = testServer.DataStreamsClient->ListStreamConsumers(streamName,
                NYDS_V1::TListStreamConsumersSettings().MaxResults(1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumers().size(), 1);
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetResult().next_token().size(), 0);

            auto nextToken = result.GetResult().next_token();
            result = testServer.DataStreamsClient->ListStreamConsumers("",
                NYDS_V1::TListStreamConsumersSettings().NextToken(nextToken)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumers().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().next_token().size(), 0);
        }

        // List stream consumers with page size 1, but then provide corrupted or expired token
        {
            auto result = testServer.DataStreamsClient->ListStreamConsumers(streamName,
                NYDS_V1::TListStreamConsumersSettings().MaxResults(1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().consumers().size(), 1);
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetResult().next_token().size(), 0);

            auto makeNextToken = [streamName](ui64 timestamp) {
                NKikimrPQ::TYdsNextToken protoNextToken;
                protoNextToken.SetStreamArn(streamName);
                protoNextToken.SetAlreadyRead(1);
                protoNextToken.SetMaxResults(1);
                protoNextToken.SetCreationTimestamp(timestamp);
                TString stringNextToken;
                Y_PROTOBUF_SUPPRESS_NODISCARD protoNextToken.SerializeToString(&stringNextToken);
                TString encodedNextToken;
                Base64Encode(stringNextToken, encodedNextToken);
                return encodedNextToken;
            };

            auto nextToken = makeNextToken(TInstant::Now().MilliSeconds() - 300001);
            result = testServer.DataStreamsClient->ListStreamConsumers("",
                NYDS_V1::TListStreamConsumersSettings().NextToken(nextToken)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

            nextToken = makeNextToken(TInstant::Now().MilliSeconds() + 1000);
            result = testServer.DataStreamsClient->ListStreamConsumers("",
                NYDS_V1::TListStreamConsumersSettings().NextToken(nextToken)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

            nextToken = makeNextToken(0);
            result = testServer.DataStreamsClient->ListStreamConsumers("",
                NYDS_V1::TListStreamConsumersSettings().NextToken(nextToken)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

            result = testServer.DataStreamsClient->ListStreamConsumers("",
                NYDS_V1::TListStreamConsumersSettings().NextToken("some_garbage")).ExtractValueSync();
            result = testServer.DataStreamsClient->ListStreamConsumers("",
                NYDS_V1::TListStreamConsumersSettings().NextToken("some_garbage")).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        // Delete stream -> OK
        {
            auto result = testServer.DataStreamsClient->DeleteStream(streamName,
                NYDS_V1::TDeleteStreamSettings().EnforceConsumerDeletion(true)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // List consumers of deleted stream -> get BAD_REQUEST
        {
            auto result = testServer.DataStreamsClient->ListStreamConsumers(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(TestGetShardIterator) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName, \
                NYDS_V1::TCreateStreamSettings().ShardCount(5)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            std::vector<NYDS_V1::TDataRecord> records;
            for (ui32 i = 1; i <= 30; ++i) {
                TString data = Sprintf("%04u", i);
                records.push_back({data, data, ""});
            }

            auto result = testServer.DataStreamsClient->PutRecords(streamName, records).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000001",
                YDS_V1::ShardIteratorType::LATEST).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000010",
                YDS_V1::ShardIteratorType::LATEST).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000001",
                YDS_V1::ShardIteratorType::AT_SEQUENCE_NUMBER,
                NYDS_V1::TGetShardIteratorSettings().StartingSequenceNumber("0")
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000001",
                YDS_V1::ShardIteratorType::AFTER_SEQUENCE_NUMBER,
                NYDS_V1::TGetShardIteratorSettings().StartingSequenceNumber("0")
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000001",
                YDS_V1::ShardIteratorType::AT_SEQUENCE_NUMBER,
                NYDS_V1::TGetShardIteratorSettings().StartingSequenceNumber("100")
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000001",
                YDS_V1::ShardIteratorType::AT_SEQUENCE_NUMBER,
                NYDS_V1::TGetShardIteratorSettings().StartingSequenceNumber("001122")
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000001",
                YDS_V1::ShardIteratorType::AT_SEQUENCE_NUMBER,
                NYDS_V1::TGetShardIteratorSettings().StartingSequenceNumber("garbage_value")
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000001",
                YDS_V1::ShardIteratorType::AT_TIMESTAMP,
                NYDS_V1::TGetShardIteratorSettings().Timestamp(TInstant::Now().MilliSeconds() * 0.99)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            NKikimrPQ::TYdsShardIterator protoShardIterator;
            TString decoded;
            Base64Decode(result.GetResult().shard_iterator(), decoded);
            UNIT_ASSERT(protoShardIterator.ParseFromString(decoded));
            UNIT_ASSERT_VALUES_EQUAL(protoShardIterator.GetStreamName(), "/Root/" + streamName);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000001",
                YDS_V1::ShardIteratorType::AT_TIMESTAMP,
                NYDS_V1::TGetShardIteratorSettings().Timestamp(TInstant::Now().MilliSeconds() * 1.3)
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(TestGetRecordsStreamWithSingleShard) {
        TInsecureDatastreamsTestServer testServer;

        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const ui32 recordsCount = 30;
        std::vector<NYDS_V1::TDataRecord> records;
        for (ui32 i = 1; i <= recordsCount; ++i) {
            TString data = Sprintf("%04u", i);
            records.push_back({data, data, ""});
        }

        {
            auto result = testServer.DataStreamsClient->PutRecords(streamName, records).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
        }

        TString shardIterator;

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000000",
                YDS_V1::ShardIteratorType::LATEST).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), 0);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000000",
                YDS_V1::ShardIteratorType::TRIM_HORIZON).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), recordsCount);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000000",
                YDS_V1::ShardIteratorType::AT_SEQUENCE_NUMBER,
                NYDS_V1::TGetShardIteratorSettings().StartingSequenceNumber("0002")
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), recordsCount - 2);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000000",
                YDS_V1::ShardIteratorType::AFTER_SEQUENCE_NUMBER,
                NYDS_V1::TGetShardIteratorSettings().StartingSequenceNumber("0002")
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), recordsCount - 3);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000000",
                YDS_V1::ShardIteratorType::AFTER_SEQUENCE_NUMBER,
                NYDS_V1::TGetShardIteratorSettings().StartingSequenceNumber("99999")
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), 0);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000000",
                YDS_V1::ShardIteratorType::LATEST).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        //
        // in order to be sure that the value of WriteTimestampMs will be greater than the current time
        //
        Sleep(TDuration::Seconds(1));

        {
            auto result = testServer.DataStreamsClient->PutRecords(streamName, records).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), recordsCount);
        }
    }

    Y_UNIT_TEST(TestGetRecordsWithoutPermission) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        SET_YDS_LOCALS;
        {
            auto result = testServer.DataStreamsClient->CreateStream("/Root/" + streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        std::string data;
        data.resize(1_KB);
        std::iota(data.begin(), data.end(), 1);
        TString id{"0000"};
        {
            NYDS_V1::TDataRecord dataRecord{{data.begin(), data.end()}, id, ""};
            auto result = testServer.DataStreamsClient->PutRecord(streamName, dataRecord).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        NYDS_V1::TDataStreamsClient client(*driver, TCommonClientSettings().AuthToken("user2@builtin"));
        NYdb::NScheme::TSchemeClient schemeClient(*driver);

        TString shardIterator;
        {
            auto result = client.GetShardIterator(
                    streamName, "shard-000000",
                    YDS_V1::ShardIteratorType::TRIM_HORIZON
                ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        }

        {
            auto result = client.GetShardIterator(
                    "/Root/" + streamName, "Shard-000000",
                    YDS_V1::ShardIteratorType::TRIM_HORIZON
                ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        }

        {
            NYdb::NScheme::TPermissions permissions("user2@builtin", {"ydb.generic.read"});
            auto result = schemeClient.ModifyPermissions("/Root",
                NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        {
            auto result = client.GetShardIterator(
                    streamName, "shard-000000",
                    YDS_V1::ShardIteratorType::TRIM_HORIZON
                ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = client.GetRecords(shardIterator,
                                            NYDS_V1::TGetRecordsSettings().Limit(2)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(std::stoi(result.GetResult().records().begin()->sequence_number()), 0);
        }
    }

    Y_UNIT_TEST(TestGetRecords1MBMessagesOneByOneBySeqNo) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        std::vector<std::string> putData;
        const ui32 recordsCount = 10;
        {
            std::string data;
            data.resize(32_KB);
            std::iota(data.begin(), data.end(), 1);
            std::random_device rd;
            std::mt19937 generator{rd()};
            for (ui32 i = 1; i <= recordsCount; ++i) {
                std::shuffle(data.begin(), data.end(), generator);
                putData.push_back(data);
                {
                    TString id = Sprintf("%04u", i);
                    NYDS_V1::TDataRecord dataRecord{{data.begin(), data.end()}, id, ""};
                    auto result = testServer.DataStreamsClient->PutRecord(streamName, dataRecord).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                }
                Sleep(TDuration::Seconds(1));
            }
        }

        for (ui32 i = 0; i < recordsCount; ++i) {
            TString shardIterator;
            {
                TString id = Sprintf("%04u", i);
                auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000000",
                    YDS_V1::ShardIteratorType::AT_SEQUENCE_NUMBER,
                     NYDS_V1::TGetShardIteratorSettings().StartingSequenceNumber(id)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                shardIterator = result.GetResult().shard_iterator();
            }

            {
                auto result = testServer.DataStreamsClient->GetRecords(shardIterator,
                     NYDS_V1::TGetRecordsSettings().Limit(1)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(
                    std::stoi(result.GetResult().records().begin()->sequence_number()), i);
                UNIT_ASSERT_VALUES_EQUAL(MD5::Calc(result.GetResult().records().begin()->data()),
                                         MD5::Calc(putData[i]));
            }
        }
    }

    Y_UNIT_TEST(TestGetRecords1MBMessagesOneByOneByTS) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const ui32 recordsCount = 24;
        std::vector<ui64> timestamps;
        {
            std::string data;
            data.resize(32_KB);
            std::iota(data.begin(), data.end(), 1);
            std::random_device rd;
            std::mt19937 generator{rd()};
            for (ui32 i = 1; i <= recordsCount; ++i) {
                std::shuffle(data.begin(), data.end(), generator);
                {
                    TString id = Sprintf("%04u", i);
                    NYDS_V1::TDataRecord dataRecord{{data.begin(), data.end()}, id, ""};
                    //
                    // we make sure that the value of WriteTimestampMs is between neighboring timestamps
                    //
                    timestamps.push_back(TInstant::Now().MilliSeconds());
                    Sleep(TDuration::MilliSeconds(500));
                    auto result = testServer.DataStreamsClient->PutRecord(streamName, dataRecord).ExtractValueSync();
                    UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                }
                Sleep(TDuration::MilliSeconds(500));
            }
        }

        for (ui32 i = 0; i < recordsCount; ++i) {
            TString shardIterator;

            {
                auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000000",
                    YDS_V1::ShardIteratorType::AT_TIMESTAMP,
                     NYDS_V1::TGetShardIteratorSettings().Timestamp(timestamps[i])).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                shardIterator = result.GetResult().shard_iterator();
            }

            {
                auto result = testServer.DataStreamsClient->GetRecords(shardIterator,
                     NYDS_V1::TGetRecordsSettings().Limit(1)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(std::stoi(result.GetResult().records().begin()->sequence_number()), i);
            }
        }

    }

    Y_UNIT_TEST(TestGetRecordsStreamWithMultipleShards) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(5)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const ui32 recordsCount = 30;
        {
            std::vector<NYDS_V1::TDataRecord> records;
            for (ui32 i = 1; i <= recordsCount; ++i) {
                TString data = Sprintf("%04u", i);
                records.push_back({data, data, ""});
            }

            auto result = testServer.DataStreamsClient->PutRecords(streamName, records).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
        }

        TString shardIterator;
        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000001",
                YDS_V1::ShardIteratorType::LATEST).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), 0);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000002",
                YDS_V1::ShardIteratorType::LATEST).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), 0);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000003",
                YDS_V1::ShardIteratorType::LATEST).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), 0);
        }

        {
            auto result = testServer.DataStreamsClient->GetShardIterator(streamName, "shard-000002",
                YDS_V1::ShardIteratorType::LATEST).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), 0);
        }
    }

    Y_UNIT_TEST(TestListShards1Shard) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->ListShards(streamName, {},
                NYDS_V1::TListShardsSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            YDS_V1::ShardFilter filter;
            filter.set_type(YDS_V1::ShardFilter::FROM_TRIM_HORIZON);
            auto result = testServer.DataStreamsClient->ListShards(streamName, filter,
                NYDS_V1::TListShardsSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().shards().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().shards().begin()->shard_id(), "shard-000000");
            UNIT_ASSERT_VALUES_UNEQUAL(result.GetResult().shards().begin()->shard_id(), "shard-000001");
        }

        {
            YDS_V1::ShardFilter filter;
            filter.set_type(YDS_V1::ShardFilter::AT_TRIM_HORIZON);
            auto result = testServer.DataStreamsClient->ListShards(streamName, filter,
                NYDS_V1::TListShardsSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().shards().size(), 1);
        }

        {
            YDS_V1::ShardFilter filter;
            filter.set_type(YDS_V1::ShardFilter::AFTER_SHARD_ID);
            filter.set_shard_id("00000");
            auto result = testServer.DataStreamsClient->ListShards(streamName, filter,
                NYDS_V1::TListShardsSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().shards().size(), 1);
        }

        {
            YDS_V1::ShardFilter filter;
            filter.set_type(YDS_V1::ShardFilter::AFTER_SHARD_ID);
            auto result = testServer.DataStreamsClient->ListShards(streamName, filter,
                NYDS_V1::TListShardsSettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(TestUnsupported) {
        TInsecureDatastreamsTestServer testServer;
        {
            auto result = testServer.DataStreamsClient->DescribeLimits().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStreamConsumer().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

        {
            auto result = testServer.DataStreamsClient->AddTagsToStream().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

        {
            auto result = testServer.DataStreamsClient->DisableEnhancedMonitoring().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

        {
            auto result = testServer.DataStreamsClient->EnableEnhancedMonitoring().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

        {
            auto result = testServer.DataStreamsClient->ListTagsForStream().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

        {
            auto result = testServer.DataStreamsClient->MergeShards().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

        {
            auto result = testServer.DataStreamsClient->RemoveTagsFromStream().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

        {
            auto result = testServer.DataStreamsClient->SplitShard().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

        {
            auto result = testServer.DataStreamsClient->StartStreamEncryption().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

        {
            auto result = testServer.DataStreamsClient->StopStreamEncryption().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        }

    }

    Y_UNIT_TEST(TestInvalidRetentionCombinations) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        const ui32 shardCount = 5;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(shardCount)
                                                .RetentionStorageMegabytes(10)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        /*
        { //TODO: datastreams api uses only one retention parameter
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(shardCount)
                                                .RetentionStorageMegabytes(55_KB).RetentionPeriodHours(8 * 24)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }*/

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(shardCount)
                                                .RetentionPeriodHours(6 * 24)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(shardCount).WriteQuotaKbPerSec(127)
                                                .RetentionPeriodHours(24)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(shardCount).WriteQuotaKbPerSec(1025)
                                                .RetentionPeriodHours(24)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName + "b",
                NYDS_V1::TCreateStreamSettings().ShardCount(shardCount)
                                                .RetentionStorageMegabytes(55_KB)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }


        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName + "a",
                NYDS_V1::TCreateStreamSettings().ShardCount(shardCount)
                                                .RetentionPeriodHours(24)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(TestGetRecordsWithBigSeqno) {
        TInsecureDatastreamsTestServer testServer;
        const TString streamName = TStringBuilder() << "stream_" << Y_UNIT_TEST_NAME;
        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName,
                NYDS_V1::TCreateStreamSettings().ShardCount(1)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        std::vector<NYDS_V1::TDataRecord> records;
        records.push_back({
            .Data = "overflow",
            .PartitionKey = "overflow",
            .ExplicitHashDecimal = "",
        });

        {
            auto result = testServer.DataStreamsClient->PutRecords(streamName, records).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
        }

        TString shardIterator;
        {
            auto seqNo = std::to_string(static_cast<ui64>(std::numeric_limits<ui32>::max()) + 5);
            auto result = testServer.DataStreamsClient->GetShardIterator(
                streamName,
                "shard-000000",
                YDS_V1::ShardIteratorType::LATEST,
                NYDS_V1::TGetShardIteratorSettings().StartingSequenceNumber(seqNo.c_str())
            ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            shardIterator = result.GetResult().shard_iterator();
        }

        {
            auto result = testServer.DataStreamsClient->GetRecords(shardIterator).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResult().records().size(), 0);
        }
    }

    Y_UNIT_TEST(ListStreamsValidation) {
        TInsecureDatastreamsTestServer testServer;

        {
            auto result = testServer.DataStreamsClient->ListStreams(
                NYdb::NDataStreams::V1::TListStreamsSettings().Limit(1000000000).Recurse(false)
                ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(Test_AutoPartitioning_Describe) {
        TInsecureDatastreamsTestServer testServer(true);
        SET_YDS_LOCALS;

        TString streamName = "test-topic";
        TString streamName2 = "test-topic-2";

        {
            NYdb::NTopic::TTopicClient pqClient(*testServer.Driver);
            auto settings = NYdb::NTopic::TCreateTopicSettings()
                .BeginConfigurePartitioningSettings()
                    .MinActivePartitions(3)
                    .MaxActivePartitions(7)
                    .BeginConfigureAutoPartitioningSettings()
                        .Strategy(NYdb::NTopic::EAutoPartitioningStrategy::ScaleUpAndDown)
                        .StabilizationWindow(TDuration::Seconds(123))
                        .UpUtilizationPercent(97)
                        .DownUtilizationPercent(13)
                    .EndConfigureAutoPartitioningSettings()
                .EndConfigurePartitioningSettings()
                ;
            auto result = pqClient.CreateTopic(streamName, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& description = result.GetResult().stream_description();
            UNIT_ASSERT_VALUES_EQUAL(description.stream_status(), YDS_V1::StreamDescription::ACTIVE);
            UNIT_ASSERT_VALUES_EQUAL(description.stream_name(), streamName);
            UNIT_ASSERT_VALUES_EQUAL(description.stream_arn(), streamName);
            UNIT_ASSERT_VALUES_EQUAL(description.write_quota_kb_per_sec(), 1_KB);
            UNIT_ASSERT_VALUES_EQUAL(description.retention_period_hours(), 24);

            UNIT_ASSERT_VALUES_EQUAL(description.shards().size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(description.shards(0).sequence_number_range().starting_sequence_number(), "0");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(0).hash_key_range().starting_hash_key(), "0");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(0).hash_key_range().ending_hash_key(), "113427455640312821154458202477256070484");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(1).hash_key_range().starting_hash_key(), "113427455640312821154458202477256070485");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(1).hash_key_range().ending_hash_key(), "226854911280625642308916404954512140969");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(2).hash_key_range().starting_hash_key(), "226854911280625642308916404954512140970");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(2).hash_key_range().ending_hash_key(), "340282366920938463463374607431768211455");

            UNIT_ASSERT_VALUES_EQUAL(description.partitioning_settings().min_active_partitions(), 3);
            UNIT_ASSERT_VALUES_EQUAL(description.partitioning_settings().max_active_partitions(), 7);
            UNIT_ASSERT_VALUES_EQUAL(description.partitioning_settings().auto_partitioning_settings().strategy(), ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);
            UNIT_ASSERT_VALUES_EQUAL(description.partitioning_settings().auto_partitioning_settings().partition_write_speed().stabilization_window().seconds(), 123);
            UNIT_ASSERT_VALUES_EQUAL(description.partitioning_settings().auto_partitioning_settings().partition_write_speed().up_utilization_percent(), 97);
            UNIT_ASSERT_VALUES_EQUAL(description.partitioning_settings().auto_partitioning_settings().partition_write_speed().down_utilization_percent(), 13);
        }

        {
            ui64 txId = 107;
            NPQ::NTest::SplitPartition(*kikimr->GetRuntime(), txId, 1, "a");
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& description = result.GetResult().stream_description();
            UNIT_ASSERT_VALUES_EQUAL(description.stream_status(), YDS_V1::StreamDescription::ACTIVE);
            UNIT_ASSERT_VALUES_EQUAL(description.stream_name(), streamName);
            UNIT_ASSERT_VALUES_EQUAL(description.stream_arn(), streamName);
            UNIT_ASSERT_VALUES_EQUAL(description.write_quota_kb_per_sec(), 1_KB);
            UNIT_ASSERT_VALUES_EQUAL(description.retention_period_hours(), 24);

            UNIT_ASSERT_VALUES_EQUAL(description.shards().size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(description.shards(0).sequence_number_range().starting_sequence_number(), "0");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(0).hash_key_range().starting_hash_key(), "0");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(0).hash_key_range().ending_hash_key(), "113427455640312821154458202477256070484");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(1).hash_key_range().starting_hash_key(), "113427455640312821154458202477256070485");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(1).hash_key_range().ending_hash_key(), "226854911280625642308916404954512140969");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(2).hash_key_range().starting_hash_key(), "226854911280625642308916404954512140970");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(2).hash_key_range().ending_hash_key(), "340282366920938463463374607431768211455");

            UNIT_ASSERT_VALUES_EQUAL(description.shards(3).hash_key_range().starting_hash_key(), "113427455640312821154458202477256070485");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(3).hash_key_range().ending_hash_key(), "128935115591136839671669284847193423872");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(3).parent_shard_id(), "shard-000001");

            UNIT_ASSERT_VALUES_EQUAL(description.shards(4).hash_key_range().starting_hash_key(), "128935115591136839671669284847193423873");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(4).hash_key_range().ending_hash_key(), "226854911280625642308916404954512140969");
            UNIT_ASSERT_VALUES_EQUAL(description.shards(4).parent_shard_id(), "shard-000001");
        }

        {
            auto result = testServer.DataStreamsClient->CreateStream(streamName2,
                NYDS_V1::TCreateStreamSettings()
                    //.ShardCount(3)
                    .BeginConfigurePartitioningSettings()
                        .MinActivePartitions(3)
                        .MaxActivePartitions(7)
                        .BeginConfigureAutoPartitioningSettings()
                            .Strategy(NYdb::NDataStreams::V1::EAutoPartitioningStrategy::ScaleUpAndDown)
                            .StabilizationWindow(TDuration::Seconds(123))
                            .UpUtilizationPercent(97)
                            .DownUtilizationPercent(13)
                        .EndConfigureAutoPartitioningSettings()
                    .EndConfigurePartitioningSettings()
                ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName2).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& d = result.GetResult().stream_description();
            UNIT_ASSERT_VALUES_EQUAL(d.stream_status(), YDS_V1::StreamDescription::ACTIVE);
            UNIT_ASSERT_VALUES_EQUAL(d.stream_name(), streamName2);
            UNIT_ASSERT_VALUES_EQUAL(d.stream_arn(), streamName2);
            UNIT_ASSERT_VALUES_EQUAL(d.write_quota_kb_per_sec(), 1_KB);
            UNIT_ASSERT_VALUES_EQUAL(d.retention_period_hours(), 24);

            UNIT_ASSERT_VALUES_EQUAL(d.shards().size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(d.shards(0).sequence_number_range().starting_sequence_number(), "0");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(0).hash_key_range().starting_hash_key(), "0");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(0).hash_key_range().ending_hash_key(), "113427455640312821154458202477256070484");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(1).hash_key_range().starting_hash_key(), "113427455640312821154458202477256070485");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(1).hash_key_range().ending_hash_key(), "226854911280625642308916404954512140969");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(2).hash_key_range().starting_hash_key(), "226854911280625642308916404954512140970");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(2).hash_key_range().ending_hash_key(), "340282366920938463463374607431768211455");

            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().min_active_partitions(), 3);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().max_active_partitions(), 7);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().strategy(), ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().partition_write_speed().stabilization_window().seconds(), 123);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().partition_write_speed().up_utilization_percent(), 97);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().partition_write_speed().down_utilization_percent(), 13);
        }

        {
            auto result = testServer.DataStreamsClient->UpdateStream(streamName2,
                 NYDS_V1::TUpdateStreamSettings()
                    //.TargetShardCount(3)
                    .BeginConfigurePartitioningSettings()
                        .MinActivePartitions(2)
                        .MaxActivePartitions(11)
                        .BeginConfigureAutoPartitioningSettings()
                            .Strategy(NYdb::NDataStreams::V1::EAutoPartitioningStrategy::ScaleUp)
                            .StabilizationWindow(TDuration::Seconds(121))
                            .UpUtilizationPercent(93)
                            .DownUtilizationPercent(17)
                        .EndConfigureAutoPartitioningSettings()
                    .EndConfigurePartitioningSettings()
                ).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName2).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& d = result.GetResult().stream_description();
            UNIT_ASSERT_VALUES_EQUAL(d.stream_status(), YDS_V1::StreamDescription::ACTIVE);
            UNIT_ASSERT_VALUES_EQUAL(d.stream_name(), streamName2);
            UNIT_ASSERT_VALUES_EQUAL(d.stream_arn(), streamName2);
            UNIT_ASSERT_VALUES_EQUAL(d.write_quota_kb_per_sec(), 1_KB);
            UNIT_ASSERT_VALUES_EQUAL(d.retention_period_hours(), 24);

            UNIT_ASSERT_VALUES_EQUAL(d.shards().size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(d.shards(0).sequence_number_range().starting_sequence_number(), "0");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(0).hash_key_range().starting_hash_key(), "0");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(0).hash_key_range().ending_hash_key(), "113427455640312821154458202477256070484");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(1).hash_key_range().starting_hash_key(), "113427455640312821154458202477256070485");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(1).hash_key_range().ending_hash_key(), "226854911280625642308916404954512140969");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(2).hash_key_range().starting_hash_key(), "226854911280625642308916404954512140970");
            UNIT_ASSERT_VALUES_EQUAL(d.shards(2).hash_key_range().ending_hash_key(), "340282366920938463463374607431768211455");

            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().min_active_partitions(), 2);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().max_active_partitions(), 11);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().strategy(), ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().partition_write_speed().stabilization_window().seconds(), 121);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().partition_write_speed().up_utilization_percent(), 93);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().partition_write_speed().down_utilization_percent(), 17);
        }

        {
            auto result = testServer.DataStreamsClient->UpdateStream(streamName2,
                 NYDS_V1::TUpdateStreamSettings()
                    .BeginConfigurePartitioningSettings()
                        .MaxActivePartitions(0)
                        .BeginConfigureAutoPartitioningSettings()
                            .Strategy(NYdb::NDataStreams::V1::EAutoPartitioningStrategy::Disabled)
                        .EndConfigureAutoPartitioningSettings()
                    .EndConfigurePartitioningSettings()
                ).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            if (result.GetStatus() != EStatus::SUCCESS) {
                result.GetIssues().PrintTo(Cerr);
            }
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = testServer.DataStreamsClient->DescribeStream(streamName2).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            Cerr << result.GetIssues().ToString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto& d = result.GetResult().stream_description();
            UNIT_ASSERT_VALUES_EQUAL(d.stream_status(), YDS_V1::StreamDescription::ACTIVE);
            UNIT_ASSERT_VALUES_EQUAL(d.partitioning_settings().auto_partitioning_settings().strategy(), ::Ydb::DataStreams::V1::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED);
        }

    }

}
