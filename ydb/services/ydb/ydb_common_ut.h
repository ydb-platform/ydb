#pragma once

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/security/certificate_check/cert_auth_utils.h>
#include <ydb/services/ydb/ydb_dummy.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <util/system/tempfile.h>

#include "ydb_keys_ut.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/csv/api.h> // for WriteCSV()
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>

#include <functional>

using namespace NKikimr;
namespace NYdb {

using namespace Tests;
using namespace NYdb;

struct TKikimrTestSettings {
    static constexpr bool SSL = false;
    static constexpr bool AUTH = false;
    static constexpr bool PrecreatePools = true;
    static constexpr bool EnableSystemViews = true;

    static TString GetCaCrt() { return NYdbSslTestData::CaCrt; }
    static TString GetServerCrt() { return NYdbSslTestData::ServerCrt; }
    static TString GetServerKey() { return NYdbSslTestData::ServerKey; }

    static NKikimr::TCertificateAuthorizationParams GetCertAuthParams() {return {}; }
};

struct TKikimrTestWithAuth : TKikimrTestSettings {
    static constexpr bool AUTH = true;
};

struct TKikimrTestWithAuthAndSsl : TKikimrTestWithAuth {
    static constexpr bool SSL = true;
};

struct TKikimrTestWithServerCert : TKikimrTestWithAuthAndSsl {
    static constexpr bool SSL = true;

    static const TCertAndKey& GetCACertAndKey() {
        static const TCertAndKey ca = GenerateCA(TProps::AsCA());
        return ca;
    }

    static const TCertAndKey& GetServerCert() {
        static const TCertAndKey server = GenerateSignedCert(GetCACertAndKey(), TProps::AsServer());
        return server;
    }

    static TString GetCaCrt() {
        return GetCACertAndKey().Certificate.c_str();
    }

    static TString GetServerCrt() {
        return GetServerCert().Certificate.c_str();
    }

    static TString GetServerKey() {
        return GetServerCert().PrivateKey.c_str();
    }
};

struct TKikimrTestNoSystemViews : TKikimrTestSettings {
    static constexpr bool EnableSystemViews = false;
};

template <typename TestSettings = TKikimrTestSettings>
class TBasicKikimrWithGrpcAndRootSchema {
public:
    TBasicKikimrWithGrpcAndRootSchema(
            NKikimrConfig::TAppConfig appConfig = {},
            const TVector<NKikimrKqp::TKqpSetting>& kqpSettings = {},
            TAutoPtr<TLogBackend> logBackend = {},
            bool enableYq = false,
            TAppPrepare::TFnReg udfFrFactory = nullptr,
            std::function<void(TServerSettings& settings)> builder = nullptr,
            ui16 dynamicNodeCount = 2, ui16 nodeCount = 0)
    {
        ui16 port = PortManager.GetPort(2134);
        ui16 grpc = PortManager.GetPort(2135);

        NKikimrProto::TAuthConfig authConfig = appConfig.GetAuthConfig();
        authConfig.SetUseBuiltinDomain(true);
        ServerSettings = new TServerSettings(port, authConfig);
        ServerSettings->SetGrpcPort(grpc);
        ServerSettings->SetLogBackend(logBackend);
        ServerSettings->SetDomainName("Root");
        ServerSettings->SetDynamicNodeCount(dynamicNodeCount);
        if (nodeCount > 0)
            ServerSettings->SetNodeCount(nodeCount);
        if (TestSettings::PrecreatePools) {
            ServerSettings->AddStoragePool("ssd");
            ServerSettings->AddStoragePool("hdd");
            ServerSettings->AddStoragePool("hdd1");
            ServerSettings->AddStoragePool("hdd2");
        } else {
            ServerSettings->AddStoragePoolType("ssd");
            ServerSettings->AddStoragePoolType("hdd");
            ServerSettings->AddStoragePoolType("hdd1");
            ServerSettings->AddStoragePoolType("hdd2");
        }
        ServerSettings->AppConfig->MergeFrom(appConfig);
        ServerSettings->FeatureFlags = appConfig.GetFeatureFlags();
        ServerSettings->SetKqpSettings(kqpSettings);
        ServerSettings->SetEnableDataColumnForIndexTable(true);
        ServerSettings->SetEnableNotNullColumns(true);
        ServerSettings->SetEnableSystemViews(TestSettings::EnableSystemViews);
        ServerSettings->SetEnableYq(enableYq);
        ServerSettings->Formats = new TFormatFactory;
        ServerSettings->PQConfig = appConfig.GetPQConfig();
        if (appConfig.HasMeteringConfig() && appConfig.GetMeteringConfig().HasMeteringFilePath()) {
            ServerSettings->SetMeteringFilePath(appConfig.GetMeteringConfig().GetMeteringFilePath());
        }
        ServerSettings->RegisterGrpcService<NKikimr::NGRpcService::TGRpcYdbDummyService>("dummy");
        if (udfFrFactory) {
            ServerSettings->SetFrFactory(udfFrFactory);
        }
        if (builder) {
            builder(*ServerSettings);;
        }

        ServerCertificateFile.Write(TestSettings::GetServerCrt().data(), TestSettings::GetServerCrt().size());
        ServerSettings->ServerCertFilePath = ServerCertificateFile.Name();

        Server_.Reset(new TServer(*ServerSettings));
        Tenants_.Reset(new Tests::TTenants(Server_));

        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_DEBUG);
        Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_INFO);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_OLAPSHARD, NActors::NLog::PRI_DEBUG);
        //Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
        if (enableYq) {
            Server_->GetRuntime()->SetLogPriority(NKikimrServices::YQL_PROXY, NActors::NLog::PRI_DEBUG);
            Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::PRI_DEBUG);
            Server_->GetRuntime()->SetLogPriority(NKikimrServices::YQ_CONTROL_PLANE_STORAGE, NActors::NLog::PRI_DEBUG);
            Server_->GetRuntime()->SetLogPriority(NKikimrServices::YQ_CONTROL_PLANE_PROXY, NActors::NLog::PRI_DEBUG);
        }

        NYdbGrpc::TServerOptions grpcOption;
        if (TestSettings::AUTH) {
            grpcOption.SetUseAuth(appConfig.GetDomainsConfig().GetSecurityConfig().GetEnforceUserTokenRequirement()); // In real life UseAuth is initialized with EnforceUserTokenRequirement. To avoid incorrect tests we must do the same.
        }
        grpcOption.SetPort(grpc);
        if (TestSettings::SSL) {
            NYdbGrpc::TSslData sslData;
            sslData.Cert = TestSettings::GetServerCrt();
            sslData.Key = TestSettings::GetServerKey();
            sslData.Root =TestSettings::GetCaCrt();
            sslData.DoRequestClientCertificate = appConfig.GetClientCertificateAuthorization().GetRequestClientCertificate();

            grpcOption.SetSslData(sslData);
        }
        Server_->EnableGRpc(grpcOption);

        TClient annoyingClient(*ServerSettings);
        if (ServerSettings->AppConfig->GetDomainsConfig().GetSecurityConfig().GetEnforceUserTokenRequirement()) {
            annoyingClient.SetSecurityToken("root@builtin");
        }
        annoyingClient.InitRootScheme("Root");
        GRpcPort_ = grpc;
    }

    ui16 GetPort() {
        return GRpcPort_;
    }

    TPortManager& GetPortManager() {
        return PortManager;
    }

    void ResetSchemeCache(TString path, ui32 nodeIndex = 0) {
        TTestActorRuntime* runtime = Server_->GetRuntime();
        TClient annoyingClient(*ServerSettings);
        annoyingClient.RefreshPathCache(runtime, path, nodeIndex);
    }

    TTestActorRuntime* GetRuntime() {
        return Server_->GetRuntime();
    }

    Tests::TServer& GetServer() {
        return *Server_;
    }

    TServerSettings::TPtr ServerSettings;
    Tests::TServer::TPtr Server_;
    THolder<Tests::TTenants> Tenants_;
private:
    TPortManager PortManager;
    ui16 GRpcPort_;
    TTempFileHandle ServerCertificateFile;
};

struct TTestOlap {
    static constexpr const char * StoreName = "OlapStore";
    static constexpr const char * TableName = "OlapTable";
    static constexpr const char * TablePath = "/Root/OlapStore/OlapTable";

    static std::shared_ptr<arrow::Schema> ArrowSchema(
            std::shared_ptr<arrow::DataType> tsType = arrow::timestamp(arrow::TimeUnit::TimeUnit::MICRO))
    {
        return std::make_shared<arrow::Schema>(
            std::vector<std::shared_ptr<arrow::Field>>{
                arrow::field("timestamp", tsType, false),
                arrow::field("resource_type", arrow::utf8()),
                arrow::field("resource_id", arrow::utf8()),
                arrow::field("uid", arrow::utf8(), false),
                arrow::field("level", arrow::int32()),
                arrow::field("message", arrow::utf8()),
                arrow::field("json_payload", arrow::binary()),
                arrow::field("ingested_at", tsType),
                arrow::field("saved_at", tsType),
                arrow::field("request_id", arrow::utf8())
            });
    }

    static std::vector<std::pair<TString, NYdb::EPrimitiveType>> PublicSchema(bool sort = false) {
        std::vector<std::pair<TString, NYdb::EPrimitiveType>> schema = {
            { "timestamp", NYdb::EPrimitiveType::Timestamp },
            { "resource_type", NYdb::EPrimitiveType::Utf8 },
            { "resource_id", NYdb::EPrimitiveType::Utf8 },
            { "uid", NYdb::EPrimitiveType::Utf8 },
            { "level", NYdb::EPrimitiveType::Int32 },
            { "message", NYdb::EPrimitiveType::Utf8 },
            { "json_payload", NYdb::EPrimitiveType::JsonDocument },
            { "ingested_at", NYdb::EPrimitiveType::Timestamp },
            { "saved_at", NYdb::EPrimitiveType::Timestamp },
            { "request_id", NYdb::EPrimitiveType::Utf8 }
        };

        if (sort) {
            std::sort(schema.begin(), schema.end(), [&](const std::pair<TString, NYdb::EPrimitiveType>& x,
                                                        const std::pair<TString, NYdb::EPrimitiveType>& y) {
                return x.first < y.first;
            });
        }

        return schema;
    }

    static void CreateTable(const TServerSettings& settings, ui32 shards = 2,
                            const TString& storeName = StoreName, const TString& tableName = TableName) {
        TString tableDescr = Sprintf(R"(
            Name: "%s"
            ColumnShardCount: 4
            SchemaPresets {
                Name: "default"
                Schema {
                    Columns { Name: "timestamp" Type: "Timestamp" NotNull : true }
                    Columns { Name: "resource_type" Type: "Utf8" }
                    Columns { Name: "resource_id" Type: "Utf8" }
                    Columns { Name: "uid" Type: "Utf8" NotNull : true }
                    Columns { Name: "level" Type: "Int32" }
                    Columns { Name: "message" Type: "Utf8" }
                    Columns { Name: "json_payload" Type: "JsonDocument" }
                    Columns { Name: "ingested_at" Type: "Timestamp" }
                    Columns { Name: "saved_at" Type: "Timestamp" }
                    Columns { Name: "request_id" Type: "Utf8" }
                    KeyColumnNames: "timestamp"
                    KeyColumnNames: "uid"
                    Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                }
            }
        )", storeName.c_str());

        TClient annoyingClient(settings);
        annoyingClient.SetSecurityToken("root@builtin");
        NMsgBusProxy::EResponseStatus status = annoyingClient.CreateOlapStore("/Root", tableDescr);
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);
        status = annoyingClient.CreateColumnTable("/Root", Sprintf(R"(
            Name: "%s/%s"
            ColumnShardCount : %d
            Sharding {
                HashSharding {
                    Function: HASH_FUNCTION_CLOUD_LOGS
                    Columns: ["timestamp", "uid"]
                }
            }
        )", storeName.c_str(), tableName.c_str(), shards));
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::EResponseStatus::MSTATUS_OK);
    }

    static std::shared_ptr<arrow::RecordBatch> SampleBatch(bool strTimestamps = false, ui32 rowsCount = 100) {
        auto schema = ArrowSchema();
        if (strTimestamps) {
            schema = ArrowSchema(arrow::binary());
        }
        auto builders = NArrow::MakeBuilders(schema, rowsCount);

        for (size_t i = 0; i < rowsCount; ++i) {
            std::string s(std::to_string(i));

            // WriteCSV() does not support Timestamp yet, make CSV via strings
            if (strTimestamps) {
                std::string us = s;
                while (us.size() < 6) {
                    us = std::string("0") + us;
                }

                std::string ts = std::string("1970-01-01T00:00:00.") + std::string(us.data(), us.size());

                Y_ABORT_UNLESS(NArrow::Append<arrow::BinaryType>(*builders[0], ts));
                Y_ABORT_UNLESS(NArrow::Append<arrow::BinaryType>(*builders[7], ts));
                Y_ABORT_UNLESS(NArrow::Append<arrow::BinaryType>(*builders[8], ts));
            } else {
                Y_ABORT_UNLESS(NArrow::Append<arrow::TimestampType>(*builders[0], i));
                Y_ABORT_UNLESS(NArrow::Append<arrow::TimestampType>(*builders[7], i));
                Y_ABORT_UNLESS(NArrow::Append<arrow::TimestampType>(*builders[8], i));
            }
            Y_ABORT_UNLESS(NArrow::Append<arrow::StringType>(*builders[1], s));
            Y_ABORT_UNLESS(NArrow::Append<arrow::StringType>(*builders[2], s));
            Y_ABORT_UNLESS(NArrow::Append<arrow::StringType>(*builders[3], s));
            Y_ABORT_UNLESS(NArrow::Append<arrow::Int32Type>(*builders[4], i));
            Y_ABORT_UNLESS(NArrow::Append<arrow::StringType>(*builders[5], s + "str"));
            Y_ABORT_UNLESS(NArrow::Append<arrow::BinaryType>(*builders[6], "{ \"value\": " + s + " }"));
            Y_ABORT_UNLESS(NArrow::Append<arrow::StringType>(*builders[9], s + "str"));
        }

        return arrow::RecordBatch::Make(schema, rowsCount, NArrow::Finish(std::move(builders)));
    }

    static TString ToCSV(const std::shared_ptr<arrow::RecordBatch>& batch, bool withHeader = false) {
        auto res1 = arrow::io::BufferOutputStream::Create();
        Y_ABORT_UNLESS(res1.ok());
        std::shared_ptr<arrow::io::BufferOutputStream> outStream = *res1;

        arrow::csv::WriteOptions options = arrow::csv::WriteOptions::Defaults();
        options.include_header = withHeader;

        auto status = arrow::csv::WriteCSV(*batch, options, outStream.get());
        Y_ABORT_UNLESS(status.ok(), "%s", status.ToString().c_str());

        auto res2 = outStream->Finish();
        Y_ABORT_UNLESS(res2.ok());

        std::shared_ptr<arrow::Buffer> buffer = *res2;
        TString out((const char*)buffer->data(), buffer->size());
        //Cerr << out;
        return out;
    }
};

using TKikimrWithGrpcAndRootSchema = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestSettings>;
using TKikimrWithGrpcAndRootSchemaWithAuth = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuth>;
using TKikimrWithGrpcAndRootSchemaWithAuthAndSsl = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestWithAuthAndSsl>;
using TKikimrWithGrpcAndRootSchemaNoSystemViews = TBasicKikimrWithGrpcAndRootSchema<TKikimrTestNoSystemViews>;

}
