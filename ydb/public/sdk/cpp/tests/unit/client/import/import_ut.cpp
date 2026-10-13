#include <ydb/public/api/grpc/ydb_import_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/import/import.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <mutex>

namespace NYdb::inline Dev::NImport {
    namespace {

        class TImportService: public Ydb::Import::V1::ImportService::Service {
        public:
            grpc::Status ImportFromS3(
                grpc::ServerContext*,
                const Ydb::Import::ImportFromS3Request* request,
                Ydb::Import::ImportFromS3Response* response) override {
                {
                    std::lock_guard lock(Mutex_);
                    Request_ = *request;
                    ++Requests_;
                }
                auto* operation = response->mutable_operation();
                operation->set_ready(true);
                operation->set_status(Ydb::StatusIds::SUCCESS);
                operation->mutable_result();
                Ydb::Import::ImportFromS3Metadata metadata;
                *metadata.mutable_settings() = request->settings();
                operation->mutable_metadata()->PackFrom(metadata);
                return grpc::Status::OK;
            }

            Ydb::Import::ImportFromS3Request GetRequest() const {
                std::lock_guard lock(Mutex_);
                return Request_;
            }

            size_t GetRequestCount() const {
                std::lock_guard lock(Mutex_);
                return Requests_;
            }

        private:
            mutable std::mutex Mutex_;
            Ydb::Import::ImportFromS3Request Request_;
            size_t Requests_ = 0;
        };

        class TServer {
        public:
            TServer() {
                grpc::ServerBuilder builder;
                int port = 0;
                builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
                builder.RegisterService(&Service);
                Server_ = builder.BuildAndStart();
                UNIT_ASSERT(Server_);
                UNIT_ASSERT(port != 0);
                Endpoint = "127.0.0.1:" + std::to_string(port);
            }

            ~TServer() {
                Server_->Shutdown();
                Server_->Wait();
            }

        public:
            TImportService Service;
            std::string Endpoint;

        private:
            std::unique_ptr<grpc::Server> Server_;
        };

    } // namespace

    Y_UNIT_TEST_SUITE(ImportSourceFormat) {
        Y_UNIT_TEST(SerializeRealImportRequests) {
            TServer server;
            TDriver driver(TDriverConfig().SetEndpoint(server.Endpoint));
            TImportClient client(driver);

            const auto check = [&](const TImportFromS3Settings& settings, auto expected) {
                const auto result = client.ImportFromS3(settings).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                const auto request = server.Service.GetRequest();
                UNIT_ASSERT_VALUES_EQUAL(request.settings().source_format(), expected);
                UNIT_ASSERT_VALUES_EQUAL(request.settings().items_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(request.settings().items(0).source_prefix(), "backup/with_underscores");
                UNIT_ASSERT_VALUES_EQUAL(request.settings().items(0).destination_path(), "/Root/restored");
                UNIT_ASSERT_VALUES_EQUAL(request.settings().bucket(), "backup-bucket");
            };
            const auto settings = TImportFromS3Settings()
                                      .Endpoint("s3.example")
                                      .Bucket("backup-bucket")
                                      .AppendItem({"backup/with_underscores", "/Root/restored"})
                                      .ClientTimeout(TDuration::Seconds(5));
            check(settings, Ydb::Import::ImportFromS3Settings::SOURCE_FORMAT_UNSPECIFIED);
            auto explicitYdb = settings;
            explicitYdb.SourceFormat(EImportSourceFormat::Ydb);
            check(explicitYdb, Ydb::Import::ImportFromS3Settings::SOURCE_FORMAT_UNSPECIFIED);
            auto yt = settings;
            yt.SourceFormat(EImportSourceFormat::Yt);
            check(yt, Ydb::Import::ImportFromS3Settings::SOURCE_FORMAT_YT);
        }

        Y_UNIT_TEST(ParseMetadataSourceFormats) {
            using TProto = Ydb::Import::ImportFromS3Settings;
            const std::pair<TProto::SourceFormat, EImportSourceFormat> cases[] = {
                {TProto::SOURCE_FORMAT_UNSPECIFIED, EImportSourceFormat::Ydb},
                {TProto::SOURCE_FORMAT_YDB, EImportSourceFormat::Ydb},
                {TProto::SOURCE_FORMAT_YT, EImportSourceFormat::Yt},
                {static_cast<TProto::SourceFormat>(12345), EImportSourceFormat::Unknown},
            };
            for (const auto& [wire, expected] : cases) {
                Ydb::Import::ImportFromS3Metadata metadata;
                metadata.mutable_settings()->set_source_format(wire);
                auto* item = metadata.mutable_settings()->add_items();
                item->set_source_prefix("backup/prefix");
                item->set_destination_path("/Root/table");
                Ydb::Operations::Operation operation;
                operation.set_ready(true);
                operation.set_status(Ydb::StatusIds::SUCCESS);
                operation.mutable_metadata()->PackFrom(metadata);
                TImportFromS3Response response(TStatus(EStatus::SUCCESS, {}), std::move(operation));
                UNIT_ASSERT(response.Metadata().Settings.SourceFormat_ == expected);
                UNIT_ASSERT_VALUES_EQUAL(response.Metadata().Settings.Item_.size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(response.Metadata().Settings.Item_[0].Src, "backup/prefix");
            }
        }

        Y_UNIT_TEST(RejectInvalidSourceFormatBeforeRpc) {
            TServer server;
            TDriver driver(TDriverConfig().SetEndpoint(server.Endpoint));
            TImportClient client(driver);
            for (const auto format : {EImportSourceFormat::Unknown, static_cast<EImportSourceFormat>(-1)}) {
                const auto settings = TImportFromS3Settings().SourceFormat(format);
                UNIT_ASSERT_EXCEPTION(client.ImportFromS3(settings), TContractViolation);
            }
            UNIT_ASSERT_VALUES_EQUAL(server.Service.GetRequestCount(), 0);
        }
    } // Y_UNIT_TEST_SUITE(ImportSourceFormat)

} // namespace NYdb::inline Dev::NImport
