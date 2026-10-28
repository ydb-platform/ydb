#include "helpers/grpc_server.h"

#include <ydb/public/api/grpc/ydb_udf_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_udf.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/common/network.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>

#include <functional>
#include <thread>

#ifdef _unix_
    #include <fcntl.h>
    #include <sys/stat.h>
    #include <unistd.h>
#endif

using namespace NYdb;
using namespace NYdb::NUdf;

namespace {

    class TUdfService: public Ydb::Udf::V1::UdfService::Service {
    public:
        using TStream = grpc::ServerReaderWriter<Ydb::Udf::UploadModuleResponse, Ydb::Udf::UploadModuleChunk>;
        std::function<grpc::Status(TStream&)> Upload;

        grpc::Status UploadModule(grpc::ServerContext*, TStream* stream) override {
            return Upload(*stream);
        }

        grpc::Status DescribeModule(grpc::ServerContext*, const Ydb::Udf::DescribeModuleRequest*,
                                    Ydb::Udf::DescribeModuleResponse* response) override {
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            operation->mutable_result()->PackFrom(Ydb::Udf::DescribeModuleResult());
            return grpc::Status::OK;
        }
    };

    struct TFixture {
        NTesting::TPortHolder Port = NTesting::GetFreePort();
        TUdfService Service;
        std::string Address = "localhost:" + std::to_string(static_cast<ui16>(Port));
        std::unique_ptr<grpc::Server> Server = StartGrpcServer(Address, Service);
        TDriver Driver{TDriverConfig().SetEndpoint(Address).SetClientThreadsNum(1)};
        TUdfClient Client{Driver};
    };

    Ydb::Udf::UploadModuleResponse SuccessResponse() {
        Ydb::Udf::UploadModuleResponse response;
        auto* operation = response.mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        Ydb::Udf::UploadModuleResult result;
        result.set_name("module");
        operation->mutable_result()->PackFrom(result);
        return response;
    }

    void ReadUpload(TUdfService::TStream& stream, std::string& body) {
        Ydb::Udf::UploadModuleChunk chunk;
        while (stream.Read(&chunk)) {
            body += chunk.data();
        }
    }

} // namespace

Y_UNIT_TEST_SUITE(UdfClient) {
    Y_UNIT_TEST(CompileTimestampPresence) {
        Ydb::Udf::DescribeModuleResult proto;
        proto.add_platforms()->set_cpu_spec("pending");
        auto* platform = proto.add_platforms();
        platform->set_cpu_spec("ready");
        platform->mutable_compile_started_at()->set_seconds(123);
        platform->mutable_compile_started_at()->set_nanos(456000);
        platform->mutable_compile_finished_at()->set_seconds(789);
        // Explicit epoch is present, unlike a missing timestamp.
        proto.add_platforms()->mutable_compile_started_at();
        TDescribeModuleResult result(TStatus(EStatus::SUCCESS, {}), std::move(proto));
        const auto& platforms = result.GetPlatforms();
        UNIT_ASSERT(!platforms[0].CompileStartedAt);
        UNIT_ASSERT(!platforms[0].CompileFinishedAt);
        UNIT_ASSERT_VALUES_EQUAL(*platforms[1].CompileStartedAt, TInstant::MicroSeconds(123000456));
        UNIT_ASSERT_VALUES_EQUAL(*platforms[1].CompileFinishedAt, TInstant::Seconds(789));
        UNIT_ASSERT(platforms[2].CompileStartedAt);
        UNIT_ASSERT_VALUES_EQUAL(*platforms[2].CompileStartedAt, TInstant::Zero());
    }

    Y_UNIT_TEST(FinalTransportErrorOverridesSuccessfulMessage) {
        TFixture fixture;
        fixture.Service.Upload = [](TUdfService::TStream& stream) {
            std::string body;
            ReadUpload(stream, body);
            stream.Write(SuccessResponse());
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "final transport error");
        };
        auto result = fixture.Client.UploadModule("body").GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "final transport error");
    }

    Y_UNIT_TEST(ServerErrorBeforeBodyCompletes) {
        TFixture fixture;
        fixture.Service.Upload = [](TUdfService::TStream& stream) {
            Ydb::Udf::UploadModuleChunk metadata;
            stream.Read(&metadata);
            auto response = SuccessResponse();
            response.mutable_operation()->set_status(Ydb::StatusIds::BAD_REQUEST);
            response.mutable_operation()->clear_result();
            response.mutable_operation()->add_issues()->set_message("invalid manifest");
            stream.Write(response);
            return grpc::Status::OK;
        };
        auto result = fixture.Client.UploadModule(std::string(1024 * 1024, 'x'),
                                                  TUploadModuleSettings().ChunkSize(1))
                          .GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "invalid manifest");
    }

    Y_UNIT_TEST(UnfinishedOperationRejected) {
        TFixture fixture;
        fixture.Service.Upload = [](TUdfService::TStream& stream) {
            std::string body;
            ReadUpload(stream, body);
            auto response = SuccessResponse();
            response.mutable_operation()->set_ready(false);
            stream.Write(response);
            return grpc::Status::OK;
        };
        auto result = fixture.Client.UploadModule("body").GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "unfinished operation");
    }

    Y_UNIT_TEST(InvalidResultRejected) {
        TFixture fixture;
        fixture.Service.Upload = [](TUdfService::TStream& stream) {
            std::string body;
            ReadUpload(stream, body);
            auto response = SuccessResponse();
            response.mutable_operation()->mutable_result()->PackFrom(Ydb::Udf::DescribeModuleResult());
            stream.Write(response);
            return grpc::Status::OK;
        };
        UNIT_ASSERT(!fixture.Client.UploadModule("body").GetValueSync().IsSuccess());
    }

    Y_UNIT_TEST(MultiChunkFileOutlivesClient) {
        TFixture fixture;
        const std::string body(100000, 'x');
        fixture.Service.Upload = [&](TUdfService::TStream& stream) {
            std::string received;
            ReadUpload(stream, received);
            if (received != body) {
                return grpc::Status(grpc::StatusCode::DATA_LOSS, "body mismatch");
            }
            stream.Write(SuccessResponse());
            return grpc::Status::OK;
        };
        TTempDir dir;
        const auto path = dir.Path() / "module.wasm";
        TFileOutput(path).Write(body);
        auto future = [&] {
            TUdfClient client(fixture.Driver);
            return client.UploadModuleFromFile(path.GetPath(), TUploadModuleSettings().ChunkSize(1000));
        }();
        auto result = future.GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetName(), "module");
    }

    Y_UNIT_TEST(DriverStopCompletesPendingUpload) {
        auto accepted = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        TFixture fixture;
        fixture.Service.Upload = [&](TUdfService::TStream& stream) {
            std::string body;
            ReadUpload(stream, body);
            stream.Write(SuccessResponse());
            accepted.SetValue();
            release.GetFuture().Wait();
            return grpc::Status::OK;
        };
        auto upload = fixture.Client.UploadModule("body");
        const bool received = accepted.GetFuture().Wait(TDuration::Seconds(5));
        fixture.Driver.Stop(true);
        const bool completed = upload.Wait(TDuration::Seconds(5));
        release.SetValue();
        UNIT_ASSERT(received);
        UNIT_ASSERT(completed);
        UNIT_ASSERT(!upload.GetValueSync().IsSuccess());
    }

    Y_UNIT_TEST(FileUploadAfterDriverStopCompletesFuture) {
        TFixture fixture;
        TTempDir dir;
        const auto path = dir.Path() / "module.wasm";
        TFileOutput(path).Write("body");
        fixture.Driver.Stop(true);
        auto upload = fixture.Client.UploadModuleFromFile(path.GetPath());
        UNIT_ASSERT(upload.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT(!upload.GetValueSync().IsSuccess());
    }

    Y_UNIT_TEST(MissingFileCompletesFuture) {
        TFixture fixture;
        TTempDir dir;
        auto result = fixture.Client.UploadModuleFromFile((dir.Path() / "missing").GetPath()).GetValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Cannot read module file");
    }

#ifdef _unix_
    Y_UNIT_TEST(BlockedFileOpenDoesNotBlockCallerOrDriverResponses) {
        TFixture fixture;
        TTempDir dir;
        const auto path = dir.Path() / "fifo";
        UNIT_ASSERT_VALUES_EQUAL(mkfifo(path.c_str(), 0600), 0);
        auto called = NThreading::NewPromise<TAsyncUploadModuleResult>();
        std::thread caller([&] {
            called.SetValue(fixture.Client.UploadModuleFromFile(path.GetPath()));
        });
        // Always unblock the file before asserting, including on a regression.
        const bool returned = called.GetFuture().Wait(TDuration::Seconds(5));
        auto describe = fixture.Client.DescribeModule("module");
        const bool responsive = describe.Wait(TDuration::Seconds(5));
        const int writer = open(path.c_str(), O_RDWR | O_NONBLOCK);
        caller.join();
        auto upload = called.GetFuture().GetValueSync();
        const bool completed = upload.Wait(TDuration::Seconds(5));
        if (writer >= 0) {
            close(writer);
        }
        UNIT_ASSERT(returned);
        UNIT_ASSERT(responsive);
        UNIT_ASSERT(completed);
        UNIT_ASSERT(describe.GetValueSync().IsSuccess());
        UNIT_ASSERT(!upload.GetValueSync().IsSuccess()); // FIFO is not seekable.
    }
#endif
} // Y_UNIT_TEST_SUITE(UdfClient)
