#include "helpers/grpc_server.h"

#include <ydb/public/api/grpc/ydb_keyvalue_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_keyvalue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/scheme_helpers/helpers.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <functional>

using namespace NYdb;

namespace {

    class TKeyValueService: public Ydb::KeyValue::V1::KeyValueService::Service {
    public:
        std::function<grpc::Status(grpc::ServerContext*, const Ydb::KeyValue::DropVolumeRequest*,
                                   Ydb::KeyValue::DropVolumeResponse*)>
            Drop;

        grpc::Status DropVolume(grpc::ServerContext* context, const Ydb::KeyValue::DropVolumeRequest* request,
                                Ydb::KeyValue::DropVolumeResponse* response) override {
            return Drop(context, request, response);
        }
    };

    struct TFixture {
        NTesting::TPortHolder Port = NTesting::GetFreePort();
        TKeyValueService Service;
        std::string Address = "localhost:" + std::to_string(static_cast<ui16>(Port));
        std::unique_ptr<grpc::Server> Server = StartGrpcServer(Address, Service);
        TDriver Driver{TDriverConfig().SetEndpoint(Address).SetClientThreadsNum(1)};
        NKeyValue::TKeyValueClient Client{Driver};
    };

} // namespace

Y_UNIT_TEST_SUITE(KeyValueClient) {
    Y_UNIT_TEST(DropVolumeSettings) {
        TFixture fixture;
        Ydb::KeyValue::DropVolumeRequest actualRequest;
        std::string actualHeader;
        fixture.Service.Drop = [&](grpc::ServerContext* context, const auto* request, auto* response) {
            actualRequest = *request;
            const auto header = context->client_metadata().find("x-test-header");
            if (header != context->client_metadata().end()) {
                actualHeader = std::string(header->second.data(), header->second.size());
            }
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            return grpc::Status::OK;
        };

        const auto directorySettings = NScheme::TRemoveDirectorySettings()
                                           .OperationTimeout(TDuration::Seconds(10))
                                           .CancelAfter(TDuration::Seconds(5))
                                           .ClientTimeout(TDuration::Seconds(20))
                                           .Header({{"x-test-header", "test-value"}});
        const auto result = fixture.Client.DropVolume("/Root/volume", NKeyValue::TDropVolumeSettings(directorySettings)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(actualRequest.path(), "/Root/volume");
        UNIT_ASSERT_VALUES_EQUAL(actualRequest.operation_params().operation_timeout().seconds(), 10);
        UNIT_ASSERT_VALUES_EQUAL(actualRequest.operation_params().cancel_after().seconds(), 5);
        UNIT_ASSERT_VALUES_EQUAL(actualHeader, "test-value");
    }

    Y_UNIT_TEST(DropVolumeSchemeError) {
        TFixture fixture;
        fixture.Service.Drop = [](auto*, const auto*, auto* response) {
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SCHEME_ERROR);
            operation->add_issues()->set_message("Volume does not exist");
            return grpc::Status::OK;
        };

        const auto result = fixture.Client.DropVolume("/Root/missing").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Volume does not exist");
    }

    Y_UNIT_TEST(DropVolumeTransportError) {
        TFixture fixture;
        fixture.Service.Drop = [](auto*, const auto*, auto*) {
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Service is unavailable");
        };

        const auto result = fixture.Client.DropVolume("/Root/volume").GetValueSync();
        UNIT_ASSERT(result.IsTransportError());
    }

    Y_UNIT_TEST(SolomonVolumeSchemeEntry) {
        Ydb::Scheme::Entry proto;
        // Existing servers already return the internal Solomon volume path type.
        proto.set_type(static_cast<Ydb::Scheme::Entry::Type>(8));
        proto.set_name("volume");
        NScheme::TSchemeEntry entry(proto);
        UNIT_ASSERT_VALUES_EQUAL(entry.Type, NScheme::ESchemeEntryType::SolomonVolume);

        Ydb::Scheme::Entry serialized;
        NScheme::SchemeEntryToProto(entry, &serialized);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(serialized.type()), static_cast<int>(Ydb::Scheme::Entry::SOLOMON_VOLUME));
        UNIT_ASSERT_VALUES_EQUAL(serialized.name(), "volume");
    }
} // Y_UNIT_TEST_SUITE(KeyValueClient)
