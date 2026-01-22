#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/client/flat_ut_client.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/kafka_proxy/kafka_messages.h>
#include <ydb/core/kafka_proxy/kafka_metrics.h>
#include <ydb/core/kafka_proxy/kafka_constants.h>
#include <ydb/core/kafka_proxy/actors/actors.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <ydb/services/ydb/ydb_keys_ut.h>

#include <ydb/library/testlib/service_mocks/access_service_mock.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <util/system/tempfile.h>

using namespace NYdb;

static constexpr const char NON_CHARGEABLE_USER[] = "superuser@builtin";
static constexpr const char NON_CHARGEABLE_USER_X[] = "superuser_x@builtin";
static constexpr const char NON_CHARGEABLE_USER_Y[] = "superuser_y@builtin";

static constexpr const char DEFAULT_CLOUD_ID[] = "somecloud";
static constexpr const char DEFAULT_FOLDER_ID[] = "somefolder";

static constexpr const ui64 FAKE_SERVERLESS_KAFKA_PROXY_PORT = 19092;

struct WithSslAndAuth: TKikimrTestSettings {
    static constexpr bool SSL = true;
    static constexpr bool AUTH = true;
};
using TKikimrWithGrpcAndRootSchemaSecure = NYdb::TBasicKikimrWithGrpcAndRootSchema<WithSslAndAuth>;

struct TTestServerSettings {
    const TString KafkaApiMode = "1";
    bool Serverless = false;
    bool EnableNativeKafkaBalancing = true;
    bool EnableAutoTopicCreation = false;
    bool EnableAutoConsumerCreation = true;
    bool EnableQuoting = true;
    bool CheckACL = false;
};

template <class TKikimr, bool secure>
class TTestServer {
public:
    TIpPort Port;

    TTestServer(const TTestServerSettings& settings);

    TTestServer(const TString& kafkaApiMode = "1", bool serverless = false, bool enableNativeKafkaBalancing = true,
                bool enableAutoTopicCreation = false, bool enableAutoConsumerCreation = true, bool enableQuoting = true, bool checkACL = false);

public:
    std::unique_ptr<TKikimr> KikimrServer;
    std::unique_ptr<TDriver> Driver;
    THolder<TTempFileHandle> MeteringFile;

    TTicketParserAccessServiceMock accessServiceMock;
    std::unique_ptr<grpc::Server> AccessServer;
};

class TInsecureTestServer : public TTestServer<TKikimrWithGrpcAndRootSchema, false> {
    using TTestServer::TTestServer;
};
class TSecureTestServer : public TTestServer<TKikimrWithGrpcAndRootSchemaSecure, true> {
    using TTestServer::TTestServer;
};
