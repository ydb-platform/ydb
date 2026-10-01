#include "ydb_common_ut.h"
#include "ydb_secret.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/public/api/grpc/ydb_secret_v1.grpc.pb.h>

#include <google/protobuf/text_format.h>
#include <grpcpp/create_channel.h>

namespace NKikimr {

using namespace Tests;

namespace {

// Runs scheme operations on secrets: a delegation secret is created by KQP and has no statement here
class TSecretSchemeClient : public TClient {
public:
    using TClient::TClient;

    void RunSecretOperation(NKikimrSchemeOp::EOperationType type, const TString& description) {
        TAutoPtr<NMsgBusProxy::TBusSchemeOperation> request = new NMsgBusProxy::TBusSchemeOperation();
        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetWorkingDir("/Root");
        tx.SetOperationType(type);
        auto& secret = type == NKikimrSchemeOp::ESchemeOpCreateSecret ? *tx.MutableCreateSecret() : *tx.MutableAlterSecret();
        UNIT_ASSERT_C(google::protobuf::TextFormat::ParseFromString(description, &secret), description);
        TAutoPtr<NBus::TBusMessage> reply;
        UNIT_ASSERT_VALUES_EQUAL(SendAndWaitCompletion(request, reply), NBus::MESSAGE_OK);
        const auto& response = dynamic_cast<NMsgBusProxy::TBusResponse*>(reply.Get())->Record;
        UNIT_ASSERT_VALUES_EQUAL_C(response.GetStatus(), NMsgBusProxy::MSTATUS_OK, response.GetErrorReason());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TGRpcSecretService) {
    Y_UNIT_TEST(DescribeSecretShowsTheIamDelegation) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableIamDelegationSecrets(true);
        NYdb::TKikimrWithGrpcAndRootSchema server(appConfig, {}, {}, false, nullptr, [](TServerSettings& settings) {
            settings.RegisterGrpcService<NGRpcService::TGRpcYdbSecretService>("secret", std::nullopt, false);
        });
        TSecretSchemeClient client(*server.ServerSettings);
        client.RunSecretOperation(NKikimrSchemeOp::ESchemeOpCreateSecret, R"(Name: "plain" Value: "v")");
        client.RunSecretOperation(NKikimrSchemeOp::ESchemeOpCreateSecret,
            R"(Name: "delegated" IamDelegation { ServiceAccountId: "aje-sa-1" CloudId: "b1g-cloud-1" ReferrerId: "referrer-1" })");
        client.RunSecretOperation(NKikimrSchemeOp::ESchemeOpAlterSecret,
            R"(Name: "delegated" IamDelegationAlter: IAM_DELEGATION_ALTER_STAGE IamDelegation { ServiceAccountId: "aje-sa-2" CloudId: "b1g-cloud-1" ReferrerId: "referrer-2" })");

        const auto channel = grpc::CreateChannel("localhost:" + ToString(server.GetPort()), grpc::InsecureChannelCredentials());
        const auto stub = Ydb::Secret::V1::SecretService::NewStub(channel);
        const auto describe = [&](const TString& path) {
            grpc::ClientContext context;
            Ydb::Secret::DescribeSecretRequest request;
            request.set_path(path);
            Ydb::Secret::DescribeSecretResponse response;
            const auto status = stub->DescribeSecret(&context, request, &response);
            UNIT_ASSERT_C(status.ok(), status.error_message());
            UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.operation().DebugString());
            Ydb::Secret::DescribeSecretResult result;
            UNIT_ASSERT(response.operation().result().UnpackTo(&result));
            return result;
        };

        const auto plain = describe("/Root/plain");
        UNIT_ASSERT_VALUES_EQUAL(plain.self().name(), "plain");
        UNIT_ASSERT_VALUES_EQUAL(plain.version(), 0);
        UNIT_ASSERT(!plain.has_iam_delegation());
        UNIT_ASSERT(!plain.has_pending_iam_delegation());
        UNIT_ASSERT(!plain.DebugString().Contains("\"v\"")); // the value is never described

        const auto delegated = describe("/Root/delegated");
        UNIT_ASSERT_VALUES_EQUAL(delegated.self().name(), "delegated");
        UNIT_ASSERT_VALUES_EQUAL(delegated.version(), 1);
        UNIT_ASSERT_VALUES_EQUAL(delegated.iam_delegation().service_account_id(), "aje-sa-1");
        UNIT_ASSERT_VALUES_EQUAL(delegated.iam_delegation().cloud_id(), "b1g-cloud-1");
        UNIT_ASSERT_VALUES_EQUAL(delegated.iam_delegation().referrer_id(), "referrer-1");
        UNIT_ASSERT_VALUES_EQUAL(delegated.pending_iam_delegation().service_account_id(), "aje-sa-2");
        UNIT_ASSERT_VALUES_EQUAL(delegated.pending_iam_delegation().referrer_id(), "referrer-2");
    }
}

} // namespace NKikimr
