#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/audit_helpers/audit_helper.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/ymq/actor/actor.h>
#include <ydb/core/ymq/actor/auth_multi_factory.h>
#include <ydb/core/ymq/actor/events.h>
#include <ydb/core/ymq/actor/serviceid.h>
#include <ydb/services/ymq/rpc_params.h>
#include <ydb/services/ymq/ymq_proxy.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NSQS {
namespace {

const TString RawToken = "raw-token-with-secret-signature";
const TString SanitizedToken = "token-without-signature";

class TNoopReplyCallback final : public IReplyCallback {
    void DoSendReply(const NKikimrClient::TSqsResponse&) override {}
};

class TAuditLogRuntime {
public:
    explicit TAuditLogRuntime(const TString& sanitizedToken)
        : Token_(MakeIntrusive<NACLib::TUserToken>("audit-user", TVector<TString>{}))
    {
        Token_->SetSanitizedToken(sanitizedToken);
        Runtime_.AuditLogBackends = Tests::CreateTestAuditLogBackends(AuditLines_);
        Runtime_.Initialize(TAppPrepare().Unwrap());
        Runtime_.GetAppData().SqsConfig.SetRequestTimeoutMs(0);
        EdgeActor_ = Runtime_.AllocateEdgeActor();
        Runtime_.RegisterService(MakeSqsServiceID(Runtime_.GetNodeId()), EdgeActor_);
        Runtime_.RegisterService(MakeTicketParserID(), EdgeActor_);
        Runtime_.RegisterService(MakeSqsFolderServiceID(), EdgeActor_);
        Runtime_.RegisterService(MakeTxProxyID(), EdgeActor_);
        Runtime_.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) { return Observe(ev); });
    }

    std::string RunLocal(const TString& ticket) {
        NKikimrClient::TSqsRequest request;
        request.SetRequestId("local-audit-token");
        auto* createUser = request.MutableCreateUser();
        createUser->MutableAuth()->SetUserName("audit-account");
        createUser->MutableAuth()->SetMaskedToken("masked-token");
        createUser->MutableCredentials()->SetOAuthToken(ticket);
        // An empty user name is rejected after authorization.
        Runtime_.Register(CreateActionActor(request, MakeHolder<TNoopReplyCallback>()));
        return WaitForAuditLine();
    }

    std::string RunCloud(TAuthActorData::ESqsRequestFormat format = TAuthActorData::Xml) {
        auto& config = Runtime_.GetAppData().SqsConfig;
        config.SetYandexCloudMode(true);
        config.SetYandexCloudFolderServiceAddress("folder-service");
        TCloudAuthCounters counters(config, MakeIntrusive<::NMonitoring::TDynamicCounters>());

        auto request = MakeHolder<NKikimrClient::TSqsRequest>();
        request->SetRequestId("cloud-audit-token");
        // An empty queue name is rejected by the action actor after authorization.
        request->MutableCreateQueue();
        TAuthActorData data{
            .SQSRequest = std::move(request),
            .HTTPCallback = MakeHolder<TNoopReplyCallback>(),
            .UserSidCallback = [](TString) {},
            .EnableQueueLeader = false,
            .Action = EAction::CreateQueue,
            .ExecutorPoolID = 0,
            .CloudID = "cloud",
            .Counters = &counters,
            .IAMToken = RawToken,
            .FolderID = "folder",
        };
        if (format == TAuthActorData::Json) {
            Runtime_.Register(new THttpProxyAuthRequestProxy(std::move(data), {}, EdgeActor_));
            auto response = Runtime_.GrabEdgeEvent<NHttpProxy::TEvYmqCloudAuthResponse>(EdgeActor_);
            UNIT_ASSERT(response->Get()->IsSuccess);
            Ydb::Ymq::V1::CreateQueueRequest request;
            NRpcService::DoLocalRpc<NGRpcService::TEvYmqCreateQueueRequest>(
                std::move(request), {}, Nothing(), Nothing(), Runtime_.GetActorSystem(0), {
                    {NYmq::V1::CLOUD_ID, response->Get()->CloudId},
                    {NYmq::V1::FOLDER_ID, response->Get()->FolderId},
                    {NYmq::V1::USER_SID, response->Get()->Sid},
                    {NYmq::V1::SANITIZED_TOKEN, response->Get()->SanitizedToken},
                    {NYmq::V1::REQUEST_ID, "json-audit-token"},
                });
        } else {
            Runtime_.Register(new TCloudAuthRequestProxy(std::move(data), {}));
        }
        return WaitForAuditLine();
    }

private:
    NActors::TTestActorRuntimeBase::EEventAction Observe(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            case TSqsEvents::EvGetConfiguration: {
                auto response = MakeHolder<TSqsEvents::TEvConfiguration>();
                response->UserExists = true;
                response->SchemeCache = EdgeActor_;
                Runtime_.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release()));
                return NActors::TTestActorRuntimeBase::EEventAction::DROP;
            }
            case TEvTicketParser::EvAuthorizeTicket: {
                const auto* request = ev->Get<TEvTicketParser::TEvAuthorizeTicket>();
                AuthorizedTicket_ = request->Ticket;
                Runtime_.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new TEvTicketParser::TEvAuthorizeTicketResult(request->Ticket, Token_)));
                return NActors::TTestActorRuntimeBase::EEventAction::DROP;
            }
            case TEvTxProxySchemeCache::EvNavigateKeySet: {
                auto* request = ev->Get<TEvTxProxySchemeCache::TEvNavigateKeySet>();
                for (auto& entry : request->Request->ResultSet) {
                    entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
                }
                Runtime_.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new TEvTxProxySchemeCache::TEvNavigateKeySetResult(request->Request.Release())));
                return NActors::TTestActorRuntimeBase::EEventAction::DROP;
            }
            case TEvTxUserProxy::EvProposeTransaction: {
                const auto* request = ev->Get<TEvTxUserProxy::TEvProposeTransaction>();
                if (request->Record.GetTransaction().GetModifyScheme().GetOperationType() != NKikimrSchemeOp::ESchemeOpMkDir) {
                    return NActors::TTestActorRuntimeBase::EEventAction::PROCESS;
                }
                // Queue creation first ensures that the cloud account directory exists.
                auto response = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(
                    TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAlready);
                response->Record.SetSchemeShardStatus(NKikimrScheme::StatusAlreadyExists);
                Runtime_.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release()));
                return NActors::TTestActorRuntimeBase::EEventAction::DROP;
            }
            case NFolderService::TEvFolderService::EvGetCloudByFolderRequest: {
                auto response = MakeHolder<NFolderService::TEvFolderService::TEvGetCloudByFolderResponse>();
                response->FolderId = "folder";
                response->CloudId = "cloud";
                Runtime_.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release()));
                return NActors::TTestActorRuntimeBase::EEventAction::DROP;
            }
            default:
                return NActors::TTestActorRuntimeBase::EEventAction::PROCESS;
        }
    }

    std::string WaitForAuditLine() {
        Runtime_.WaitFor("YMQ audit record", [&] {
            return AnyOf(AuditLines_, [](const auto& line) { return line.find("component=ymq") != std::string::npos; });
        }, TDuration::Seconds(10));
        const auto line = Tests::FindAuditLine(AuditLines_, "component=ymq");
        UNIT_ASSERT_STRING_CONTAINS(line, "status=ERROR");
        UNIT_ASSERT_C(line.find(RawToken.c_str()) == std::string::npos, line);
        if (AuthorizedTicket_) {
            UNIT_ASSERT_VALUES_EQUAL(*AuthorizedTicket_, RawToken);
        }
        return line;
    }

private:
    std::vector<std::string> AuditLines_;
    NActors::TTestBasicRuntime Runtime_;
    TActorId EdgeActor_;
    TIntrusivePtr<NACLib::TUserToken> Token_;
    TMaybe<TString> AuthorizedTicket_;
};

} // namespace

Y_UNIT_TEST_SUITE(YmqAuditLog) {
    Y_UNIT_TEST(LocalAuthorizationPreservesSanitizedToken) {
        TAuditLogRuntime runtime(SanitizedToken);
        const auto line = runtime.RunLocal(RawToken);
        UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "sanitized_token=" << SanitizedToken);
    }

    Y_UNIT_TEST(LocalTokenWithoutSanitizedValueIsWrittenAsNone) {
        TAuditLogRuntime runtime({});
        const auto line = runtime.RunLocal(RawToken);
        UNIT_ASSERT_STRING_CONTAINS(line, "sanitized_token={none}");
    }

    Y_UNIT_TEST(MissingTokenIsWrittenAsNone) {
        TAuditLogRuntime runtime({});
        const auto line = runtime.RunLocal({});
        UNIT_ASSERT_STRING_CONTAINS(line, "sanitized_token={none}");
    }

    Y_UNIT_TEST(CloudAuthorizationPreservesSanitizedToken) {
        TAuditLogRuntime runtime(SanitizedToken);
        const auto line = runtime.RunCloud();
        UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "sanitized_token=" << SanitizedToken);
    }

    Y_UNIT_TEST(CloudTokenWithoutSanitizedValueIsWrittenAsNone) {
        TAuditLogRuntime runtime({});
        const auto line = runtime.RunCloud();
        UNIT_ASSERT_STRING_CONTAINS(line, "sanitized_token={none}");
    }

    Y_UNIT_TEST(CloudJsonAuthorizationPreservesSanitizedToken) {
        TAuditLogRuntime runtime(SanitizedToken);
        const auto line = runtime.RunCloud(TAuthActorData::Json);
        UNIT_ASSERT_STRING_CONTAINS(line, TStringBuilder() << "sanitized_token=" << SanitizedToken);
    }

    Y_UNIT_TEST(CloudJsonTokenWithoutSanitizedValueIsWrittenAsNone) {
        TAuditLogRuntime runtime({});
        const auto line = runtime.RunCloud(TAuthActorData::Json);
        UNIT_ASSERT_STRING_CONTAINS(line, "sanitized_token={none}");
    }
}

} // namespace NKikimr::NSQS
