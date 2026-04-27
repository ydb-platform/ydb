#include "actors.h"
#include "common.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NGRpcProxy::V1::NPQv1 {

using namespace NYdb::NTopic::NTests;

std::shared_ptr<TTopicSdkTestSetup> CreateSetup() {
    auto setup = std::make_shared<TTopicSdkTestSetup>("PQv1");
    setup->GetServer().EnableLogs({
            NKikimrServices::PQ_MLP_READER,
            NKikimrServices::PQ_MLP_WRITER,
            NKikimrServices::PQ_MLP_COMMITTER,
            NKikimrServices::PQ_MLP_UNLOCKER,
            NKikimrServices::PQ_MLP_DEADLINER,
            NKikimrServices::PQ_MLP_PURGER,
            NKikimrServices::PQ_MLP_CONSUMER,
            NKikimrServices::PQ_MLP_ENRICHER,
            NKikimrServices::PQ_MLP_DLQ_MOVER,
            NKikimrServices::PQ_MLP_DESCRIBER,
        },
        NActors::NLog::PRI_DEBUG
    );
    setup->GetServer().EnableLogs({
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PERSQUEUE_READ_BALANCER,
            NKikimrServices::PQ_WRITE_PROXY
        },
        NActors::NLog::PRI_INFO
    );

    setup->GetRuntime().GetAppData().PQConfig.SetBalancerWakeupIntervalSec(1);
    setup->GetRuntime().GetAppData().PQConfig.SetBalancerStatsWakeupIntervalSec(1);

    return setup;
}

template<typename TRes>
struct TResultHolder {
    NYql::TIssue Issue;
    std::optional<Ydb::StatusIds::StatusCode> ResultStatus;
    TRes Result;
};

template<typename TReq, typename TRes>
class TRequestCtx: public NKikimr::NGRpcService::IRequestOpCtx {
    public:
        using TRequest = TRequestCtx;
    
        TRequestCtx(
                const TReq& request,
                TString topicPath,
                TString databaseName,
                std::shared_ptr<TResultHolder<TRes>> resultHolder
            )
            : Request(request)
            , TopicPath(topicPath)
            , DatabaseName(databaseName)
            , ResultHolder(resultHolder)
        {
        };
    
        const TString path() const {
            return TopicPath;
        }
    
        TMaybe<TString> GetTraceId() const override {
            return Nothing();
        }
    
        const TMaybe<TString> GetDatabaseName() const override {
            return DatabaseName;
        }
    
        TString GetRpcMethodName() const override {
            // We have no grpc method, but the closest analog is protobuf name
            if (const NProtoBuf::Message* req = GetRequest()) {
                return req->GetDescriptor()->name();
            }
            return {};
        }
    
        const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
            return UserToken;
        }
    
        const TString& GetSerializedToken() const override {
            static TString emptyString;
            return UserToken == nullptr ? emptyString : UserToken->GetSerializedToken();
        }
    
        bool IsClientLost() const override {
            return false;
        };
    
        const google::protobuf::Message* GetRequest() const override {
            return &Request;
        };
    
        const TMaybe<TString> GetRequestType() const override {
            return Nothing();
        };
    
        void SetFinishAction(std::function<void()>&& cb) override {
            Y_UNUSED(cb);
        };
    
        google::protobuf::Arena* GetArena() override {
            return nullptr;
        };
    
        bool HasClientCapability(const TString& capability) const override {
            Y_UNUSED(capability);
            return false;
        };
    
        void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
            ProcessYdbStatusCode(status, google::protobuf::Empty{});
        };
    
        void ReplyWithRpcStatus(grpc::StatusCode code, const TString& msg = "", const TString& details = "") override {
            Y_UNUSED(code);
            Y_UNUSED(msg);
            Y_UNUSED(details);
        }
    
        TString GetPeerName() const override {
            return "";
        }
    
        TInstant GetDeadline() const override {
            return TInstant();
        }
    
        const TMaybe<TString> GetPeerMetaValues(const TString&) const override {
            return Nothing();
        }
    
        TVector<TStringBuf> FindClientCert() const override {
            return TVector<TStringBuf>();
        }
    
        TMaybe<NKikimr::NRpcService::TRlPath> GetRlPath() const override {
            return Nothing();
        }
    
        void RaiseIssue(const NYql::TIssue& issue) override {
            ResultHolder->Issue = issue;
        }
    
        void RaiseIssues(const NYql::TIssues& issues) override {
            Y_UNUSED(issues);
        };
    
        const TString& GetRequestName() const override {
            return DummyString;
        };
    
        bool GetDiskQuotaExceeded() const override {
            return false;
        };
    
        void AddAuditLogPart(const TStringBuf& name, const TString& value) override {
            Y_UNUSED(name);
            Y_UNUSED(value);
        };
    
        const NKikimr::NGRpcService::TAuditLogParts& GetAuditLogParts() const override {
            return DummyAuditLogParts;
        };
    
        void SetRuHeader(ui64 ru) override {
            Y_UNUSED(ru);
        };
    
        void AddServerHint(const TString& hint) override {
            Y_UNUSED(hint);
        };
    
        void SetCostInfo(float consumed_units) override {
            Y_UNUSED(consumed_units);
        };
    
        void SetStreamingNotify(NYdbGrpc::IRequestContextBase::TOnNextReply&& cb) override {
            Y_UNUSED(cb);
        };
    
        void FinishStream(ui32 status) override {
            Y_UNUSED(status);
        };
    
        void SendSerializedResult(TString&& in, Ydb::StatusIds::StatusCode status, EStreamCtrl flag = EStreamCtrl::CONT)
                override {
            Y_UNUSED(flag);
            Y_UNUSED(in);
            Y_UNUSED(status);
        };
    
        void Reply(NProtoBuf::Message* resp, ui32 status = 0) override {
            Y_UNUSED(resp);
            Y_UNUSED(status);
        };
    
        void SendOperation(const Ydb::Operations::Operation& operation) override {
            Y_UNUSED(operation);
        };
    
        NWilson::TTraceId GetWilsonTraceId() const override {
            return {};
        }
    
        void SendResult(const google::protobuf::Message& result, Ydb::StatusIds::StatusCode status) override {
            ProcessYdbStatusCode(status, result);
        };
    
        void SendResult(
                const google::protobuf::Message& result,
                Ydb::StatusIds::StatusCode status,
                const google::protobuf::RepeatedPtrField<NKikimr::NGRpcService::TYdbIssueMessageType>& message) override {
    
            Y_UNUSED(message);
            ProcessYdbStatusCode(status, result);
        };
    
        const Ydb::Operations::OperationParams& operation_params() const {
            return DummyParams;
        }
    
        static TRequestCtx* GetProtoRequest(std::shared_ptr<IRequestOpCtx> request) {
            return static_cast<TRequestCtx*>(request.get());
        }
    
    protected:
        void FinishRequest() override {
        };
    
    private:
        const TReq Request;
        const Ydb::Operations::OperationParams DummyParams;
        const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        const TString DummyString;
        const NKikimr::NGRpcService::TAuditLogParts DummyAuditLogParts;
        const TString TopicPath;
        const TString DatabaseName;

        std::shared_ptr<TResultHolder<TRes>> ResultHolder;

        void ProcessYdbStatusCode(Ydb::StatusIds::StatusCode& status, const google::protobuf::Message& result) {
            ResultHolder->ResultStatus = status;

            if (status == Ydb::StatusIds::SUCCESS) {
                Cerr << (TStringBuilder() << "RESULT:" << result.DebugString() << Endl);
                auto r = dynamic_cast<const TRes*>(&result);
                UNIT_ASSERT(r);

                ResultHolder->Result = *r;
            } else {
                Cerr << (TStringBuilder() << "RESULT:" << status << " " << ResultHolder->Issue.GetMessage() << Endl);
            }
        }
    };
    

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(CreateTopic) {

Y_UNIT_TEST(SharedConsumer) {

    auto setup = CreateSetup();
    auto& runtime = setup->GetRuntime();
    runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(false);


    Ydb::PersQueue::V1::CreateTopicRequest request;
    auto& settings = *request.mutable_settings();
    settings.set_partitions_count(1);
    settings.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    settings.set_retention_period_ms(TDuration::Days(1).MilliSeconds());

    settings.mutable_attributes()->insert({"_federation_account", "account1"});

    auto& readRule = *settings.add_read_rules();

    readRule.set_consumer_name("test_consumer");
    readRule.set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
    readRule.set_version(1);
    //readRule.set_service_type("test_service_type");
    readRule.set_starting_message_timestamp_ms(1000);

    auto& type = *readRule.mutable_shared_read_rule_type();
    type.set_keep_messages_order(true);
    type.mutable_default_processing_timeout()->set_seconds(3);
    type.mutable_receive_message_wait_time()->set_seconds(5);
    type.mutable_receive_message_delay()->set_seconds(7);
    type.mutable_dead_letter_policy()->set_enabled(true);
    type.mutable_dead_letter_policy()->mutable_condition()->set_max_processing_attempts(11);
    type.mutable_dead_letter_policy()->mutable_move_action()->set_dead_letter_queue("test_dead_letter_queue");

    const TString localCluster = "dc-sas";
    const TString database = "/Root/test_db";
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    NKikimrSchemeOp::TPersQueueGroupDescription targetConfig;

    auto result = std::make_shared<TResultHolder<Ydb::PersQueue::V1::CreateTopicResponse>>();

    auto ctx = new TRequestCtx<Ydb::PersQueue::V1::CreateTopicRequest, Ydb::PersQueue::V1::CreateTopicResponse>(request, "/Root/test_db", database, result);
    runtime.Register(CreateCreateTopicActor(ctx));

    for (int i = 0; i < 10; ++i) {
        auto status = result->ResultStatus;
        if (status) {
            break;
        }

        Sleep(TDuration::MilliSeconds(50));
    }

    auto status = result->ResultStatus;
    UNIT_ASSERT(status);
    UNIT_ASSERT_EQUAL_C(*status, Ydb::StatusIds::SUCCESS, result->Issue.GetMessage());

    // UNIT_ASSERT_EQUAL_C(result.GetStatus(), Ydb::StatusIds::SUCCESS, result.GetErrorMessage());
}

};

} // namespace NKikimr::NGRpcProxy::V1::NPQv1
