#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcService {

template <>
void FillYdbStatus(Ydb::Table::ReadTableResponse& response, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status) {
    response.set_status(status);
    NYql::IssuesToMessage(issues, response.mutable_issues());
}

namespace {

using TOperationRequest = TGrpcRequestOperationCall<Ydb::Scheme::MakeDirectoryRequest, Ydb::Scheme::MakeDirectoryResponse>;
using TStreamRequest = TGrpcRequestNoOperationCall<Ydb::Table::ReadTableRequest, Ydb::Table::ReadTableResponse>;

template <typename TRpc>
class TTestContext : public NLocalGrpc::TContextBase {
    using TResponse = typename TRpc::TResponse;
    using TBase = NLocalGrpc::TContextBase;
    using TLocalContext = NRpcService::TLocalRpcCtx<TRpc, std::function<void(const TResponse&)>>;
public:
    explicit TTestContext(bool stream = false)
        : TBase(std::make_shared<TLocalContext>(typename TRpc::TRequest{}, [](const TResponse&) {},
            typename TLocalContext::TSettings{}))
        , Stream(stream)
    {}

    const NProtoBuf::Message* GetRequest() const override {
        return &Request;
    }

    TString GetRpcMethodName() const override {
        return {};
    }

    bool IsStreamCall() const override {
        return Stream;
    }

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode) override {
    }

    void Reply(NProtoBuf::Message*, ui32) override {
    }

    void Reply(grpc::ByteBuffer*, ui32, NYdbGrpc::IRequestContextBase::EStreamCtrl) override {
    }

private:
    typename TRpc::TRequest Request;
    bool Stream;
};

using TBiStreamRequest = TGRpcRequestBiStreamWrapper<TRpcServices::EvGrpcRuntimeRequest,
    Ydb::Table::ReadTableRequest, Ydb::Table::ReadTableResponse>;

class TTestBiStreamContext : public TBiStreamRequest::IStreamCtx {
public:
    void Cancel() override {}
    void Attach(TActorId) override {}
    bool Read() override { return true; }
    bool Finish(const grpc::Status&) override { return true; }
    NYdbGrpc::TAuthState& GetAuthState() const override { return AuthState; }
    TString GetPeerName() const override { return "127.0.0.1"; }
    TVector<TStringBuf> GetPeerMetaValues(TStringBuf) const override { return {}; }
    grpc_compression_level GetCompressionLevel() const override { return GRPC_COMPRESS_LEVEL_NONE; }
    void UseDatabase(const TString&) override {}
    TString GetRpcMethodName() const override { return {}; }
    bool Write(Ydb::Table::ReadTableResponse&&, const grpc::WriteOptions& = {}) override { return true; }
    bool WriteAndFinish(Ydb::Table::ReadTableResponse&&, const grpc::Status&) override { return true; }
    bool WriteAndFinish(Ydb::Table::ReadTableResponse&&, const grpc::WriteOptions&, const grpc::Status&) override { return true; }

private:
    mutable NYdbGrpc::TAuthState AuthState{true};
};

struct TAuditCapture {
    TAuditLogParts Parts;
    ui32 Count = 0;

    template <typename TRequest>
    void Attach(TRequest& request) {
        request.SetAuditLogHook([this](ui32, const TAuditLogParts& parts) {
            Parts = parts;
            ++Count;
        });
    }

    TString Reason() const {
        TString reason;
        ui32 count = 0;
        for (const auto& [name, value] : Parts) {
            if (name == "reason") {
                reason = value;
                ++count;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(count, 1);
        return reason;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(GrpcCompletionAudit) {
    Y_UNIT_TEST(OperationResponsePreservesIssue) {
        auto ctx = MakeIntrusive<TTestContext<TOperationRequest>>();
        TOperationRequest request(ctx.Get(), [](std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&) {});
        TAuditCapture audit;
        audit.Attach(request);
        Ydb::Operations::Operation operation;
        operation.set_ready(true);
        operation.set_status(Ydb::StatusIds::SCHEME_ERROR);
        operation.add_issues()->set_message("Parent directory does not exist");
        request.SendOperation(operation);
        UNIT_ASSERT_VALUES_EQUAL(audit.Count, 1);
        UNIT_ASSERT_STRING_CONTAINS(audit.Reason(), "Parent directory does not exist");
    }

    Y_UNIT_TEST(IssueManagerPreservesIssue) {
        auto ctx = MakeIntrusive<TTestContext<TOperationRequest>>();
        TOperationRequest request(ctx.Get(), [](std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&) {});
        TAuditCapture audit;
        audit.Attach(request);
        request.RaiseIssue(NYql::TIssue("Database is unavailable"));
        request.ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(audit.Reason(), "Database is unavailable");
    }

    Y_UNIT_TEST(ErrorWithoutIssuesUsesStatus) {
        auto ctx = MakeIntrusive<TTestContext<TOperationRequest>>();
        TOperationRequest request(ctx.Get(), [](std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&) {});
        TAuditCapture audit;
        audit.Attach(request);
        request.ReplyWithYdbStatus(Ydb::StatusIds::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(audit.Reason(), "OVERLOADED");
    }

    Y_UNIT_TEST_TWIN(SerializedStreamPreservesIssueAndCompletesOnce, UseRope) {
        for (const auto flag : {IRequestCtx::EStreamCtrl::CONT, IRequestCtx::EStreamCtrl::FINISH}) {
            auto ctx = MakeIntrusive<TTestContext<TStreamRequest>>(true);
            TStreamRequest request(ctx.Get(), [](std::unique_ptr<IRequestNoOpCtx>, const IFacilityProvider&) {});
            TAuditCapture audit;
            audit.Attach(request);
            Ydb::Table::ReadTableResponse response;
            response.set_status(Ydb::StatusIds::SCHEME_ERROR);
            response.add_issues()->set_message("Table does not exist");
            TString serialized;
            UNIT_ASSERT(response.SerializeToString(&serialized));
            if (UseRope) {
                request.SendSerializedResult(TRope(std::move(serialized)), response.status(), flag);
            } else {
                request.SendSerializedResult(std::move(serialized), response.status(), flag);
            }
            request.FinishStream(response.status());
            UNIT_ASSERT_VALUES_EQUAL(audit.Count, 1);
            UNIT_ASSERT_STRING_CONTAINS(audit.Reason(), "Table does not exist");
        }
    }

    Y_UNIT_TEST(TypedStreamPreservesIssue) {
        auto ctx = MakeIntrusive<TTestContext<TStreamRequest>>(true);
        TStreamRequest request(ctx.Get(), [](std::unique_ptr<IRequestNoOpCtx>, const IFacilityProvider&) {});
        TAuditCapture audit;
        audit.Attach(request);
        Ydb::Table::ReadTableResponse response;
        response.set_status(Ydb::StatusIds::SCHEME_ERROR);
        response.add_issues()->set_message("Table does not exist");
        static_cast<IRequestCtx&>(request).Reply(&response, response.status());
        request.FinishStream(response.status());
        UNIT_ASSERT_VALUES_EQUAL(audit.Count, 1);
        UNIT_ASSERT_STRING_CONTAINS(audit.Reason(), "Table does not exist");
    }

    Y_UNIT_TEST(SerializedUnaryPreservesIssue) {
        auto ctx = MakeIntrusive<TTestContext<TOperationRequest>>();
        TOperationRequest request(ctx.Get(), [](std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&) {});
        TAuditCapture audit;
        audit.Attach(request);
        Ydb::Scheme::MakeDirectoryResponse response;
        auto* operation = response.mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SCHEME_ERROR);
        operation->add_issues()->set_message("Parent directory does not exist");
        TString serialized;
        UNIT_ASSERT(response.SerializeToString(&serialized));
        request.SendSerializedResult(std::move(serialized), operation->status());
        UNIT_ASSERT_VALUES_EQUAL(audit.Count, 1);
        UNIT_ASSERT_STRING_CONTAINS(audit.Reason(), "Parent directory does not exist");
    }

    Y_UNIT_TEST_TWIN(BiStreamPreservesIssueAndCompletesOnce, SeparateFinish) {
        auto ctx = MakeIntrusive<TTestBiStreamContext>();
        TBiStreamRequest request(ctx);
        TAuditCapture audit;
        audit.Attach(request);
        Ydb::Table::ReadTableResponse response;
        response.set_status(Ydb::StatusIds::SCHEME_ERROR);
        response.add_issues()->set_message("Table does not exist");
        if (SeparateFinish) {
            request.Write(std::move(response));
        } else {
            request.WriteAndFinish(std::move(response), Ydb::StatusIds::SCHEME_ERROR);
        }
        request.Finish(Ydb::StatusIds::SCHEME_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(audit.Count, 1);
        UNIT_ASSERT_STRING_CONTAINS(audit.Reason(), "Table does not exist");
    }

    Y_UNIT_TEST(ExistingReasonIsNotDuplicated) {
        auto ctx = MakeIntrusive<TTestContext<TOperationRequest>>();
        TOperationRequest request(ctx.Get(), [](std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&) {});
        TAuditCapture audit;
        audit.Attach(request);
        request.AddAuditLogPart("reason", "Access to database denied");
        request.ReplyWithYdbStatus(Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(audit.Reason(), "Access to database denied");
    }
}
} // namespace NKikimr::NGRpcService
