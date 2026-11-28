#include <ydb/core/fq/libs/events/events.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_applicator_actor.h>
#include <ydb/library/yql/providers/s3/proto/credentials.pb.h>
#include <ydb/library/yql/providers/s3/proto/sink.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

namespace NYql::NDq {
namespace {

const TString CompleteXml = "<CompleteMultipartUploadResult><ETag>test-etag</ETag></CompleteMultipartUploadResult>";
const TString NoSuchUploadXml = "<Error><Code>NoSuchUpload</Code><Message>Upload does not exist</Message></Error>";

class TCompletionGateway final: public IHTTPGateway {
public:
    std::function<TResult()> Response;
    size_t Uploads = 0;
    bool RetriedRewind = false;
    bool Retried503 = true;
    bool UsedPut = false;

    void Upload(TString, THeaders, TString, TOnResult callback, bool put, TRetryPolicy::TPtr policy, IHttpRequestContext::TPtr) override {
        ++Uploads;
        UsedPut |= put;
        RetriedRewind |= policy->CreateRetryState()->GetNextRetryDelay(CURLE_SEND_FAIL_REWIND, 0).Defined();
        Retried503 &= policy->CreateRetryState()->GetNextRetryDelay(CURLE_OK, 503).Defined();
        callback(Response());
    }

    void Delete(TString, THeaders, TOnResult, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("Unexpected DELETE");
    }

    void Download(TString, THeaders, size_t, size_t, TOnResult, TString, TRetryPolicy::TPtr, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("Unexpected GET");
    }

    TCancelHook Download(TString, THeaders, size_t, size_t, TOnDownloadStart, TOnNewDataPart, TOnDownloadFinish,
        const NMonitoring::TDynamicCounters::TCounterPtr&, IHttpRequestContext::TPtr) override {
        UNIT_FAIL("Unexpected streaming GET");
        return {};
    }

    ui64 GetBuffersSizePerStream() override {
        return 0;
    }

    void UpdatePoolCaps(THashMap<TWorkScope, size_t>) override {
    }
};

struct TCompletionFixture: NUnitTest::TBaseFixture {
    NActors::TTestActorRuntimeBase Runtime;
    std::shared_ptr<TCompletionGateway> Gateway = std::make_shared<TCompletionGateway>();
    NActors::TActorId Parent;

    TCompletionFixture() {
        Runtime.Initialize();
        Runtime.GetLogSettings(0)->Append(
            NKikimrServices::EServiceKikimr_MIN, NKikimrServices::EServiceKikimr_MAX, NKikimrServices::EServiceKikimr_Name);
        Parent = Runtime.AllocateEdgeActor();
    }

    NFq::TEvents::TEvEffectApplicationResult::TPtr Run() {
        NS3::TAwsParams aws;
        aws.SetAwsAccessKey("test-access-key");
        aws.SetAwsRegion("us-east-1");
        const auto token = TStructuredTokenBuilder().SetBasicAuth(aws.SerializeAsString(), "test-secret").ToJson();
        NS3::TEffect sink;
        sink.SetToken("s3");
        sink.MutableCommit()->SetUrl("http://s3.invalid/bucket/object");
        sink.MutableCommit()->SetUploadId("test-upload");
        sink.MutableCommit()->AddETag("test-etag");
        NDqProto::TExternalEffect effect;
        effect.AddEffects()->SetData(sink.SerializeAsString());
        Runtime.RunCall([&] {
            return Runtime.Register(
                MakeS3ApplicatorActor(Parent, Gateway, "test-query", "test-job", 0, true, { { "s3", token } }, nullptr, effect).Release());
        });
        auto response = Runtime.GrabEdgeEvent<NFq::TEvents::TEvEffectApplicationResult>(Parent, TDuration::Seconds(2));
        UNIT_ASSERT(response);
        UNIT_ASSERT(!Gateway->UsedPut);
        // Check the policy actually passed to the inner gateway retry layer.
        UNIT_ASSERT(!Gateway->RetriedRewind);
        UNIT_ASSERT(Gateway->Retried503);
        return response;
    }

    void CheckLostResponse(bool uploadCompleted) {
        bool objectExists = false;
        Gateway->Response = [&] {
            if (Gateway->Uploads == 1) {
                // The server either completed the upload or aborted it, but the
                // response was lost. Both outcomes would yield 404 on replay.
                objectExists = uploadCompleted;
                return IHTTPGateway::TResult(CURLE_SEND_FAIL_REWIND, TIssues{ TIssue("Lost completion response") });
            }
            return IHTTPGateway::TResult(IHTTPGateway::TContent(NoSuchUploadXml, 404));
        };
        auto response = Run();
        UNIT_ASSERT_VALUES_EQUAL(objectExists, uploadCompleted);
        UNIT_ASSERT(response->Get()->FatalError);
        UNIT_ASSERT(!response->Get()->Issues.Empty());
        // Also check the outer applicator retry layer, not only gateway policy.
        UNIT_ASSERT_VALUES_EQUAL(Gateway->Uploads, 1);
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TS3ApplicatorRetryTest) {
    Y_UNIT_TEST_F(AbortedUploadWithLostResponseFails, TCompletionFixture) {
        CheckLostResponse(false);
    }

    Y_UNIT_TEST_F(CompletedUploadWithLostResponseRemainsUncertain, TCompletionFixture) {
        CheckLostResponse(true);
    }

    Y_UNIT_TEST_F(TransientHttpFailureIsStillRetried, TCompletionFixture) {
        Gateway->Response = [&] {
            return IHTTPGateway::TResult(
                IHTTPGateway::TContent(Gateway->Uploads == 1 ? "<Error><Code>SlowDown</Code><Message>Please retry</Message></Error>"
                                                             : CompleteXml, Gateway->Uploads == 1 ? 503 : 200));
        };
        auto response = Run();
        UNIT_ASSERT(!response->Get()->FatalError);
        UNIT_ASSERT(response->Get()->Issues.Empty());
        UNIT_ASSERT_VALUES_EQUAL(Gateway->Uploads, 2);
    }
}   // Y_UNIT_TEST_SUITE(TS3ApplicatorRetryTest)

}   // namespace NYql::NDq
