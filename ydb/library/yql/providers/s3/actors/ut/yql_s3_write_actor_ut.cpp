#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_write_actor.h>
#include <ydb/library/yql/providers/s3/actors_factory/yql_s3_actors_factory.h>
#include <ydb/library/yql/providers/s3/proto/credentials.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

#include <mutex>
#include <vector>

namespace NYql::NDq {
namespace {

class TWriteGateway final: public IHTTPGateway {
public:
    struct TRequest {
        TString Url;
        TString Method;
        TRetryPolicy::TPtr Policy;
    };

    bool FailPart = false;
    NThreading::TPromise<void> Initiated = NThreading::NewPromise();

    std::vector<TRequest> GetRequests() const {
        const std::lock_guard lock(Mutex);
        return Requests;
    }

    void Upload(TString url, THeaders, TString, TOnResult callback, bool put, TRetryPolicy::TPtr policy, IHttpRequestContext::TPtr) override {
        Record(url, put ? "PUT" : "POST", std::move(policy));
        if (url.EndsWith("?uploads")) {
            callback(
                TResult(TContent("<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                                 "<UploadId>test-upload</UploadId></InitiateMultipartUploadResult>", 200)));
            Initiated.SetValue();
        } else if (put && url.Contains("partNumber=") && !FailPart) {
            callback(TResult(TContent("", 200, "HTTP/1.1 200 OK\r\nETag: test-etag\r\n\r\n")));
        } else {
            // Finish at the single PUT or multipart completion with an ambiguous
            // response, so the test can inspect the policy passed to the gateway.
            callback(TResult(CURLE_SEND_FAIL_REWIND, TIssues{ TIssue("Lost upload response") }));
        }
    }

    void Delete(TString url, THeaders, TOnResult callback, TRetryPolicy::TPtr policy, IHttpRequestContext::TPtr) override {
        Record(url, "DELETE", std::move(policy));
        callback(TResult(TContent("", 204)));
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

private:
    void Record(const TString& url, const TString& method, TRetryPolicy::TPtr policy) {
        const std::lock_guard lock(Mutex);
        Requests.push_back({ url, method, std::move(policy) });
    }

    mutable std::mutex Mutex;
    std::vector<TRequest> Requests;
};

std::vector<TWriteGateway::TRequest> Write(bool multipart, const TS3WriteRetryPolicies& policies, bool failPart = false) {
    auto gateway = std::make_shared<TWriteGateway>();
    gateway->FailPart = failPart;
    TFakeCASetup setup;
    setup.Execute([&](TFakeActor& actor) {
        NS3::TAwsParams aws;
        aws.SetAwsAccessKey("test-access-key");
        aws.SetAwsRegion("us-east-1");
        const auto token = TStructuredTokenBuilder().SetBasicAuth(aws.SerializeAsString(), "test-secret").ToJson();
        NS3::TSink sink;
        sink.SetUrl("http://s3.invalid/bucket/");
        sink.SetToken("s3");
        sink.SetMultipart(multipart);
        sink.SetAtomicUploadCommit(false);
        auto [output, writer] = CreateS3WriteActor(actor.TypeEnv, *actor.FunctionRegistry, nullptr, gateway, std::move(sink), 0,
            TCollectStatsLevel::None, TTxId{}, "", { { "s3", token } }, &actor.GetAsyncOutputCallbacks(), nullptr, policies);
        actor.InitAsyncOutput(output, writer);
    });
    auto error = setup.AsyncOutputPromises->Issue.GetFuture();
    setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory&) {
        NKikimr::NMiniKQL::TUnboxedValueBatch batch;
        batch.emplace_back(NKikimr::NMiniKQL::MakeString("payload"));
        return batch;
    }, Nothing(), !multipart);
    if (multipart) {
        UNIT_ASSERT(gateway->Initiated.GetFuture().Wait(TDuration::Seconds(10)));
        setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory&) {
            return NKikimr::NMiniKQL::TUnboxedValueBatch{};
        }, Nothing(), true);
    }
    UNIT_ASSERT(error.Wait(TDuration::Seconds(10)));
    UNIT_ASSERT(!error.GetValue().Empty());
    setup.Terminate();
    return gateway->GetRequests();
}

TS3WriteRetryPolicies FqWritePolicies() {
    return {
        .Default = GetFqHTTPRetryPolicy(),
        .DataUpload = GetFqHTTPRetryPolicy(ERewindRetry::Enabled),
    };
}

}   // namespace

Y_UNIT_TEST_SUITE(TS3WriteRetryTest) {
    Y_UNIT_TEST(SinglePartUsesDataUploadPolicy) {
        const auto policies = FqWritePolicies();
        const auto requests = Write(false, policies);
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Method, "PUT");
        UNIT_ASSERT(requests[0].Policy == policies.DataUpload);
        UNIT_ASSERT(requests[0].Policy->CreateRetryState()->GetNextRetryDelay(CURLE_SEND_FAIL_REWIND, 0).Defined());
    }

    Y_UNIT_TEST(MultipartUsesSeparatePolicies) {
        const auto policies = FqWritePolicies();
        const auto requests = Write(true, policies);
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Method, "POST");
        UNIT_ASSERT_VALUES_EQUAL(requests[1].Method, "PUT");
        UNIT_ASSERT_VALUES_EQUAL(requests[2].Method, "POST");
        UNIT_ASSERT(requests[0].Policy == policies.Default);
        UNIT_ASSERT(requests[1].Policy == policies.DataUpload);
        UNIT_ASSERT(requests[2].Policy == policies.Default);
        UNIT_ASSERT(requests[1].Policy->CreateRetryState()->GetNextRetryDelay(CURLE_SEND_FAIL_REWIND, 0).Defined());
        UNIT_ASSERT(!requests[2].Policy->CreateRetryState()->GetNextRetryDelay(CURLE_SEND_FAIL_REWIND, 0).Defined());
        UNIT_ASSERT(requests[2].Policy->CreateRetryState()->GetNextRetryDelay(CURLE_OK, 503).Defined());
    }

    Y_UNIT_TEST(AbortUsesDefaultPolicy) {
        const auto policies = FqWritePolicies();
        const auto requests = Write(true, policies, true);
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(requests.back().Method, "DELETE");
        UNIT_ASSERT(requests.back().Policy == policies.Default);
    }

    Y_UNIT_TEST(CustomPoliciesArePreserved) {
        const TS3WriteRetryPolicies policies{
            .Default = GetHTTPDefaultRetryPolicy(THttpRetryPolicyOptions{ .MaxRetries = 1 }),
            .DataUpload = IHTTPGateway::TRetryPolicy::GetNoRetryPolicy(),
        };
        const auto requests = Write(true, policies);
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 3);
        UNIT_ASSERT(requests[0].Policy == policies.Default);
        UNIT_ASSERT(requests[1].Policy == policies.DataUpload);
        UNIT_ASSERT(requests[2].Policy == policies.Default);
        UNIT_ASSERT(!requests[1].Policy->CreateRetryState()->GetNextRetryDelay(CURLE_RECV_ERROR, 0).Defined());
        auto state = requests[2].Policy->CreateRetryState();
        UNIT_ASSERT(state->GetNextRetryDelay(CURLE_OK, 503).Defined());
        UNIT_ASSERT(!state->GetNextRetryDelay(CURLE_OK, 503).Defined());
    }

    Y_UNIT_TEST(NullPoliciesArePreserved) {
        for (bool multipart : { false, true }) {
            const auto requests = Write(multipart, {});
            UNIT_ASSERT_VALUES_EQUAL(requests.size(), multipart ? 3 : 1);
            for (const auto& request : requests) {
                UNIT_ASSERT(!request.Policy);
            }
        }
    }
}

}   // namespace NYql::NDq
