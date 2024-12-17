#include <yt/cpp/mapreduce/raw_client/raw_batch_request.h>

#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;
using namespace NYT::NDetail;
using namespace NYT::NDetail::NRawClient;


class TTestRetryPolicy
    : public IRequestRetryPolicy
{
private:
    static constexpr int RetriableCode = 904;

public:
    void NotifyNewAttempt() override
    { }

    TMaybe<TDuration> OnGenericError(const std::exception& /*e*/) override
    {
        return TDuration::Seconds(42);
    }

    void OnIgnoredError(const TErrorResponse& /*e*/) override
    { }

    TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override
    {
        if (e.GetError().GetCode() == RetriableCode) {
            return TDuration::Seconds(e.GetError().GetAttributes().at("retry_interval").AsUint64());
        } else {
            return Nothing();
        }
    }

    TString GetAttemptDescription() const override
    {
        return "attempt";
    }

    static TNode GenerateRetriableError(TDuration retryDuration)
    {
        Y_ABORT_UNLESS(retryDuration - TDuration::Seconds(retryDuration.Seconds()) == TDuration::Zero());

        return TNode()
            ("code", RetriableCode)
            ("attributes",
                TNode()
                    ("retry_interval", retryDuration.Seconds()));
    }
};


TString GetPathFromRequest(const TNode& params)
{
    return params.AsMap().at("parameters").AsMap().at("path").AsString();
}

TVector<TString> GetAllPathsFromRequestList(const TNode& requestList)
{
    TVector<TString> result;
    for (const auto& request : requestList.AsList()) {
        result.push_back(GetPathFromRequest(request)); }
    return result;
}


TEST(TBatchRequestImplTest, ParseResponse) {
    TClientContext context;
    TRawBatchRequest batchRequest(context.Config);

    EXPECT_EQ(batchRequest.BatchSize(), 0u);

    auto get1 = batchRequest.Get(
        TTransactionId(),
        "//getOk",
        TGetOptions());

    auto get2 = batchRequest.Get(
        TTransactionId(),
        "//getError-3",
        TGetOptions());

    auto get3 = batchRequest.Get(
        TTransactionId(),
        "//getError-5",
        TGetOptions());

    EXPECT_EQ(batchRequest.BatchSize(), 3u);

    auto testRetryPolicy = MakeIntrusive<TTestRetryPolicy>();
    const TInstant now = TInstant::Seconds(100500);

    TRawBatchRequest retryBatch(context.Config);
    batchRequest.ParseResponse(
        TNode()
            .Add(TNode()("output", 5))
            .Add(TNode()("error",
                    TTestRetryPolicy::GenerateRetriableError(TDuration::Seconds(3))))
            .Add(TNode()("error",
                    TTestRetryPolicy::GenerateRetriableError(TDuration::Seconds(5)))),
            "<no-request-id>",
            testRetryPolicy,
            &retryBatch,
            now);

    EXPECT_EQ(batchRequest.BatchSize(), 0u);
    EXPECT_EQ(retryBatch.BatchSize(), 2u);

    TNode retryParameterList;
    TInstant nextTry;
    retryBatch.FillParameterList(3, &retryParameterList, &nextTry);
    EXPECT_EQ(
        GetAllPathsFromRequestList(retryParameterList),
        TVector<TString>({"//getError-3", "//getError-5"}));

    EXPECT_EQ(nextTry, now + TDuration::Seconds(5));
}
