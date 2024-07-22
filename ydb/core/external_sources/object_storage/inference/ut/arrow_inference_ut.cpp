#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/core/external_sources/object_storage/s3_fetcher.h>
#include <ydb/core/external_sources/object_storage/inference/arrow_fetcher.h>
#include <ydb/core/external_sources/object_storage/inference/arrow_inferencinator.h>
#include <ydb/core/util/testactorsys.h>
#include <ydb/library/yql/providers/common/http_gateway/mock/yql_http_mock_gateway.h>
#include <arrow/buffer.h>
#include <arrow/table.h>
#include <arrow/csv/api.h>

namespace {

using namespace NKikimr::NExternalSource::NObjectStorage;
using namespace NYql;

class TForwardingActor : public NActors::TActorBootstrapped<TForwardingActor> {
public:
    explicit TForwardingActor(NActors::TActorId forwardTo)
        : ForwardTo{forwardTo}
    {}

    void Bootstrap() {
        Become(&TForwardingActor::ForwardingState);
    }

    void ForwardingState(TAutoPtr<::NActors::IEventHandle>& ev) {
        ASSERT_TRUE(Forward(ev, ForwardTo));
    }

private:
    NActors::TActorId ForwardTo;
};

class ArrowInferenceTest : public testing::Test {
public:
    void SetUp() override {
        ActorSystem.Start();
        EdgeActorId = ActorSystem.AllocateEdgeActor(1);

        Gateway = IHTTPMockGateway::Make();
        ForwardingActor = new TForwardingActor(EdgeActorId);
        ActorSystem.Register(ForwardingActor, 1);

        S3ActorId = ActorSystem.Register(NKikimr::NExternalSource::NObjectStorage::CreateS3FetcherActor(
            BaseUrl,
            Gateway,
            NYql::IHTTPGateway::TRetryPolicy::GetNoRetryPolicy(),
            NYql::TS3Credentials{}), 1);
    }

    NActors::TActorId RegisterInferencinator(TStringBuf formatStr) {
        auto format = NInference::ConvertFileFormat(formatStr);
        auto arrowFetcher = ActorSystem.Register(NInference::CreateArrowFetchingActor(S3ActorId, format), 1);
        return ActorSystem.Register(NInference::CreateArrowInferencinator(arrowFetcher, format, {}), 1);
    }

    void TearDown() override {
        ActorSystem.Stop();
    }

protected:
    NKikimr::TTestActorSystem ActorSystem{1, NActors::NLog::PRI_DEBUG};
    NActors::TActorId EdgeActorId;

    IHTTPMockGateway::TPtr Gateway;
    TString BaseUrl = "not_a_real_url";
    TString Path = "/path/is/neither/real";
    TForwardingActor* ForwardingActor;

    NActors::TActorId S3ActorId;
};

TEST_F(ArrowInferenceTest, csv_simple) {
    TString s3Data = "A,B,C\n"
        "1,kek,2.3\n"
        "this part should not matter because it will be omitted as a partial row";

    Gateway->AddDefaultResponse([=, this](TString url, NYql::IHTTPGateway::THeaders, TString data) -> NYql::IHTTPGateway::TResult {
        EXPECT_EQ(url, BaseUrl + Path);
        EXPECT_EQ(data, "");

        NYql::IHTTPGateway::TResult result(NYql::IHTTPGateway::TContent(s3Data, 200));;
        return result;
    });

    auto inferencinatorId = RegisterInferencinator("csv_with_names");
    ActorSystem.WrapInActorContext(EdgeActorId, [this, inferencinatorId] {
        NActors::TActivationContext::AsActorContext().Send(inferencinatorId, new TEvInferFileSchema(TString{Path}));
    });

    std::unique_ptr<NActors::IEventHandle> event = ActorSystem.WaitForEdgeActorEvent({EdgeActorId});
    auto response = event->CastAsLocal<TEvInferredFileSchema>();
    ASSERT_NE(response, nullptr);

    auto& fields = response->Fields;
    ASSERT_TRUE(fields[0].type().has_type_id());
    ASSERT_EQ(response->Fields[0].type().type_id(), Ydb::Type::INT64);
    ASSERT_EQ(response->Fields[0].name(), "A");

    ASSERT_TRUE(fields[1].type().has_type_id());
    ASSERT_EQ(fields[1].type().type_id(), Ydb::Type::UTF8);
    ASSERT_EQ(fields[1].name(), "B");

    ASSERT_TRUE(fields[2].type().has_type_id());
    ASSERT_EQ(fields[2].type().type_id(), Ydb::Type::DOUBLE);
    ASSERT_EQ(fields[2].name(), "C");
}

TEST_F(ArrowInferenceTest, tsv_simple) {
    TString s3Data = "A\tB\tC\n"
        "1\tkek\t2.3\n"
        "this part should not matter because it will be omitted as a partial row,,";

    Gateway->AddDefaultResponse([=, this](TString url, NYql::IHTTPGateway::THeaders, TString data) -> NYql::IHTTPGateway::TResult {
        EXPECT_EQ(url, BaseUrl + Path);
        EXPECT_EQ(data, "");

        NYql::IHTTPGateway::TResult result(NYql::IHTTPGateway::TContent(s3Data, 200));;
        return result;
    });

    auto inferencinatorId = RegisterInferencinator("tsv_with_names");
    ActorSystem.WrapInActorContext(EdgeActorId, [this, inferencinatorId] {
        NActors::TActivationContext::AsActorContext().Send(inferencinatorId, new TEvInferFileSchema(TString{Path}));
    });

    std::unique_ptr<NActors::IEventHandle> event = ActorSystem.WaitForEdgeActorEvent({EdgeActorId});
    auto response = event->CastAsLocal<TEvInferredFileSchema>();
    ASSERT_NE(response, nullptr);

    auto& fields = response->Fields;
    ASSERT_TRUE(fields[0].type().has_type_id());
    ASSERT_EQ(response->Fields[0].type().type_id(), Ydb::Type::INT64);
    ASSERT_EQ(response->Fields[0].name(), "A");

    ASSERT_TRUE(fields[1].type().has_type_id());
    ASSERT_EQ(fields[1].type().type_id(), Ydb::Type::UTF8);
    ASSERT_EQ(fields[1].name(), "B");

    ASSERT_TRUE(fields[2].type().has_type_id());
    ASSERT_EQ(fields[2].type().type_id(), Ydb::Type::DOUBLE);
    ASSERT_EQ(fields[2].name(), "C");
}

} // namespace
