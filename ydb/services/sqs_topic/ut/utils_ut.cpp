#include <ydb/services/sqs_topic/utils.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NKikimr::NSqsTopic;

namespace {

    void InitRuntime(NKikimr::TTestActorRuntime& runtime) {
        runtime.Initialize({
            new NKikimr::TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr),
            nullptr,
            nullptr,
            {},
            {}
        });
    }

    TString GetLabelValue(
        const TVector<std::pair<TString, TString>>& labels,
        const TString& key
    ) {
        for (const auto& [labelKey, labelValue] : labels) {
            if (labelKey == key) {
                return labelValue;
            }
        }
        return {};
    }

    struct TEvMetricsLabelsResult
        : public NActors::TEventLocal<TEvMetricsLabelsResult, NActors::TEvents::ES_PRIVATE + 7421> {
        TVector<std::pair<TString, TString>> Labels;
    };

    class TMetricsLabelsTestActor : public NActors::TActorBootstrapped<TMetricsLabelsTestActor> {
    public:
        TMetricsLabelsTestActor(NActors::TActorId edge, TString consumer, bool firstClassCitizen)
            : Edge_(edge)
            , Consumer_(std::move(consumer))
            , FirstClassCitizen_(firstClassCitizen)
        {
        }

        void Bootstrap(const NActors::TActorContext& ctx) {
            NKikimr::AppData(ctx)->PQConfig.SetTopicsAreFirstClassCitizen(FirstClassCitizen_);

            auto* ev = new TEvMetricsLabelsResult;
            ev->Labels = GetRequestMessageCountMetricsLabels(
                "/Root/db",
                "/Root/db/topic",
                Consumer_,
                "SendMessage"
            );
            ctx.Send(Edge_, ev);
            Die(ctx);
        }

    private:
        NActors::TActorId Edge_;
        TString Consumer_;
        bool FirstClassCitizen_;
    };

    TVector<std::pair<TString, TString>> CollectRequestMessageCountMetricsLabels(
        NKikimr::TTestActorRuntime& runtime,
        const TString& consumer,
        bool firstClassCitizen
    ) {
        const auto edge = runtime.AllocateEdgeActor();
        runtime.Register(
            new TMetricsLabelsTestActor(edge, consumer, firstClassCitizen),
            0,
            runtime.GetAppData().SystemPoolId
        );
        auto ev = runtime.GrabEdgeEvent<TEvMetricsLabelsResult>(edge);
        return ev->Get()->Labels;
    }

} // namespace

TEST(SqsTopicMetricsLabels, ConvertOldConsumerNameForFirstClassCitizen) {
    NKikimr::TTestActorRuntime runtime(1, false);
    InitRuntime(runtime);

    const auto labels = CollectRequestMessageCountMetricsLabels(
        runtime,
        "ydb_sqs_consumer",
        true
    );

    EXPECT_EQ(GetLabelValue(labels, "consumer"), "ydb_sqs_consumer");
    EXPECT_EQ(GetLabelValue(labels, "name"), "api.sqs.request.message_count");
    EXPECT_EQ(GetLabelValue(labels, "method"), "SendMessage");
    EXPECT_EQ(GetLabelValue(labels, "topic"), "topic");
}

TEST(SqsTopicMetricsLabels, ConvertOldConsumerNameForSharedConsumerInFederation) {
    NKikimr::TTestActorRuntime runtime(1, false);
    InitRuntime(runtime);

    const auto labels = CollectRequestMessageCountMetricsLabels(
        runtime,
        "ydb_sqs_consumer",
        false
    );

    EXPECT_EQ(GetLabelValue(labels, "consumer"), "shared/ydb_sqs_consumer");
}

TEST(SqsTopicMetricsLabels, ConvertOldConsumerNameForNonSharedConsumerInFederation) {
    NKikimr::TTestActorRuntime runtime(1, false);
    InitRuntime(runtime);

    const auto labels = CollectRequestMessageCountMetricsLabels(
        runtime,
        "account@dir--topic",
        false
    );

    EXPECT_EQ(GetLabelValue(labels, "consumer"), "account/dir--topic");
}
