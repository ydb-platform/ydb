#include <ydb/services/sqs_topic/utils.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NKikimr::NSqsTopic;

namespace {

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
        NKikimr::TTestActorSystem& actorSystem,
        const TString& consumer,
        bool firstClassCitizen
    ) {
        const auto edge = actorSystem.AllocateEdgeActor(1);
        actorSystem.Register(
            new TMetricsLabelsTestActor(edge, consumer, firstClassCitizen),
            1,
            NKikimr::TTestActorSystem::SYSTEM_POOL_ID
        );
        auto ev = actorSystem.WaitForEdgeActorEvent<TEvMetricsLabelsResult>(edge);
        return ev->Get()->Labels;
    }

} // namespace

TEST(SqsTopicMetricsLabels, ConvertOldConsumerNameForFirstClassCitizen) {
    NKikimr::TTestActorSystem actorSystem(1);
    actorSystem.Start();

    const auto labels = CollectRequestMessageCountMetricsLabels(
        actorSystem,
        "ydb_sqs_consumer",
        true
    );

    EXPECT_EQ(GetLabelValue(labels, "consumer"), "ydb_sqs_consumer");
    EXPECT_EQ(GetLabelValue(labels, "name"), "api.sqs.request.message_count");
    EXPECT_EQ(GetLabelValue(labels, "method"), "SendMessage");
    EXPECT_EQ(GetLabelValue(labels, "topic"), "topic");

    actorSystem.Stop();
}

TEST(SqsTopicMetricsLabels, ConvertOldConsumerNameForSharedConsumerInFederation) {
    NKikimr::TTestActorSystem actorSystem(1);
    actorSystem.Start();

    const auto labels = CollectRequestMessageCountMetricsLabels(
        actorSystem,
        "ydb_sqs_consumer",
        false
    );

    EXPECT_EQ(GetLabelValue(labels, "consumer"), "shared/ydb_sqs_consumer");

    actorSystem.Stop();
}

TEST(SqsTopicMetricsLabels, ConvertOldConsumerNameForNonSharedConsumerInFederation) {
    NKikimr::TTestActorSystem actorSystem(1);
    actorSystem.Start();

    const auto labels = CollectRequestMessageCountMetricsLabels(
        actorSystem,
        "account@dir--topic",
        false
    );

    EXPECT_EQ(GetLabelValue(labels, "consumer"), "account/dir--topic");

    actorSystem.Stop();
}
