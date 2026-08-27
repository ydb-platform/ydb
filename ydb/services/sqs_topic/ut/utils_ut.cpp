#include <ydb/services/sqs_topic/utils.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>

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

    struct TEvMakeQueueUrlResult
        : public NActors::TEventLocal<TEvMakeQueueUrlResult, NActors::TEvents::ES_PRIVATE + 7422> {
        TString QueueUrl;
    };

    class TMakeQueueUrlTestActor : public NActors::TActorBootstrapped<TMakeQueueUrlTestActor> {
    public:
        TMakeQueueUrlTestActor(NActors::TActorId edge, ui16 httpProxyPort, bool httpProxySecure)
            : Edge_(edge)
            , HttpProxyPort_(httpProxyPort)
            , HttpProxySecure_(httpProxySecure)
        {
        }

        void Bootstrap(const NActors::TActorContext& ctx) {
            auto& httpProxyConfig = NKikimr::AppData(ctx)->HttpProxyConfig;
            httpProxyConfig.SetPort(HttpProxyPort_);
            httpProxyConfig.SetSecure(HttpProxySecure_);

            const TRichQueueUrl queueUrl{
                .Database = "/Root",
                .TopicPath = "topic",
                .Consumer = "consumer",
                .Fifo = false,
            };
            auto* ev = new TEvMakeQueueUrlResult;
            ev->QueueUrl = MakeQueueUrl(queueUrl, nullptr);
            ctx.Send(Edge_, ev);
            Die(ctx);
        }

    private:
        NActors::TActorId Edge_;
        ui16 HttpProxyPort_;
        bool HttpProxySecure_;
    };

    TString CollectMakeQueueUrlWithoutRequestMetadata(
        NKikimr::TTestActorRuntime& runtime,
        ui16 httpProxyPort,
        bool httpProxySecure)
    {
        const auto edge = runtime.AllocateEdgeActor();
        runtime.Register(
            new TMakeQueueUrlTestActor(edge, httpProxyPort, httpProxySecure),
            0,
            runtime.GetAppData().SystemPoolId);
        auto ev = runtime.GrabEdgeEvent<TEvMakeQueueUrlResult>(edge);
        return ev->Get()->QueueUrl;
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

Y_UNIT_TEST_SUITE(SqsTopicMetricsLabels) {
    Y_UNIT_TEST(ConvertOldConsumerNameForFirstClassCitizen) {
        NKikimr::TTestActorRuntime runtime(1, false);
        InitRuntime(runtime);

        const auto labels = CollectRequestMessageCountMetricsLabels(
            runtime,
            "ydb_sqs_consumer",
            true
        );

        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "consumer"), "ydb_sqs_consumer");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "name"), "api.sqs.request.message_count");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "method"), "SendMessage");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "topic"), "topic");
    }

    Y_UNIT_TEST(ConvertOldConsumerNameForSharedConsumerInFederation) {
        NKikimr::TTestActorRuntime runtime(1, false);
        InitRuntime(runtime);

        const auto labels = CollectRequestMessageCountMetricsLabels(
            runtime,
            "ydb_sqs_consumer",
            false
        );

        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "consumer"), "shared/ydb_sqs_consumer");
    }

    Y_UNIT_TEST(ConvertOldConsumerNameForNonSharedConsumerInFederation) {
        NKikimr::TTestActorRuntime runtime(1, false);
        InitRuntime(runtime);

        const auto labels = CollectRequestMessageCountMetricsLabels(
            runtime,
            "account@dir--topic",
            false
        );

        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "consumer"), "account/dir--topic");
    }
}

Y_UNIT_TEST_SUITE(SqsTopicMakeQueueUrl) {
    Y_UNIT_TEST(FallsBackToHttpProxyConfigFromAppDataWhenRequestMetadataMissing) {
        NKikimr::TTestActorRuntime runtime(1, false);
        InitRuntime(runtime);

        const TString url = CollectMakeQueueUrlWithoutRequestMetadata(runtime, 8443, true);
        UNIT_ASSERT_VALUES_EQUAL(
            url,
            TStringBuilder() << "https://" << FQDNHostName() << ":8443/v1/5//Root/5/topic/8/consumer");
    }

    Y_UNIT_TEST(FallsBackToHttpWhenHttpProxyConfigIsNotSecure) {
        NKikimr::TTestActorRuntime runtime(1, false);
        InitRuntime(runtime);

        const TString url = CollectMakeQueueUrlWithoutRequestMetadata(runtime, 2135, false);
        UNIT_ASSERT_VALUES_EQUAL(
            url,
            TStringBuilder() << "http://" << FQDNHostName() << ":2135/v1/5//Root/5/topic/8/consumer");
    }

    Y_UNIT_TEST(FallsBackWithoutPortWhenHttpProxyConfigPortIsZero) {
        NKikimr::TTestActorRuntime runtime(1, false);
        InitRuntime(runtime);

        const TString url = CollectMakeQueueUrlWithoutRequestMetadata(runtime, 0, true);
        UNIT_ASSERT_VALUES_EQUAL(
            url,
            TStringBuilder() << "https://" << FQDNHostName() << "/v1/5//Root/5/topic/8/consumer");
        UNIT_ASSERT(!url.Contains(":0"));
    }
}
