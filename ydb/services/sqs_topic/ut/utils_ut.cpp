#include <ydb/services/sqs_topic/billing.h>
#include <ydb/services/sqs_topic/statuses.h>
#include <ydb/services/sqs_topic/utils.h>
#include <ydb/services/sqs_topic/queue_url/utils.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
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
        TMetricsLabelsTestActor(
            NActors::TActorId edge,
            TString consumer,
            bool firstClassCitizen,
            TString databaseId = {},
            TString cloudId = {},
            TString folderId = {}
        )
            : Edge_(edge)
            , Consumer_(std::move(consumer))
            , FirstClassCitizen_(firstClassCitizen)
            , DatabaseId_(std::move(databaseId))
            , CloudId_(std::move(cloudId))
            , FolderId_(std::move(folderId))
        {
        }

        void Bootstrap(const NActors::TActorContext& ctx) {
            NKikimr::AppData(ctx)->PQConfig.SetTopicsAreFirstClassCitizen(FirstClassCitizen_);

            auto* ev = new TEvMetricsLabelsResult;
            if (DatabaseId_ || CloudId_ || FolderId_) {
                ev->Labels = GetMetricsLabels(
                    "/Root/db",
                    "/Root/db/topic",
                    Consumer_,
                    "SendMessage",
                    {{"name", "api.sqs.request.count"}},
                    DatabaseId_,
                    CloudId_,
                    FolderId_
                );
            } else {
                ev->Labels = GetRequestMessageCountMetricsLabels(
                    "/Root/db",
                    "/Root/db/topic",
                    Consumer_,
                    "SendMessage"
                );
            }
            ctx.Send(Edge_, ev);
            Die(ctx);
        }

    private:
        NActors::TActorId Edge_;
        TString Consumer_;
        bool FirstClassCitizen_;
        TString DatabaseId_;
        TString CloudId_;
        TString FolderId_;
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

    TVector<std::pair<TString, TString>> CollectMetricsLabelsWithIdentity(
        NKikimr::TTestActorRuntime& runtime,
        const TString& databaseId,
        const TString& cloudId = {},
        const TString& folderId = {}
    ) {
        const auto edge = runtime.AllocateEdgeActor();
        runtime.Register(
            new TMetricsLabelsTestActor(edge, "ydb_sqs_consumer", true, databaseId, cloudId, folderId),
            0,
            runtime.GetAppData().SystemPoolId
        );
        auto ev = runtime.GrabEdgeEvent<TEvMetricsLabelsResult>(edge);
        return ev->Get()->Labels;
    }

    bool HasLabel(
        const TVector<std::pair<TString, TString>>& labels,
        const TString& key
    ) {
        for (const auto& [labelKey, _] : labels) {
            if (labelKey == key) {
                return true;
            }
        }
        return false;
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
        UNIT_ASSERT(HasLabel(labels, "database_id"));
        UNIT_ASSERT(!HasLabel(labels, "cloud_id"));
        UNIT_ASSERT(!HasLabel(labels, "folder_id"));
    }

    Y_UNIT_TEST(IncludesDatabaseIdLabel) {
        NKikimr::TTestActorRuntime runtime(1, false);
        InitRuntime(runtime);

        const auto labels = CollectMetricsLabelsWithIdentity(runtime, "database4");

        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "database_id"), "database4");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "database"), "/Root/db");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "name"), "api.sqs.request.count");
        UNIT_ASSERT(!HasLabel(labels, "cloud_id"));
        UNIT_ASSERT(!HasLabel(labels, "folder_id"));
    }

    Y_UNIT_TEST(OmitsCloudIdAndFolderIdWhenEmpty) {
        NKikimr::TTestActorRuntime runtime(1, false);
        InitRuntime(runtime);

        const auto labels = CollectMetricsLabelsWithIdentity(runtime, "database4", "", "");

        UNIT_ASSERT(!HasLabel(labels, "cloud_id"));
        UNIT_ASSERT(!HasLabel(labels, "folder_id"));
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "database_id"), "database4");
    }

    Y_UNIT_TEST(IncludesCloudIdAndFolderIdLabels) {
        NKikimr::TTestActorRuntime runtime(1, false);
        InitRuntime(runtime);

        const auto labels = CollectMetricsLabelsWithIdentity(runtime, "database4", "cloud4", "folder4");

        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "database_id"), "database4");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "cloud_id"), "cloud4");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "folder_id"), "folder4");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "database"), "/Root/db");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(labels, "name"), "api.sqs.request.count");
    }

    Y_UNIT_TEST(IncludesBothCloudAndFolderWhenOnlyOneIsSet) {
        NKikimr::TTestActorRuntime runtime(1, false);
        InitRuntime(runtime);

        const auto cloudOnly = CollectMetricsLabelsWithIdentity(runtime, "database4", "cloud4", "");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(cloudOnly, "cloud_id"), "cloud4");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(cloudOnly, "folder_id"), "");

        const auto folderOnly = CollectMetricsLabelsWithIdentity(runtime, "database4", "", "folder4");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(folderOnly, "cloud_id"), "");
        UNIT_ASSERT_VALUES_EQUAL(GetLabelValue(folderOnly, "folder_id"), "folder4");
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

Y_UNIT_TEST_SUITE(SqsTopicBilling) {
    Y_UNIT_TEST(DefaultRequestCostIsTwoRu) {
        using namespace NKikimr::NSqsTopic::V1::NBilling;

        UNIT_ASSERT_VALUES_EQUAL(RoundRu(DEFAULT_REQUEST_COST), 2);
        UNIT_ASSERT_VALUES_EQUAL(RoundRu(WRITE_BASE_COST), RoundRu(DEFAULT_REQUEST_COST));
        UNIT_ASSERT_VALUES_EQUAL(RoundRu(READ_BASE_COST), RoundRu(DEFAULT_REQUEST_COST));
        UNIT_ASSERT_VALUES_EQUAL(RoundRu(DELETE_BASE_COST), RoundRu(DEFAULT_REQUEST_COST));
    }

    Y_UNIT_TEST(CalcRuAddsFifoAdjunct) {
        using namespace NKikimr::NSqsTopic::V1::NBilling;

        UNIT_ASSERT_VALUES_EQUAL(CalcRu(0, WRITE_BASE_COST, WRITE_COST_PER_BLOCK, false), 2);
        UNIT_ASSERT_VALUES_EQUAL(CalcRu(0, WRITE_BASE_COST, WRITE_COST_PER_BLOCK, true), 3);
        UNIT_ASSERT_VALUES_EQUAL(CalcRu(5, WRITE_BASE_COST, WRITE_COST_PER_BLOCK, false), 7);
        UNIT_ASSERT_VALUES_EQUAL(CalcRu(5, WRITE_BASE_COST, WRITE_COST_PER_BLOCK, true), 8);
    }

    Y_UNIT_TEST(PayloadBlocksMatchesOneShotCalculator) {
        using namespace NKikimr::NSqsTopic::V1::NBilling;

        UNIT_ASSERT_VALUES_EQUAL(PayloadBlocks(0, WRITE_BLOCK_SIZE), 0);
        UNIT_ASSERT_VALUES_EQUAL(PayloadBlocks(WRITE_BLOCK_SIZE, WRITE_BLOCK_SIZE), 0);
        UNIT_ASSERT_VALUES_EQUAL(PayloadBlocks(3 * READ_BLOCK_SIZE, WRITE_BLOCK_SIZE), 5);
        UNIT_ASSERT_VALUES_EQUAL(
            CalcRu(PayloadBlocks(3 * READ_BLOCK_SIZE, WRITE_BLOCK_SIZE), WRITE_BASE_COST, WRITE_COST_PER_BLOCK, false),
            7);
    }
}

Y_UNIT_TEST_SUITE(SqsTopicDescribeStatus) {
    Y_UNIT_TEST(MapTopicInfoCreateVsSendPolicies) {
        using namespace NKikimr::NSqsTopic::V1;
        using NKikimr::NPQ::NDescriber::TTopicInfo;
        using NKikimr::NPQ::NDescriber::EStatus;

        TTopicInfo notTopic;
        notTopic.Status = EStatus::NotTopic;
        {
            auto error = MapTopicInfoToSqsError("/Root/q", notTopic, ExistingQueuePolicy());
            UNIT_ASSERT(error.Defined());
            UNIT_ASSERT_VALUES_EQUAL(error->GetErrorCode(), "AWS.SimpleQueueService.NonExistentQueue");
            UNIT_ASSERT_VALUES_EQUAL(error->GetMessage(), QUEUE_USED_BY_ANOTHER_SCHEME_OBJECT);
        }
        {
            auto error = MapTopicInfoToSqsError("/Root/q", notTopic, CreateQueueDescribePolicy());
            UNIT_ASSERT(error.Defined());
            UNIT_ASSERT_VALUES_EQUAL(error->GetErrorCode(), "InvalidParameterValue");
            UNIT_ASSERT_VALUES_EQUAL(error->GetMessage(), QUEUE_USED_BY_ANOTHER_SCHEME_OBJECT);
        }

        TTopicInfo missing;
        missing.Status = EStatus::NotFound;
        UNIT_ASSERT(MapTopicInfoToSqsError("/Root/q", missing, ExistingQueuePolicy()).Defined());
        UNIT_ASSERT(!MapTopicInfoToSqsError("/Root/q", missing, CreateQueueDescribePolicy()).Defined());

        TTopicInfo cdc;
        cdc.Status = EStatus::Success;
        cdc.CdcStream = true;
        cdc.Info = new NKikimr::NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo();
        {
            auto error = MapTopicInfoToSqsError(
                "/Root/q", cdc, ExistingQueuePolicy(TString("Writing to the Changefeed is not supported")));
            UNIT_ASSERT(error.Defined());
            UNIT_ASSERT_VALUES_EQUAL(error->GetErrorCode(), "AWS.SimpleQueueService.UnsupportedOperation");
        }
        UNIT_ASSERT(!MapTopicInfoToSqsError("/Root/q", cdc, ExistingQueuePolicy()).Defined());
    }

    Y_UNIT_TEST(UnauthorizedHidesExistenceAndDescribeAccessIsDenied) {
        using namespace NKikimr::NSqsTopic::V1;
        using NKikimr::NPQ::NDescriber::TTopicInfo;
        using NKikimr::NPQ::NDescriber::EStatus;

        TTopicInfo unauthorized;
        unauthorized.Status = EStatus::Unauthorized;
        for (const auto& policy : {
                 ExistingQueuePolicy(),
                 CreateQueueDescribePolicy(),
                 DeleteQueueDescribePolicy(),
                 SetQueueAttributesDescribePolicy(),
                 GetQueueAttributesDescribePolicy(),
             })
        {
            auto error = MapTopicInfoToSqsError("/Root/q", unauthorized, policy);
            UNIT_ASSERT(error.Defined());
            UNIT_ASSERT_VALUES_EQUAL(error->GetErrorCode(), "AWS.SimpleQueueService.NonExistentQueue");
            UNIT_ASSERT_VALUES_EQUAL(error->GetMessage(), SPECIFIED_QUEUE_DOES_NOT_EXIST);
        }

        TTopicInfo describeDenied;
        describeDenied.Status = EStatus::UnauthorizedWithDescribeAccess;
        {
            auto error = MapTopicInfoToSqsError("/Root/q", describeDenied, ExistingQueuePolicy());
            UNIT_ASSERT(error.Defined());
            UNIT_ASSERT_VALUES_EQUAL(error->GetErrorCode(), "AccessDeniedException");
            UNIT_ASSERT_VALUES_EQUAL(error->GetMessage(), "Access denied");
        }
    }
}
