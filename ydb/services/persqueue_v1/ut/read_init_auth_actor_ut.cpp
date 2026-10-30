#include <ydb/services/persqueue_v1/actors/read_init_auth_actor.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcProxy::V1 {
    namespace {

        using namespace NActors;
        using TEvMetaCache = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache;

        class TStartAuthActor: public TActorBootstrapped<TStartAuthActor> {
        public:
            TStartAuthActor(TActorId parent, TActorId metaCache, NPersQueue::TTopicsToConverter topics)
                : Parent(parent)
                , MetaCache(metaCache)
                , Topics(std::move(topics))
            {
            }

            void Bootstrap(const TActorContext& ctx) {
                ctx.Register(new TReadInitAndAuthActor(ctx, Parent, "consumer", 0, "session",
                                                       MetaCache, MetaCache, nullptr, nullptr, Topics, "", true));
                Die(ctx);
            }

        private:
            const TActorId Parent;
            const TActorId MetaCache;
            const NPersQueue::TTopicsToConverter Topics;
        };

    } // namespace

    Y_UNIT_TEST_SUITE(ReadInitAndAuthActor) {
        Y_UNIT_TEST(CdcDiscoveryPathsRemainIsolatedAfterTermination) {
            TTestBasicRuntime runtime;
            TAppPrepare app;
            runtime.Initialize(app.Unwrap());
            const auto parent = runtime.AllocateEdgeActor();
            const auto metaCache = runtime.AllocateEdgeActor();

            const TString streamPath = "/Root/table/stream";
            const TString topicPath = "Root/table/stream/topic";
            NPersQueue::TTopicNamesConverterFactory factory(true, "/Root", "");
            const auto converter = factory.MakeDiscoveryConverter(streamPath, Nothing(), "", "/Root");
            UNIT_ASSERT_C(converter->IsValid(), converter->GetReason());
            NPersQueue::TTopicsToConverter topics;
            topics.Topics.emplace(streamPath, converter);
            runtime.Register(new TStartAuthActor(parent, metaCache, std::move(topics)));

            auto firstRequest = runtime.GrabEdgeEvent<TEvMetaCache::TEvDescribeTopicsRequest>(metaCache);
            const auto authActor = firstRequest->Sender;
            UNIT_ASSERT_VALUES_EQUAL(firstRequest->Get()->Topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(firstRequest->Get()->Topics.front()->GetPrimaryPath(), streamPath);

            auto result = std::make_shared<NSchemeCache::TSchemeCacheNavigate>();
            auto& entry = result->ResultSet.emplace_back();
            entry.Kind = NSchemeCache::TSchemeCacheNavigate::KindCdcStream;
            entry.Path = {"Root", "table", "stream"};
            auto children = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TListNodeEntry>();
            children->Children.emplace_back("topic", TPathId(), NSchemeCache::TSchemeCacheNavigate::KindTopic);
            entry.ListNodeEntry = children;
            runtime.Send(new IEventHandle(authActor, metaCache,
                                          new TEvMetaCache::TEvDescribeTopicsResponse(std::move(firstRequest->Get()->Topics), result)));

            auto secondRequest = runtime.GrabEdgeEvent<TEvMetaCache::TEvDescribeTopicsRequest>(metaCache);
            UNIT_ASSERT_VALUES_EQUAL(converter->GetPrimaryPath(), streamPath);
            UNIT_ASSERT_VALUES_EQUAL(converter->GetOriginalPath(), streamPath);
            UNIT_ASSERT_VALUES_EQUAL(secondRequest->Get()->Topics.size(), 1);
            const auto& topicConverter = secondRequest->Get()->Topics.front();
            UNIT_ASSERT_VALUES_EQUAL(topicConverter->GetPrimaryPath(), topicPath);
            UNIT_ASSERT_VALUES_EQUAL(topicConverter->GetOriginalPath(), streamPath);

            runtime.Send(new IEventHandle(authActor, parent, new TEvents::TEvPoisonPill()));
            runtime.Send(new IEventHandle(authActor, parent, new TEvents::TEvPing(), IEventHandle::FlagTrackDelivery));
            auto undelivered = runtime.GrabEdgeEvent<TEvents::TEvUndelivered>(parent);
            UNIT_ASSERT_VALUES_EQUAL(undelivered->Get()->SourceType, TEvents::TEvPing::EventType);
            UNIT_ASSERT_VALUES_EQUAL(undelivered->Get()->Reason, TEvents::TEvUndelivered::ReasonActorUnknown);

            UNIT_ASSERT_VALUES_EQUAL(topicConverter->GetPrimaryPath(), topicPath);
            UNIT_ASSERT_VALUES_EQUAL(topicConverter->GetOriginalPath(), streamPath);
            UNIT_ASSERT_VALUES_EQUAL(converter->GetPrimaryPath(), streamPath);
        }
    } // Y_UNIT_TEST_SUITE(ReadInitAndAuthActor)

} // namespace NKikimr::NGRpcProxy::V1
