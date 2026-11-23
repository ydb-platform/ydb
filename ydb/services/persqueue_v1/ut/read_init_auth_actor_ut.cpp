#include <ydb/services/persqueue_v1/actors/events.h>
#include <ydb/services/persqueue_v1/actors/read_init_auth_actor.h>

#include <ydb/core/base/path.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <functional>

namespace NKikimr::NGRpcProxy::V1 {
    namespace {

        using namespace NActors;
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;
        using TFiller = std::function<void(TNavigate& request, TNavigate::TEntry& entry)>;

        const TString LbRoot = "/Root/LbCommunal";

        class TFakeSchemeCache: public TActorBootstrapped<TFakeSchemeCache> {
        public:
            TFakeSchemeCache(TFiller filler, TVector<TString>* requested)
                : Filler(std::move(filler))
                , Requested(requested)
            {
            }

            void Bootstrap() {
                Become(&TFakeSchemeCache::StateWork);
            }

            STRICT_STFUNC(StateWork,
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);
            )

        private:
            void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
                auto request = std::move(ev->Get()->Request);
                for (auto& entry : request->ResultSet) {
                    Requested->push_back(CanonizePath(JoinPath(entry.Path)));
                    Filler(*request, entry);
                }
                Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
            }

            TFiller Filler;
            TVector<TString>* Requested;
        };

        class TStartAuthActor: public TActorBootstrapped<TStartAuthActor> {
        public:
            TStartAuthActor(TActorId parent, NPersQueue::TTopicsToConverter topics)
                : Parent(parent)
                , Topics(std::move(topics))
            {
            }

            void Bootstrap(const TActorContext& ctx) {
                ctx.Register(new TReadInitAndAuthActor(ctx, Parent, "consumer", 0, "session",
                                                       "/Root", MakeSchemeCacheID(), nullptr, nullptr, Topics, "", true));
                Die(ctx);
            }

        private:
            const TActorId Parent;
            const NPersQueue::TTopicsToConverter Topics;
        };

        TString EntryPath(const TNavigate::TEntry& entry) {
            return CanonizePath(JoinPath(entry.Path));
        }

        void FillTopic(TNavigate::TEntry& entry, ui64 balancerTabletId,
                       const std::function<void(NKikimrPQ::TPQTabletConfig&)>& fillConfig) {
            entry.Status = TNavigate::EStatus::Ok;
            entry.Kind = TNavigate::EKind::KindTopic;
            auto pqInfo = MakeIntrusive<TNavigate::TPQGroupInfo>();
            pqInfo->Description.SetBalancerTabletID(balancerTabletId);
            auto* partition = pqInfo->Description.AddPartitions();
            partition->SetPartitionId(0);
            partition->SetTabletId(balancerTabletId + 1);
            fillConfig(*pqInfo->Description.MutablePQTabletConfig());
            entry.PQGroupInfo = pqInfo;
            entry.CreateStep = 1;
        }

        void FillFederationTopic(TNavigate::TEntry& entry, ui64 balancerTabletId, const TString& name, const TString& dc) {
            FillTopic(entry, balancerTabletId, [&](NKikimrPQ::TPQTabletConfig& config) {
                config.SetTopicName(name);
                config.SetTopicPath(EntryPath(entry));
                config.SetYdbDatabasePath(LbRoot + "/account");
                config.SetFederationAccount("account");
                config.SetDC(dc);
            });
        }

        void SetNotFound(TNavigate::TEntry& entry) {
            entry.Status = TNavigate::EStatus::PathErrorUnknown;
        }

        struct TEnv {
            TTestBasicRuntime Runtime;
            TActorId Parent;
            TVector<TString> Requested;

            TEnv(bool firstClass, TFiller filler)
                : Runtime(1, false)
            {
                TAppPrepare app;
                auto& pqConfig = app.PQConfig;
                pqConfig.SetTopicsAreFirstClassCitizen(firstClass);
                pqConfig.SetRoot(firstClass ? "/Root" : "/Root/PQ");
                if (!firstClass) {
                    pqConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(LbRoot);
                }
                Runtime.Initialize(app.Unwrap());
                Runtime.SetLogPriority(NKikimrServices::PQ_READ_PROXY, NLog::PRI_DEBUG);
                Runtime.SetLogPriority(NKikimrServices::PQ_DESCRIBER, NLog::PRI_DEBUG);

                const auto schemeCache = Runtime.Register(new TFakeSchemeCache(std::move(filler), &Requested));
                Runtime.EnableScheduleForActor(schemeCache);
                Runtime.RegisterService(MakeSchemeCacheID(), schemeCache);
                Parent = Runtime.AllocateEdgeActor();
            }

            void Start(NPersQueue::TTopicsToConverter topics) {
                Runtime.Register(new TStartAuthActor(Parent, std::move(topics)));
            }

            TEvPQProxy::TEvAuthResultOk* WaitAuthOk() {
                TAutoPtr<IEventHandle> handle;
                auto [ok, close] = Runtime.GrabEdgeEvents<TEvPQProxy::TEvAuthResultOk, TEvPQProxy::TEvCloseSession>(handle);
                UNIT_ASSERT_C(!close, "session closed: " << close->Reason);
                Handle = handle;
                return ok;
            }

            TEvPQProxy::TEvCloseSession* WaitClose() {
                TAutoPtr<IEventHandle> handle;
                auto [ok, close] = Runtime.GrabEdgeEvents<TEvPQProxy::TEvAuthResultOk, TEvPQProxy::TEvCloseSession>(handle);
                UNIT_ASSERT_C(!ok, "auth unexpectedly succeeded");
                Handle = handle;
                return close;
            }

        private:
            TAutoPtr<IEventHandle> Handle;
        };

        NPersQueue::TTopicsToConverter MakeTopics(const TVector<NPersQueue::TDiscoveryConverterPtr>& converters) {
            NPersQueue::TTopicsToConverter topics;
            for (const auto& converter : converters) {
                UNIT_ASSERT_C(converter->IsValid(), converter->GetReason());
                topics.Topics.emplace(converter->GetOriginalPath(), converter);
            }
            return topics;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(ReadInitAndAuthActor) {
        Y_UNIT_TEST(CdcDiscoveryPathsRemainIsolatedAfterTermination) {
            const TString streamPath = "/Root/table/stream";
            const TString streamImplPath = streamPath + "/streamImpl";
            TEnv env(true, [&](TNavigate&, TNavigate::TEntry& entry) {
                if (EntryPath(entry) == streamPath) {
                    entry.Status = TNavigate::EStatus::Ok;
                    entry.Kind = TNavigate::EKind::KindCdcStream;
                    auto self = MakeIntrusive<TNavigate::TDirEntryInfo>();
                    self->Info.SetName("stream");
                    entry.Self = self;
                    return;
                }
                UNIT_ASSERT_VALUES_EQUAL(EntryPath(entry), streamImplPath);
                FillTopic(entry, 10, [](NKikimrPQ::TPQTabletConfig& config) {
                    config.SetYdbDatabasePath("/Root");
                });
            });

            NPersQueue::TTopicNamesConverterFactory factory(true, "/Root", "");
            const auto converter = factory.MakeDiscoveryConverter(streamPath, Nothing(), "", "/Root");
            env.Start(MakeTopics({converter}));

            auto* result = env.WaitAuthOk();
            UNIT_ASSERT_VALUES_EQUAL(env.Requested, (TVector<TString>{streamPath, streamImplPath}));
            UNIT_ASSERT_VALUES_EQUAL(result->TopicAndTablets.size(), 1);
            const auto& info = result->TopicAndTablets.at(streamPath);
            UNIT_ASSERT_VALUES_EQUAL(info.TabletID, 10);
            UNIT_ASSERT(info.TopicNameConverter);

            UNIT_ASSERT_VALUES_EQUAL(converter->GetPrimaryPath(), streamPath);
            UNIT_ASSERT_VALUES_EQUAL(converter->GetOriginalPath(), streamPath);
        }

        Y_UNIT_TEST(LegacyMissFallsBackToModernPath) {
            const TString modernPath = LbRoot + "/account/topic";
            TEnv env(false, [&](TNavigate& request, TNavigate::TEntry& entry) {
                if (EntryPath(entry) != modernPath) {
                    return SetNotFound(entry);
                }
                UNIT_ASSERT_VALUES_EQUAL(request.DatabaseName, LbRoot + "/account");
                FillFederationTopic(entry, 20, "topic", "dc1");
            });

            NPersQueue::TTopicNamesConverterFactory factory(false, "/Root/PQ", "dc1");
            const auto converter = factory.MakeDiscoveryConverter("account/topic", {}, "dc1", "/Root");
            const TString printable = converter->GetPrintableString();
            env.Start(MakeTopics({converter}));

            auto* result = env.WaitAuthOk();
            UNIT_ASSERT_VALUES_EQUAL(env.Requested.front(), "/Root/PQ/rt3.dc1--account--topic");
            UNIT_ASSERT_VALUES_EQUAL(env.Requested.back(), modernPath);
            const auto& info = result->TopicAndTablets.at(converter->GetOriginalPath());
            UNIT_ASSERT_VALUES_EQUAL(info.TabletID, 20);
            UNIT_ASSERT(info.TopicNameConverter);
            UNIT_ASSERT_VALUES_EQUAL(converter->GetPrintableString(), printable);
        }

        Y_UNIT_TEST(RemoteDcFallsBackToMirroredPath) {
            const TString mirroredPath = LbRoot + "/account/topic-mirrored-from-dc2";
            TEnv env(false, [&](TNavigate&, TNavigate::TEntry& entry) {
                if (EntryPath(entry) != mirroredPath) {
                    return SetNotFound(entry);
                }
                FillFederationTopic(entry, 30, "topic-mirrored-from-dc2", "dc2");
            });

            NPersQueue::TTopicNamesConverterFactory factory(false, "/Root/PQ", "dc1");
            const auto converter = factory.MakeDiscoveryConverter("account/topic", {}, "dc2", "/Root");
            env.Start(MakeTopics({converter}));

            auto* result = env.WaitAuthOk();
            UNIT_ASSERT(Find(env.Requested, mirroredPath) != env.Requested.end());
            UNIT_ASSERT(Find(env.Requested, LbRoot + "/account/topic") == env.Requested.end());
            UNIT_ASSERT_VALUES_EQUAL(result->TopicAndTablets.at(converter->GetOriginalPath()).TabletID, 30);
        }

        Y_UNIT_TEST(BothPathsMissingReportsModernPath) {
            TEnv env(false, [](TNavigate&, TNavigate::TEntry& entry) {
                SetNotFound(entry);
            });

            NPersQueue::TTopicNamesConverterFactory factory(false, "/Root/PQ", "dc1");
            env.Start(MakeTopics({factory.MakeDiscoveryConverter("account/topic", {}, "dc1", "/Root")}));

            auto* close = env.WaitClose();
            UNIT_ASSERT_VALUES_EQUAL(PersQueue::ErrorCode::ErrorCode_Name(close->ErrorCode), "UNKNOWN_TOPIC");
            UNIT_ASSERT_STRING_CONTAINS(close->Reason, "Discovery for all topics failed");
            UNIT_ASSERT_STRING_CONTAINS(close->Reason, "no path '" + LbRoot + "/account/topic', Marker# PQ15");
        }

        Y_UNIT_TEST(TopicsSharingModernPathAreAllResolved) {
            // Without a local DC both clusters map to the same modern path.
            const TString modernPath = LbRoot + "/account/topic";
            TEnv env(false, [&](TNavigate&, TNavigate::TEntry& entry) {
                if (EntryPath(entry) != modernPath) {
                    return SetNotFound(entry);
                }
                FillFederationTopic(entry, 40, "topic", "dc1");
            });

            NPersQueue::TTopicNamesConverterFactory factory(false, "/Root/PQ", "");
            const auto dc1 = factory.MakeDiscoveryConverter("account/topic", {}, "dc1", "/Root");
            const auto dc2 = factory.MakeDiscoveryConverter("account/topic", {}, "dc2", "/Root");
            env.Start(MakeTopics({dc1, dc2}));

            auto* result = env.WaitAuthOk();
            UNIT_ASSERT_VALUES_EQUAL(result->TopicAndTablets.size(), 2);
            for (const auto& converter : {dc1, dc2}) {
                const auto& info = result->TopicAndTablets.at(converter->GetOriginalPath());
                UNIT_ASSERT_VALUES_EQUAL(info.TabletID, 40);
                UNIT_ASSERT(info.TopicNameConverter);
            }
        }
    } // Y_UNIT_TEST_SUITE(ReadInitAndAuthActor)

} // namespace NKikimr::NGRpcProxy::V1
