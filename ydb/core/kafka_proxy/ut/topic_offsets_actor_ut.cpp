#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kafka_proxy/actors/actors.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/schema/schema_ut_helpers.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>

#include <functional>

namespace NKafka::NTests {

    using namespace NKikimr;
    using namespace NKikimr::NPQ::NSchema::NTests;
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

    namespace {

        constexpr ui64 BALANCER_TABLET = 1001;
        constexpr ui64 PARTITION_TABLET = 2001;

        class TFakeSchemeCacheActor: public NActors::TActorBootstrapped<TFakeSchemeCacheActor> {
        public:
            explicit TFakeSchemeCacheActor(ui32 partitions)
                : Partitions(partitions)
            {
            }

            void Bootstrap() {
                Become(&TFakeSchemeCacheActor::StateWork);
            }

            STRICT_STFUNC(StateWork,
                          hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);)

        private:
            void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
                auto request = std::move(ev->Get()->Request);
                for (auto& entry : request->ResultSet) {
                    entry.Status = TNavigate::EStatus::Ok;
                    entry.Kind = TNavigate::EKind::KindTopic;
                    auto pqInfo = MakeIntrusive<TNavigate::TPQGroupInfo>();
                    pqInfo->Description.SetBalancerTabletID(BALANCER_TABLET);
                    for (ui32 i = 0; i < Partitions; ++i) {
                        auto* partition = pqInfo->Description.AddPartitions();
                        partition->SetPartitionId(i);
                        partition->SetTabletId(PARTITION_TABLET);
                    }
                    entry.PQGroupInfo = pqInfo;

                    auto self = MakeIntrusive<TNavigate::TDirEntryInfo>();
                    self->Info.SetName("topic");
                    self->Info.SetPathType(NKikimrSchemeOp::EPathTypePersQueueGroup);
                    self->Info.SetPathId(42);
                    self->Info.SetSchemeshardId(7);
                    entry.Self = self;
                }
                Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
            }

            ui32 Partitions;
        };

        class TFakePipeCacheActor: public NActors::TActorBootstrapped<TFakePipeCacheActor> {
        public:
            explicit TFakePipeCacheActor(ui32 partitions)
                : Partitions(partitions)
            {
            }

            void Bootstrap() {
                Become(&TFakePipeCacheActor::StateWork);
            }

            STRICT_STFUNC(StateWork,
                          hFunc(TEvPipeCache::TEvForward, Handle);
                          IgnoreFunc(TEvPipeCache::TEvUnlink);)

        private:
            void Handle(TEvPipeCache::TEvForward::TPtr& ev) {
                if (!ev->Get()->Ev || ev->Get()->Ev->Type() != TEvPersQueue::TEvStatus::EventType) {
                    return;
                }
                auto* response = new TEvPersQueue::TEvStatusResponse();
                for (ui32 i = 0; i < Partitions; ++i) {
                    auto* part = response->Record.AddPartResult();
                    part->SetPartition(i);
                    part->SetStatus(NKikimrPQ::TStatusResponse::STATUS_OK);
                    part->SetStartOffset(0);
                    part->SetEndOffset(0);
                    part->SetGeneration(1);
                    auto* consumer = part->AddConsumerResult();
                    consumer->SetConsumer("user");
                    consumer->SetErrorCode(NPersQueue::NErrorCode::OK);
                    consumer->SetCommitedOffset(0);
                }
                Send(ev->Sender, response, 0, ev->Cookie);
            }

            ui32 Partitions;
        };

        struct TIsolatedEnv {
            NActors::TTestBasicRuntime Runtime;

            explicit TIsolatedEnv(ui32 partitions = 1)
                : Runtime(1, false)
            {
                Runtime.Initialize(TAppPrepare().Unwrap());
                Runtime.UpdateCurrentTime(TInstant::Now());
                Runtime.GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
                Runtime.SetLogPriority(NKikimrServices::KAFKA_PROXY, NActors::NLog::PRI_DEBUG);
                Runtime.SetLogPriority(NKikimrServices::PQ_DESCRIBER, NActors::NLog::PRI_DEBUG);

                auto schemeCacheId = Runtime.Register(new TFakeSchemeCacheActor(partitions));
                Runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId);

                auto pipeCacheId = Runtime.Register(new TFakePipeCacheActor(partitions));
                Runtime.RegisterService(MakePipePerNodeCacheID(false), pipeCacheId);
                Runtime.DispatchEvents();
            }
        };

        class TEnableScheduleForRootGuard {
        public:
            explicit TEnableScheduleForRootGuard(NActors::TTestActorRuntime& runtime)
                : Runtime(runtime)
                , RootActorId(std::make_shared<TActorId>())
            {
                PrevObserver = Runtime.SetRegistrationObserverFunc(
                    [rootActorId = RootActorId](
                        TTestActorRuntimeBase& rt,
                        const TActorId& parentId,
                        const TActorId& actorId)
                    {
                        if (actorId == *rootActorId || parentId == *rootActorId) {
                            rt.EnableScheduleForActor(actorId);
                        }
                    });
            }

            ~TEnableScheduleForRootGuard() {
                Runtime.SetRegistrationObserverFunc(std::move(PrevObserver));
            }

            TEnableScheduleForRootGuard(const TEnableScheduleForRootGuard&) = delete;
            TEnableScheduleForRootGuard& operator=(const TEnableScheduleForRootGuard&) = delete;

            void SetRoot(const TActorId& actorId) {
                *RootActorId = actorId;
                Runtime.EnableScheduleForActor(actorId, true);
            }

            const TActorId& GetRoot() const {
                return *RootActorId;
            }

        private:
            NActors::TTestActorRuntime& Runtime;
            std::shared_ptr<TActorId> RootActorId;
            TTestActorRuntimeBase::TRegistrationObserver PrevObserver;
        };

        void CreateTopic(NActors::TTestActorRuntime& runtime, const TString& path, ui32 partitions = 1) {
            AssertStatus(DoCreate(runtime, MakeCreateTopicRequest(path, partitions)), Ydb::StatusIds::SUCCESS);
        }

        TTopicOffsetsSettings MakeSettings(
            const TString& path,
            TVector<ui32> partitionIds = {},
            TVector<TString> consumers = {})
        {
            return {
                .Path = path,
                .Database = "/Root",
                .PartitionIds = std::move(partitionIds),
                .Consumers = std::move(consumers),
            };
        }

        THolder<TEvKafka::TEvTopicOffsetsResponse> GrabTopicOffsetsResponse(
            NActors::TTestActorRuntime& runtime,
            const TActorId& edge,
            TDuration waitTimeout)
        {
            auto handle = runtime.GrabEdgeEvent<TEvKafka::TEvTopicOffsetsResponse>(edge, waitTimeout);
            UNIT_ASSERT(handle);
            return THolder(handle->Release());
        }

        THolder<TEvKafka::TEvTopicOffsetsResponse> RunTopicOffsets(
            NActors::TTestActorRuntime& runtime,
            TTopicOffsetsSettings settings,
            TDuration waitTimeout = TDuration::Seconds(30))
        {
            const auto edge = runtime.AllocateEdgeActor();
            TEnableScheduleForRootGuard schedule(runtime);
            schedule.SetRoot(runtime.Register(CreateTopicOffsetsActor(edge, std::move(settings))));
            return GrabTopicOffsetsResponse(runtime, edge, waitTimeout);
        }

        // Isolated runtime has no cluster timers. Wait until the test observer has
        // fired, jump past the actor retry delay, then grab the reply.
        THolder<TEvKafka::TEvTopicOffsetsResponse> RunTopicOffsetsAfterInjection(
            NActors::TTestActorRuntime& runtime,
            TTopicOffsetsSettings settings,
            std::function<bool()> injected)
        {
            const auto edge = runtime.AllocateEdgeActor();
            TEnableScheduleForRootGuard schedule(runtime);
            schedule.SetRoot(runtime.Register(CreateTopicOffsetsActor(edge, std::move(settings))));

            TDispatchOptions options;
            options.CustomFinalCondition = std::move(injected);
            runtime.DispatchEvents(options, TDuration::Seconds(5));
            runtime.AdvanceCurrentTime(TDuration::MilliSeconds(250));
            return GrabTopicOffsetsResponse(runtime, edge, TDuration::Seconds(5));
        }

        THashMap<ui32, TEvKafka::TPartitionOffsetsInfo> IndexByPartition(
            const TVector<TEvKafka::TPartitionOffsetsInfo>& partitions)
        {
            THashMap<ui32, TEvKafka::TPartitionOffsetsInfo> out;
            for (const auto& part : partitions) {
                out[part.PartitionId] = part;
            }
            return out;
        }

        auto BreakFirstStatusForward(NActors::TTestActorRuntime& runtime, size_t& broken) {
            auto* rt = &runtime;
            return runtime.AddObserver<TEvPipeCache::TEvForward>(
                [&broken, rt](TEvPipeCache::TEvForward::TPtr& ev) {
                    if (!ev || !ev->Get()->Ev) {
                        return;
                    }
                    if (ev->Get()->Ev->Type() != TEvPersQueue::TEvStatus::EventType) {
                        return;
                    }
                    if (broken >= 1) {
                        return;
                    }
                    ++broken;
                    const ui64 tabletId = ev->Get()->TabletId;
                    const ui64 subscribeCookie = ev->Get()->Options.SubscribeCookie;
                    rt->Send(new IEventHandle(
                        ev->Sender,
                        ev->Recipient,
                        new TEvPipeCache::TEvDeliveryProblem(tabletId, true /*notDelivered*/),
                        0,
                        subscribeCookie));
                    ev.Reset();
                });
        }

        auto DropStatusForwards(NActors::TTestActorRuntime& runtime, size_t* dropped = nullptr) {
            return runtime.AddObserver<TEvPipeCache::TEvForward>(
                [dropped](TEvPipeCache::TEvForward::TPtr& ev) {
                    if (ev && ev->Get()->Ev &&
                        ev->Get()->Ev->Type() == TEvPersQueue::TEvStatus::EventType)
                    {
                        if (dropped) {
                            ++*dropped;
                        }
                        ev.Reset();
                    }
                });
        }

        auto InjectStatusOnce(
            NActors::TTestActorRuntime& runtime,
            size_t& injected,
            std::function<void(TEvPersQueue::TEvStatusResponse&)> fill)
        {
            auto* rt = &runtime;
            return runtime.AddObserver<TEvPipeCache::TEvForward>(
                [&injected, rt, fill = std::move(fill)](TEvPipeCache::TEvForward::TPtr& ev) {
                    if (!ev || !ev->Get()->Ev) {
                        return;
                    }
                    if (ev->Get()->Ev->Type() != TEvPersQueue::TEvStatus::EventType) {
                        return;
                    }
                    if (injected >= 1) {
                        return;
                    }
                    ++injected;
                    auto* response = new TEvPersQueue::TEvStatusResponse();
                    fill(*response);
                    rt->Send(new IEventHandle(ev->Sender, ev->Recipient, response, 0, ev->Cookie));
                    ev.Reset();
                });
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TTopicOffsetsActor) {

        Y_UNIT_TEST(SuccessReturnsAllPartitions) {
            auto setup = CreateSetup("TopicOffsetsSmoke");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_smoke";
            CreateTopic(runtime, path, /*partitions=*/3);

            auto ev = RunTopicOffsets(runtime, MakeSettings(path));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 3u);

            const auto byId = IndexByPartition(ev->Partitions);
            for (ui32 i = 0; i < 3; ++i) {
                UNIT_ASSERT(byId.contains(i));
                const auto& part = byId.find(i)->second;
                UNIT_ASSERT_VALUES_EQUAL(part.StartOffset, 0u);
                UNIT_ASSERT_VALUES_EQUAL(part.EndOffset, 0u);
                UNIT_ASSERT(part.Consumers.empty());
            }
        }

        Y_UNIT_TEST(FiltersRequestedPartitions) {
            auto setup = CreateSetup("TopicOffsetsFilter");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_filter";
            CreateTopic(runtime, path, /*partitions=*/3);

            auto ev = RunTopicOffsets(runtime, MakeSettings(path, {1}));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].PartitionId, 1u);
        }

        Y_UNIT_TEST(MissingTopicIsSchemeError) {
            auto setup = CreateSetup("TopicOffsetsMissing");
            auto& runtime = setup->GetRuntime();

            auto ev = RunTopicOffsets(runtime, MakeSettings("/Root/missing_topic_offsets"));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SCHEME_ERROR);
            UNIT_ASSERT(!ev->Issues.Empty());
        }

        Y_UNIT_TEST(MissingPartitionIsSchemeError) {
            auto setup = CreateSetup("TopicOffsetsMissingPart");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_missing_part";
            CreateTopic(runtime, path);

            auto ev = RunTopicOffsets(runtime, MakeSettings(path, {99}));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SCHEME_ERROR);
            UNIT_ASSERT(ev->Issues.ToString().Contains("No partition 99"));
        }

        Y_UNIT_TEST(MissingPartitionSkippedWhenFetchingConsumers) {
            auto setup = CreateSetup("TopicOffsetsSkipPart");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_skip_part";
            CreateTopic(runtime, path);

            auto ev = RunTopicOffsets(runtime, MakeSettings(path, {0, 99}, {"user"}));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].PartitionId, 0u);
        }

        Y_UNIT_TEST(ConsumersIncludeCommittedOffset) {
            auto setup = CreateSetup("TopicOffsetsConsumer");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_consumer";
            CreateTopic(runtime, path);

            auto ev = RunTopicOffsets(runtime, MakeSettings(path, {}, {"user"}));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
            const auto it = ev->Partitions[0].Consumers.find("user");
            UNIT_ASSERT(it != ev->Partitions[0].Consumers.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second.Offset, 0u);
            UNIT_ASSERT_VALUES_EQUAL(it->second.PartitionIndex, 0u);
        }

        Y_UNIT_TEST(UnknownConsumerIsOmitted) {
            auto setup = CreateSetup("TopicOffsetsUnknownConsumer");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_unknown_consumer";
            CreateTopic(runtime, path);

            auto ev = RunTopicOffsets(runtime, MakeSettings(path, {0}, {"nosuch"}));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
            UNIT_ASSERT(!ev->Partitions[0].Consumers.contains("nosuch"));
        }

        Y_UNIT_TEST(RequireAuthenticationWithoutTokenIsUnauthorized) {
            auto setup = CreateSetup("TopicOffsetsRequireAuth");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_require_auth";
            CreateTopic(runtime, path);

            auto settings = MakeSettings(path);
            settings.RequireAuthentication = true;
            auto ev = RunTopicOffsets(runtime, std::move(settings));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::UNAUTHORIZED);
            UNIT_ASSERT(ev->Issues.ToString().Contains("unauthenticated"));
        }

        Y_UNIT_TEST(RequireSelectRowWithoutTokenSucceeds) {
            auto setup = CreateSetup("TopicOffsetsSelectRowAnon");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_select_row_anon";
            CreateTopic(runtime, path);

            auto settings = MakeSettings(path);
            settings.RequireSelectRow = true;
            auto ev = RunTopicOffsets(runtime, std::move(settings));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
        }

        Y_UNIT_TEST(RequireSelectRowWithBadTokenIsUnauthorized) {
            auto setup = CreateSetup("TopicOffsetsSelectRowAuth");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_select_row_auth";
            CreateTopic(runtime, path);

            auto token = MakeIntrusive<NACLib::TUserToken>("bad-user@staff", TVector<TString>{});
            token->SaveSerializationInfo();
            auto settings = MakeSettings(path);
            settings.Token = token->GetSerializedToken();
            settings.RequireSelectRow = true;
            auto ev = RunTopicOffsets(runtime, std::move(settings));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::UNAUTHORIZED);
        }

        Y_UNIT_TEST(UnauthenticatedExistenceCheckMissingIsSchemeError) {
            auto setup = CreateSetup("TopicOffsetsExistCheckMissing");
            auto& runtime = setup->GetRuntime();

            auto token = MakeIntrusive<NACLib::TUserToken>("bad-user@staff", TVector<TString>{});
            token->SaveSerializationInfo();
            auto settings = MakeSettings("/Root/topic_offsets_exist_check_missing");
            settings.Token = token->GetSerializedToken();
            settings.UnauthenticatedExistenceCheck = true;
            auto ev = RunTopicOffsets(runtime, std::move(settings));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SCHEME_ERROR);
        }

        Y_UNIT_TEST(UnauthenticatedExistenceCheckHiddenIsUnauthorized) {
            auto setup = CreateSetup("TopicOffsetsExistCheckHidden");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_exist_check_hidden";
            CreateTopic(runtime, path);

            auto token = MakeIntrusive<NACLib::TUserToken>("bad-user@staff", TVector<TString>{});
            token->SaveSerializationInfo();
            auto settings = MakeSettings(path);
            settings.Token = token->GetSerializedToken();
            settings.UnauthenticatedExistenceCheck = true;
            auto ev = RunTopicOffsets(runtime, std::move(settings));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::UNAUTHORIZED);
        }

        Y_UNIT_TEST(SelectRowUsesDedicatedTokenWhenDescriberIsAnonymous) {
            auto setup = CreateSetup("TopicOffsetsSelectRowOptionalAuth");
            auto& runtime = setup->GetRuntime();
            const TString path = "/Root/topic_offsets_select_row_optional";
            CreateTopic(runtime, path);

            auto token = MakeIntrusive<NACLib::TUserToken>("bad-user@staff", TVector<TString>{});
            token->SaveSerializationInfo();
            auto settings = MakeSettings(path);
            settings.RequireSelectRow = true;
            settings.SelectRowToken = token->GetSerializedToken();
            auto ev = RunTopicOffsets(runtime, std::move(settings));
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::UNAUTHORIZED);
            UNIT_ASSERT(ev->Issues.ToString().Contains("access denied"));
        }

        Y_UNIT_TEST(ParsesConsumerOffsetsAndSkipsErrors) {
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_offsets_parse_consumers";

            size_t injected = 0;
            auto injectObserver = InjectStatusOnce(runtime, injected, [](TEvPersQueue::TEvStatusResponse& response) {
                auto* part = response.Record.AddPartResult();
                part->SetPartition(0);
                part->SetStatus(NKikimrPQ::TStatusResponse::STATUS_OK);
                part->SetStartOffset(10);
                part->SetEndOffset(20);
                part->SetGeneration(3);
                auto* ok = part->AddConsumerResult();
                ok->SetConsumer("user");
                ok->SetErrorCode(NPersQueue::NErrorCode::OK);
                ok->SetCommitedOffset(15);
                ok->SetCommittedMetadata("meta");
                auto* bad = part->AddConsumerResult();
                bad->SetConsumer("gone");
                bad->SetErrorCode(NPersQueue::NErrorCode::SCHEMA_ERROR);
                bad->SetCommitedOffset(99);
            });

            auto ev = RunTopicOffsetsAfterInjection(runtime, MakeSettings(path, {0}, {"user"}), [&] {
                return injected >= 1;
            });
            UNIT_ASSERT_VALUES_EQUAL(injected, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].StartOffset, 10u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].EndOffset, 20u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].Generation, 3u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions[0].Consumers.size(), 1u);
            const auto it = ev->Partitions[0].Consumers.find("user");
            UNIT_ASSERT(it != ev->Partitions[0].Consumers.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second.Offset, 15u);
            UNIT_ASSERT_VALUES_EQUAL(it->second.Metadata, "meta");
            UNIT_ASSERT(!ev->Partitions[0].Consumers.contains("gone"));
        }

        Y_UNIT_TEST(RetriesOnDeliveryProblem) {
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_offsets_retry";

            size_t broken = 0;
            auto breakObserver = BreakFirstStatusForward(runtime, broken);
            auto ev = RunTopicOffsetsAfterInjection(runtime, MakeSettings(path), [&] {
                return broken >= 1;
            });
            UNIT_ASSERT_VALUES_EQUAL(broken, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
        }

        Y_UNIT_TEST(RetriesOnEmptyStatusResponse) {
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_offsets_empty_status";

            size_t injected = 0;
            auto injectObserver = InjectStatusOnce(runtime, injected, [](TEvPersQueue::TEvStatusResponse&) {});
            auto ev = RunTopicOffsetsAfterInjection(runtime, MakeSettings(path), [&] {
                return injected >= 1;
            });
            UNIT_ASSERT_VALUES_EQUAL(injected, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
        }

        Y_UNIT_TEST(RetriesOnInitializingStatus) {
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_offsets_initializing";

            size_t injected = 0;
            auto injectObserver = InjectStatusOnce(runtime, injected, [](TEvPersQueue::TEvStatusResponse& response) {
                auto* part = response.Record.AddPartResult();
                part->SetPartition(0);
                part->SetStatus(NKikimrPQ::TStatusResponse::STATUS_INITIALIZING);
            });
            auto ev = RunTopicOffsetsAfterInjection(runtime, MakeSettings(path), [&] {
                return injected >= 1;
            });
            UNIT_ASSERT_VALUES_EQUAL(injected, 1u);
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(ev->Partitions.size(), 1u);
        }

        Y_UNIT_TEST(TimesOutWhenStatusStuck) {
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_offsets_timeout";

            size_t dropped = 0;
            auto dropObserver = DropStatusForwards(runtime, &dropped);
            const auto edge = runtime.AllocateEdgeActor();
            TEnableScheduleForRootGuard schedule(runtime);
            schedule.SetRoot(runtime.Register(CreateTopicOffsetsActor(edge, MakeSettings(path))));

            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return dropped >= 1;
            };
            runtime.DispatchEvents(options, TDuration::Seconds(5));
            runtime.AdvanceCurrentTime(TDuration::Seconds(31));

            auto handle = runtime.GrabEdgeEvent<TEvKafka::TEvTopicOffsetsResponse>(edge, TDuration::Seconds(5));
            UNIT_ASSERT(handle);
            const auto* ev = handle->Get();
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::TIMEOUT);
            UNIT_ASSERT(ev->Issues.ToString().Contains("timed out"));

            auto* late = new TEvPersQueue::TEvStatusResponse();
            auto* part = late->Record.AddPartResult();
            part->SetPartition(0);
            part->SetStatus(NKikimrPQ::TStatusResponse::STATUS_OK);
            runtime.Send(new IEventHandle(schedule.GetRoot(), edge, late, 0, /*cookie=*/1));
            const auto prevTimeout = runtime.SetDispatchTimeout(TDuration::MilliSeconds(200));
            bool gotLate = false;
            try {
                gotLate = static_cast<bool>(runtime.GrabEdgeEvent<TEvKafka::TEvTopicOffsetsResponse>(
                    edge, TDuration::MilliSeconds(50)));
            } catch (const NActors::TEmptyEventQueueException&) {
            }
            runtime.SetDispatchTimeout(prevTimeout);
            UNIT_ASSERT(!gotLate);
        }

        Y_UNIT_TEST(PoisonRepliesCancelled) {
            TIsolatedEnv env;
            auto& runtime = env.Runtime;
            const TString path = "/Root/topic_offsets_poison";

            size_t dropped = 0;
            auto dropObserver = DropStatusForwards(runtime, &dropped);
            const auto edge = runtime.AllocateEdgeActor();
            TEnableScheduleForRootGuard schedule(runtime);
            schedule.SetRoot(runtime.Register(CreateTopicOffsetsActor(edge, MakeSettings(path))));

            TDispatchOptions options;
            options.CustomFinalCondition = [&] {
                return dropped >= 1;
            };
            runtime.DispatchEvents(options, TDuration::Seconds(5));
            runtime.Send(new IEventHandle(schedule.GetRoot(), edge, new NActors::TEvents::TEvPoison()));

            auto handle = runtime.GrabEdgeEvent<TEvKafka::TEvTopicOffsetsResponse>(edge, TDuration::Seconds(5));
            UNIT_ASSERT(handle);
            const auto* ev = handle->Get();
            UNIT_ASSERT_VALUES_EQUAL(ev->Status, Ydb::StatusIds::CANCELLED);
        }

    } // Y_UNIT_TEST_SUITE(TTopicOffsetsActor)

} // namespace NKafka::NTests
