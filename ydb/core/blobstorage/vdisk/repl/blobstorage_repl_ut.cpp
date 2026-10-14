#include "blobstorage_repl.h"
#include "blobstorage_replbroker.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_takedbsnap.h>
#include <ydb/core/blobstorage/vdisk/syncer/syncer_context.h>
#include <ydb/core/blobstorage/vdisk/syncer/syncer_job_task.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgreader.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
    namespace {

        class TReplSkeleton : public TActor<TReplSkeleton> {
        public:
            TReplSkeleton()
                : TActor(&TReplSkeleton::StateFunc)
            {}

            STFUNC(StateFunc) {
                UNIT_FAIL("Unexpected skeleton event " << ev->GetTypeRewrite());
            }
        };

        struct TReplTestEnv {
            TTestBasicRuntime Runtime;
            TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
            std::shared_ptr<TReplCtx> ReplCtx;
            TIntrusivePtr<THullDs> HullDs;
            TActorId SkeletonId;
            TActorId ReplSkeletonId;
            TActorId ReplId;
            ui64 NextLsn = 1;
            size_t SnapshotRequests = 0;
            size_t StartedPlans = 0;
            size_t StartedNotifications = 0;
            bool DeliverStartedPlans = false;
            bool SchedulerBootstrapped = false;
            std::vector<TEvReplFinished::TInfoPtr> FinishedPlans;
            std::vector<ui64> DoneNotifications;
            std::deque<ui64> Wakeups;
            std::function<void()> OnSnapshot;

            explicit TReplTestEnv(bool paused = false) {
                Runtime.Initialize(TAppPrepare().Unwrap());
                SkeletonId = Runtime.AllocateEdgeActor();
                // DispatchEvents hides events already queued at edge actors. Use an
                // ordinary mailbox for notifications consumed by the observer below.
                ReplSkeletonId = Runtime.Register(new TReplSkeleton);
                TVector<TActorId> disks;
                for (ui32 i = 0; i < 9; ++i) {
                    disks.push_back(Runtime.AllocateEdgeActor());
                }
                GInfo = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureMirror3dc,
                    1u, 3u, 3u, &disks);
                const TVDiskID self = GInfo->GetVDiskId(0);
                auto base = TVDiskConfig::TBaseInfo::SampleForTests();
                base.VDiskIdShort = TVDiskIdShort(self);
                auto config = MakeIntrusive<TVDiskConfig>(base);
                // A full scan of more than 1024 keys must yield and request another snapshot.
                config->ReplPlanQuantum = TDuration::Zero();
                auto vctx = MakeIntrusive<TVDiskContext>(TActorId(), GInfo->PickTopology(),
                    new NMonitoring::TDynamicCounters(), self, nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
                auto hullCtx = MakeIntrusive<THullCtx>(vctx, config, 128u << 20, 256u << 10,
                    true, true, true, true, 1u, 1u, 0.5, TDuration::Minutes(5), TDuration::Seconds(1), 8u, 8u);
                HullDs = MakeIntrusive<THullDs>(hullCtx);
                const TLevelIndexSettings settings(hullCtx, 0, 0, 0, TDuration::Zero(), 0, false, false);
                auto arena = std::make_shared<TRopeArena>(TRopeArenaBackend::Allocate);
                HullDs->LogoBlobs = MakeIntrusive<TLogoBlobsDs>(settings, arena);
                HullDs->Blocks = MakeIntrusive<TBlocksDs>(settings, arena);
                HullDs->Barriers = MakeIntrusive<TBarriersDs>(settings, arena);
                HullDs->LogoBlobs->LoadCompleted();
                HullDs->Blocks->LoadCompleted();
                HullDs->Barriers->LoadCompleted();
                ReplCtx = std::make_shared<TReplCtx>(vctx, hullCtx, nullptr,
                    std::make_shared<THugeBlobCtx>("", nullptr, EBlobHeaderMode::OLD_HEADER),
                    4097, HullDs, GInfo, ReplSkeletonId, config, std::make_shared<std::atomic_uint64_t>(), paused);

                Runtime.SetScheduledEventFilter([this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev,
                        TDuration, TInstant&) {
                    if (ev->Recipient == ReplId && ev->GetTypeRewrite() == TEvents::TSystem::Wakeup) {
                        Wakeups.push_back(ev->Get<TEvents::TEvWakeup>()->Tag);
                    }
                    return true;
                });
                Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
                    if (ev->GetTypeRewrite() == TEvents::TSystem::Bootstrap && ev->Recipient == ReplId) {
                        SchedulerBootstrapped = true;
                    } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvTakeHullSnapshot) {
                        ++SnapshotRequests;
                        if (OnSnapshot) {
                            std::exchange(OnSnapshot, {})();
                        }
                        Runtime.Send(new IEventHandle(ev->Sender, ReplSkeletonId,
                            new TEvTakeHullSnapshotResult(HullDs->GetIndexSnapshot())), 0, true);
                        return TTestActorRuntimeBase::EEventAction::DROP;
                    } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplFinished) {
                        FinishedPlans.push_back(ev->Get<TEvReplFinished>()->Info);
                    } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplStarted) {
                        if (ev->Recipient == ReplSkeletonId) {
                            ++StartedNotifications;
                            return TTestActorRuntimeBase::EEventAction::DROP;
                        }
                        ++StartedPlans;
                        // Stop before network/data I/O; this fixture exercises the actual planner.
                        if (!DeliverStartedPlans) {
                            return TTestActorRuntimeBase::EEventAction::DROP;
                        }
                    } else if (ev->GetTypeRewrite() == TEvQueryReplToken::EventType) {
                        // Keep the scheduler waiting for its token when checking that actual
                        // recovery work revokes readiness before any network/data I/O starts.
                        return TTestActorRuntimeBase::EEventAction::DROP;
                    } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplDone && ev->Recipient == ReplSkeletonId) {
                        DoneNotifications.push_back(ev->Cookie);
                        return TTestActorRuntimeBase::EEventAction::DROP;
                    }
                    return TTestActorRuntimeBase::EEventAction::PROCESS;
                });
            }

            void AddHealthyBlob(const TLogoBlobID& id) {
                const auto& topology = GInfo->GetTopology();
                TIngress ingress;
                for (ui8 part = 0; part < 3; ++part) {
                    const auto disk = topology.GetVDiskInSubgroup(part, id.Hash());
                    ingress.Merge(*TIngress::CreateIngressWithLocal(&topology, disk, TLogoBlobID(id, part + 1)));
                }
                ingress = ingress.CopyWithoutLocal(GInfo->Type);
                const ui8 part = topology.GetIdxInSubgroup(ReplCtx->VCtx->ShortSelfVDisk, id.Hash());
                if (part < 3) {
                    ingress.Merge(*TIngress::CreateIngressWithLocal(&topology, ReplCtx->VCtx->ShortSelfVDisk,
                        TLogoBlobID(id, part + 1)));
                    HullDs->LogoBlobs->PutToFresh(NextLsn++, TKeyLogoBlob(id), part + 1, ingress,
                        TRope(TString(id.BlobSize(), 'x')), std::nullopt);
                } else {
                    TMemRecLogoBlob memRec(ingress);
                    memRec.SetNoBlob();
                    HullDs->LogoBlobs->PutToFresh(NextLsn++, TKeyLogoBlob(id), memRec);
                }
            }

            void Start() {
                ReplId = Runtime.Register(CreateReplActor(ReplCtx));
                DispatchUntil([&] { return SchedulerBootstrapped; });
            }

            TLogoBlobID AddMissingCanonicalBlob(ui64 tabletId) {
                const auto& topology = GInfo->GetTopology();
                TLogoBlobID id;
                for (ui32 step = 1;; ++step) {
                    id = TLogoBlobID(tabletId, 1, step, 0, 64, 0);
                    if (topology.GetIdxInSubgroup(ReplCtx->VCtx->ShortSelfVDisk, id.Hash()) == 0) {
                        break;
                    }
                }
                const auto source = topology.GetVDiskInSubgroup(1, id.Hash());
                const auto ingress = TIngress::CreateIngressWithLocal(&topology, source, TLogoBlobID(id, 2))
                    ->CopyWithoutLocal(GInfo->Type);
                UNIT_ASSERT(ingress.PartsWeMustHaveLocally(&topology, ReplCtx->VCtx->ShortSelfVDisk, id).Empty());
                TMemRecLogoBlob memRec(ingress);
                memRec.SetNoBlob();
                HullDs->LogoBlobs->PutToFresh(NextLsn++, TKeyLogoBlob(id), memRec);
                return id;
            }

            void SendOverflow() {
                std::vector<TLogoBlobID> updates;
                updates.reserve(MaxReplSyncLogUpdates + 1);
                for (ui32 step = 1; step <= MaxReplSyncLogUpdates + 1; ++step) {
                    // Changed records can already have disappeared from the index through GC.
                    updates.emplace_back(999, 1, step, 0, 64, 0);
                }
                SendUpdate(std::move(updates));
            }

            void DispatchUntil(std::function<bool()> condition) {
                if (!condition()) {
                    TDispatchOptions options;
                    options.CustomFinalCondition = condition;
                    Runtime.DispatchEvents(options, TDuration::Seconds(5));
                }
                UNIT_ASSERT(condition());
            }

            void SendUpdate(std::vector<TLogoBlobID> blobs) {
                auto update = std::make_unique<TEvReplSyncLogUpdate>();
                update->Blobs = std::move(blobs);
                Runtime.Send(new IEventHandle(ReplId, SkeletonId, update.release()));
            }

            void Wakeup() {
                UNIT_ASSERT_VALUES_EQUAL(Wakeups.size(), 1);
                const ui64 tag = Wakeups.front();
                Wakeups.pop_front();
                Runtime.AdvanceCurrentTime(ReplCtx->VDiskCfg->ReplTimeInterval);
                Runtime.Send(new IEventHandle(ReplId, ReplId, new TEvents::TEvWakeup(tag)));
            }
        };

    } // anonymous namespace

    Y_UNIT_TEST_SUITE(TBlobStorageReplScheduler) {

        Y_UNIT_TEST(FullSyncMetadataReachesSkeletonAsLocalSyncData) {
            TReplTestEnv env;
            env.ReplCtx->VDiskCfg->EnableLocalSyncLogDataCutting = false;
            auto syncerCtx = MakeIntrusive<TSyncerContext>(env.ReplCtx->VCtx,
                MakeIntrusive<TLsnMngr>(ui64(0), ui64(0), false),
                std::make_shared<TPDiskCtx>(nullptr, TActorId(), TString()), env.SkeletonId, TActorId(),
                env.SkeletonId, env.SkeletonId, env.SkeletonId,
                env.HullDs->LogoBlobs, env.HullDs->Blocks, env.HullDs->Barriers, env.ReplCtx->VDiskCfg);
            const auto context = NSyncer::TSjCtx::Create(syncerCtx, env.GInfo);
            const TVDiskID source = env.GInfo->GetVDiskId(1u);
            NSyncer::TSyncerJobTask task(NSyncer::TSyncerJobTask::EFullRecover, source,
                env.GInfo->GetActorId(1u), TActorId(), NSyncer::TPeerSyncState(), context);
            auto request = task.NextRequest();
            UNIT_ASSERT(request.Ev && request.Ev->Type() == TEvBlobStorage::EvVSyncFull);

            const TLogoBlobID id(1, 1, 1, 0, 64, 0);
            const auto& topology = env.GInfo->GetTopology();
            const auto ingress = *TIngress::CreateIngressWithLocal(&topology,
                topology.GetVDiskInSubgroup(1, id.Hash()), TLogoBlobID(id, 2));
            NSyncLog::TSequenceOfRecs records;
            records.SetLogoBlob(env.GInfo->Type, 1, id, ingress);
            const TString data(records.GetData(), records.GetSize());
            auto response = std::make_unique<TEvBlobStorage::TEvVSyncFullResult>(NKikimrProto::OK, source,
                TSyncState(), 0, TInstant::Zero(), nullptr, nullptr, 0,
                NKikimrBlobStorage::EFullSyncProtocol::UnorderedData);
            response->Record.SetStage(NKikimrBlobStorage::Barriers);
            response->Record.SetFinished(true);
            response->Record.SetData(data);
            TEvBlobStorage::TEvVSyncFullResult::TPtr ev =
                static_cast<TEvBlobStorage::TEvVSyncFullResult::THandle*>(
                    new IEventHandle(env.SkeletonId, env.GInfo->GetActorId(1u), response.release()));
            auto outcome = task.Handle(ev, env.SkeletonId);
            UNIT_ASSERT(!outcome.Die);
            std::unique_ptr<TEvLocalSyncData> localData;
            if (outcome.ActorActivity) {
                env.Runtime.Register(outcome.ActorActivity.release());
                auto local = env.Runtime.GrabEdgeEventRethrow<TEvLocalSyncData>(env.SkeletonId);
                localData.reset(local->Release().Release());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(outcome.To, env.SkeletonId);
                UNIT_ASSERT(outcome.Ev && outcome.Ev->Type() == TEvBlobStorage::EvLocalSyncData);
                localData.reset(static_cast<TEvLocalSyncData*>(outcome.Ev.release()));
            }
            UNIT_ASSERT_VALUES_EQUAL(localData->Data, data);
            localData->CalculateSizesFromData();
            UNIT_ASSERT(localData->LogoBlobsSize > 0);
        }

        Y_UNIT_TEST(HealthySyncLogChecksOnlyChangedBlobs) {
            TReplTestEnv env;
            std::vector<TLogoBlobID> blobs;
            for (ui32 step = 1; step <= 2048; ++step) {
                blobs.emplace_back(1, 1, step, 0, 64, 0);
                env.AddHealthyBlob(blobs.back());
            }
            env.Start();
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 1; });
            env.DispatchUntil([&] { return env.DoneNotifications.size() == 1; });
            UNIT_ASSERT(env.SnapshotRequests > 1);
            UNIT_ASSERT_VALUES_EQUAL(env.StartedPlans, 0);
            UNIT_ASSERT_VALUES_EQUAL(env.StartedNotifications, 0);
            UNIT_ASSERT(env.Wakeups.empty());

            for (size_t batch = 0; batch < 3; ++batch) {
                const size_t snapshots = env.SnapshotRequests;
                env.SendUpdate({blobs[batch], blobs[batch]});
                env.SendUpdate({blobs[batch + 1]});
                env.Wakeup();
                env.DispatchUntil([&] { return env.FinishedPlans.size() == batch + 2; });
                UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests - snapshots, 1);
                UNIT_ASSERT_VALUES_EQUAL(env.FinishedPlans.back()->ItemsPlanned, 0);
                UNIT_ASSERT(env.Wakeups.empty());
            }
            UNIT_ASSERT_VALUES_EQUAL(env.StartedNotifications, 0);
        }

        Y_UNIT_TEST(SyncLogUpdateDuringInitialScanIsRetained) {
            TReplTestEnv env;
            const TLogoBlobID id(1, 1, 1, 0, 64, 0);
            env.AddHealthyBlob(id);
            env.OnSnapshot = [&] { env.SendUpdate({id}); };
            env.Start();
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 1; });
            const size_t snapshots = env.SnapshotRequests;
            env.Wakeup();
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 2; });
            env.DispatchUntil([&] { return !env.DoneNotifications.empty() && env.DoneNotifications.back() == 0; });
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests - snapshots, 1);
            UNIT_ASSERT_VALUES_EQUAL(env.DoneNotifications.size(), 1);
            UNIT_ASSERT(env.Wakeups.empty());
        }

        Y_UNIT_TEST(SyncLogOverflowFallsBackToFullScan) {
            TReplTestEnv env;
            for (ui32 step = 1; step <= 2048; ++step) {
                env.AddHealthyBlob(TLogoBlobID(2, 1, step, 0, 64, 0));
            }
            env.Start();
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 1; });
            const size_t fullScanSnapshots = env.SnapshotRequests;
            env.SendOverflow();
            // This update arrives after the overflow has discarded its ID set. The
            // pending full scan must still find it without expanding that set again.
            const TLogoBlobID missing = env.AddMissingCanonicalBlob(1);
            env.SendUpdate({missing});
            env.DeliverStartedPlans = true;
            env.Wakeup();
            env.DispatchUntil([&] { return env.StartedNotifications == 1; });
            UNIT_ASSERT_VALUES_EQUAL(env.StartedPlans, 1);
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests - fullScanSnapshots, fullScanSnapshots);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(env.ReplCtx->MonGroup.ReplItemsRemaining()), 1u);
        }

        Y_UNIT_TEST(SyncLogUpdateBehindOverflowScanCursorIsRetained) {
            TReplTestEnv env;
            for (ui32 step = 1; step <= 2048; ++step) {
                env.AddHealthyBlob(TLogoBlobID(2, 1, step, 0, 64, 0));
            }
            env.Start();
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 1; });
            env.DispatchUntil([&] { return env.DoneNotifications.size() == 1; });
            const size_t fullScanSnapshots = env.SnapshotRequests;
            env.DoneNotifications.clear();
            env.SendOverflow();
            env.OnSnapshot = [&] {
                // The second snapshot resumes after the first 1023 keys. Insert
                // an older key now so the ongoing full scan cannot see it.
                env.OnSnapshot = [&] { env.SendUpdate({env.AddMissingCanonicalBlob(1)}); };
            };
            env.Wakeup();
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 2; });
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests - fullScanSnapshots, fullScanSnapshots);
            UNIT_ASSERT_VALUES_EQUAL(env.StartedPlans, 0);
            const size_t snapshots = env.SnapshotRequests;
            env.DeliverStartedPlans = true;
            env.Wakeup();
            env.DispatchUntil([&] { return env.StartedNotifications == 1; });
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests - snapshots, 1);
            UNIT_ASSERT(env.DoneNotifications.empty()); // no premature phantom-only completion
        }

        Y_UNIT_TEST(PendingFullScanDoesNotReportPhantomOnlyCompletion) {
            TReplTestEnv env;
            env.AddHealthyBlob(TLogoBlobID(2, 1, 1, 0, 64, 0));
            env.OnSnapshot = [&] { env.SendOverflow(); };
            env.Start();
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 1; });
            env.Wakeup();
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 2; });
            env.DispatchUntil([&] { return !env.DoneNotifications.empty() && env.DoneNotifications.back() == 0; });
            UNIT_ASSERT_VALUES_EQUAL(env.DoneNotifications.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(env.StartedNotifications, 0);
            UNIT_ASSERT(env.Wakeups.empty());
        }

        Y_UNIT_TEST(LateSyncLogPlansMissingCanonicalReplica) {
            TReplTestEnv env;
            for (ui32 step = 1; step <= 2048; ++step) {
                env.AddHealthyBlob(TLogoBlobID(1, 1, step, 0, 64, 0));
            }
            env.Start();
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 1; });
            // Metadata from the surviving realm has no presence bit for this main replica.
            const TLogoBlobID id = env.AddMissingCanonicalBlob(2);
            const size_t snapshots = env.SnapshotRequests;
            env.SendUpdate({id, id});
            env.SendUpdate({id});
            env.Wakeup();
            env.DispatchUntil([&] { return env.StartedPlans == 1; });
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests - snapshots, 1);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(env.ReplCtx->MonGroup.ReplItemsRemaining()), 1u);
        }

        Y_UNIT_TEST(SyncLogDoesNotCommencePausedReplication) {
            TReplTestEnv env(true);
            const TLogoBlobID id(1, 1, 1, 0, 64, 0);
            env.AddHealthyBlob(id);
            env.Start();
            env.SendUpdate({id});
            UNIT_ASSERT(env.Wakeups.empty());
            env.Runtime.Send(new IEventHandle(TEvBlobStorage::EvCommenceRepl, 0, env.ReplId,
                env.SkeletonId, nullptr, 0));
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests, 1);
            UNIT_ASSERT(env.Wakeups.empty());
        }
    }

} // NKikimr
