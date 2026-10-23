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
            size_t SyncReplies = 0;
            bool RequireSyncReplyBeforeDone = false;
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
                    } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvLocalSyncDataResult
                            && ev->Recipient == ReplSkeletonId) {
                        UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvLocalSyncDataResult>()->Status, NKikimrProto::OK);
                        UNIT_ASSERT_VALUES_EQUAL(ev->Sender, SkeletonId);
                        ++SyncReplies;
                        return TTestActorRuntimeBase::EEventAction::DROP;
                    } else if (ev->GetTypeRewrite() == TEvQueryReplToken::EventType) {
                        // Keep the scheduler waiting for its token when checking that actual
                        // recovery work revokes readiness before any network/data I/O starts.
                        return TTestActorRuntimeBase::EEventAction::DROP;
                    } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvReplDone && ev->Recipient == ReplSkeletonId) {
                        if (RequireSyncReplyBeforeDone) {
                            UNIT_ASSERT(SyncReplies);
                        }
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

            std::vector<TLogoBlobID> HealthyUpdates(ui32 count, ui64 tabletId = 2) {
                std::vector<TLogoBlobID> updates;
                for (ui32 step = 1; step <= count; ++step) {
                    updates.emplace_back(tabletId, 1, step, 0, 64, 0);
                    AddHealthyBlob(updates.back());
                }
                return updates;
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
                auto update = std::make_unique<TEvReplSyncLogUpdate>(HullDs->GetIndexSnapshot());
                update->Blobs = std::move(blobs);
                auto result = std::make_unique<TEvLocalSyncDataResult>(NKikimrProto::OK,
                    TAppData::TimeProvider->Now(), nullptr, nullptr);
                const ui32 channel = result->GetChannelToSend();
                update->Reply = std::make_unique<IEventHandle>(ReplSkeletonId, SkeletonId, result.release(),
                    IEventHandle::MakeFlags(channel, 0));
                Runtime.Send(new IEventHandle(ReplId, SkeletonId, update.release()));
            }

            void Wakeup() {
                DispatchUntil([&] { return !Wakeups.empty(); });
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

            // Deferring the local ACK must also bound batches after timeout or
            // disconnection: a new job cannot start while this batch is pending.
            UNIT_ASSERT(!task.Terminate(NSyncer::TSyncerJobTask::TSyncStatusVal::Timeout).Die);
            UNIT_ASSERT(!task.Terminate(NSyncer::TSyncerJobTask::TSyncStatusVal::DroppedConnection).Die);
            auto result = std::make_unique<TEvLocalSyncDataResult>(NKikimrProto::OK,
                TAppData::TimeProvider->Now(), nullptr, nullptr);
            TEvLocalSyncDataResult::TPtr ack = static_cast<TEvLocalSyncDataResult::THandle*>(
                new IEventHandle(env.SkeletonId, env.SkeletonId, result.release()));
            UNIT_ASSERT(task.Handle(ack).Die);
        }

        Y_UNIT_TEST(HealthySyncLogDoesNotReplan) {
            TReplTestEnv env;
            const auto blobs = env.HealthyUpdates(2048);
            env.Start();
            env.DispatchUntil([&] { return env.DoneNotifications.size() == 1; });
            const size_t snapshots = env.SnapshotRequests;
            UNIT_ASSERT(snapshots > 1);
            for (size_t batch = 0; batch < 3; ++batch) {
                env.SendUpdate({blobs[batch], blobs[batch], blobs[batch + 1]});
                env.DispatchUntil([&] { return env.SyncReplies == batch + 1; });
                UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests, snapshots);
                UNIT_ASSERT_VALUES_EQUAL(env.FinishedPlans.size(), 1);
                UNIT_ASSERT(env.Wakeups.empty());
            }
            UNIT_ASSERT_VALUES_EQUAL(env.StartedNotifications, 0);
        }

        Y_UNIT_TEST(HealthySyncLogAboveOldLimitNeverScansUnchangedIndex) {
            TReplTestEnv env;
            env.HealthyUpdates(2048);
            env.Start();
            env.DispatchUntil([&] { return env.DoneNotifications.size() == 1; });
            const size_t snapshots = env.SnapshotRequests;
            // An unrelated missing record exposes any accidental full scan. It
            // must only be discovered once its own SyncLog notification arrives.
            const TLogoBlobID missing = env.AddMissingCanonicalBlob(1);
            const auto updates = env.HealthyUpdates(65537, 3);
            UNIT_ASSERT(!env.GInfo->EnableSingleDcMode);
            for (size_t batch = 0; batch < 3; ++batch) {
                env.SendUpdate(updates);
                env.DispatchUntil([&] { return env.SyncReplies == batch + 1; });
                UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests, snapshots);
                UNIT_ASSERT_VALUES_EQUAL(env.StartedPlans, 0);
                UNIT_ASSERT_VALUES_EQUAL(env.StartedNotifications, 0);
                UNIT_ASSERT(env.Wakeups.empty());
            }
            env.SendUpdate({missing});
            env.Wakeup();
            env.DispatchUntil([&] { return env.StartedPlans == 1; });
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests - snapshots, 1);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(env.ReplCtx->MonGroup.ReplItemsRemaining()), 1u);
        }

        Y_UNIT_TEST(SyncLogClassificationYieldsBeforeAcknowledgingBatch) {
            TReplTestEnv env;
            env.Start();
            env.DispatchUntil([&] { return env.DoneNotifications.size() == 1; });
            bool replySent = false;
            env.Runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvLocalSyncDataResult) {
                    replySent = true;
                }
                return false;
            });
            env.SendUpdate(env.HealthyUpdates(ReplSyncLogBatchSize * 3));
            bool inspected = false;
            // SendUpdate has already queued the first processing quantum. Send
            // through the mailbox so the probe runs after it, before the next one.
            env.Runtime.Send(new IEventHandle(env.ReplId, env.SkeletonId,
                new TEvReplInvoke([&](const TUnreplicatedBlobRecords&, TString) {
                    UNIT_ASSERT(!replySent);
                    inspected = true;
                })), 0, true);
            env.DispatchUntil([&] { return env.SyncReplies == 1; });
            UNIT_ASSERT(inspected);
            UNIT_ASSERT(env.Wakeups.empty());
        }

        Y_UNIT_TEST(SyncLogClassificationContinuesWhileAwaitingReplicationToken) {
            TReplTestEnv env;
            env.Start();
            env.DispatchUntil([&] { return env.DoneNotifications.size() == 1; });
            const auto missing = env.AddMissingCanonicalBlob(1);
            env.DeliverStartedPlans = true;
            env.SendUpdate({missing});
            env.Wakeup();
            env.DispatchUntil([&] { return env.StartedNotifications == 1; });
            env.DispatchUntil([&] { return env.SyncReplies == 1; });
            const size_t snapshots = env.SnapshotRequests;
            const auto updates = env.HealthyUpdates(65537, 3);
            env.SendUpdate(updates);
            env.DispatchUntil([&] { return env.SyncReplies == 2; });
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests, snapshots);
            UNIT_ASSERT_VALUES_EQUAL(env.StartedPlans, 1);
            UNIT_ASSERT_VALUES_EQUAL(env.DoneNotifications.size(), 1);
        }

        Y_UNIT_TEST(SyncLogUpdateBehindInitialScanCursorIsRetained) {
            TReplTestEnv env;
            env.HealthyUpdates(2048);
            env.OnSnapshot = [&] {
                env.OnSnapshot = [&] { env.SendUpdate({env.AddMissingCanonicalBlob(1)}); };
            };
            env.Start();
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 1; });
            env.DispatchUntil([&] { return env.SyncReplies == 1; });
            UNIT_ASSERT_VALUES_EQUAL(env.StartedPlans, 0);
            UNIT_ASSERT(env.DoneNotifications.empty());
            const size_t snapshots = env.SnapshotRequests;
            env.DeliverStartedPlans = true;
            env.Wakeup();
            env.DispatchUntil([&] { return env.StartedNotifications == 1; });
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests - snapshots, 1);
        }

        Y_UNIT_TEST(PendingClassificationDoesNotReportPhantomOnlyCompletion) {
            TReplTestEnv env;
            std::vector<TLogoBlobID> collected;
            for (ui32 step = 1; step <= 65537; ++step) {
                collected.emplace_back(1, 1, step, 0, 64, 0);
            }
            env.RequireSyncReplyBeforeDone = true;
            env.OnSnapshot = [&] { env.SendUpdate(std::move(collected)); };
            env.Start();
            env.DispatchUntil([&] { return env.DoneNotifications.size() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(env.SyncReplies, 1);
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests, 1);
            UNIT_ASSERT_VALUES_EQUAL(env.DoneNotifications.front(), 0);
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
            env.DispatchUntil([&] { return env.SyncReplies == 1; });
            UNIT_ASSERT(env.Wakeups.empty());
            env.Runtime.Send(new IEventHandle(TEvBlobStorage::EvCommenceRepl, 0, env.ReplId,
                env.SkeletonId, nullptr, 0));
            env.DispatchUntil([&] { return env.FinishedPlans.size() == 1; });
            UNIT_ASSERT_VALUES_EQUAL(env.SnapshotRequests, 1);
            UNIT_ASSERT(env.Wakeups.empty());
        }
    }

} // NKikimr
