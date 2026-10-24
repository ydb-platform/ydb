#include "guid_proxyobtain.h"
#include "guid_proxywrite.h"
#include "syncer_context.h"
#include "syncer_job_actor.h"
#include "syncer_job_task.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmsgwriter.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TSyncerFailedRealmTest) {
    Y_UNIT_TEST(FailedRealmCancellationWaitsForLocalWrite) {
        using namespace NSyncer;
        TTestActorSystem runtime(1);
        runtime.Start();
        const auto parent = runtime.AllocateEdgeActor(1);
        auto info = MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::ErasureMirror3dc, 1u, 3u, 3u);
        auto vctx = MakeIntrusive<TVDiskContext>(parent, info->PickTopology(),
            MakeIntrusive<::NMonitoring::TDynamicCounters>(), info->GetVDiskId(0),
            nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
        auto config = MakeIntrusive<TVDiskConfig>(TVDiskConfig::TBaseInfo::SampleForTests());
        auto context = MakeIntrusive<TSyncerContext>(vctx, MakeIntrusive<TLsnMngr>(ui64(0), ui64(0), false),
            std::make_shared<TPDiskCtx>(nullptr, parent, "test"),
            parent, parent, parent, parent, parent, nullptr, nullptr, nullptr, config);
        runtime.WrapInActorContext(parent, [&](IActor*) {
            for (bool finished : {false, true}) {
                TSyncerJobTask task(TSyncerJobTask::EJustSync, info->GetVDiskId(1), parent,
                    TActorId(), TPeerSyncState(), TSjCtx::Create(context, info));
                UNIT_ASSERT(task.NextRequest().Ev);
                auto reply = std::make_unique<TEvBlobStorage::TEvVSyncResult>();
                reply->Record.SetStatus(NKikimrProto::OK);
                reply->Record.SetFinished(finished);
                VDiskIDFromVDiskID(info->GetVDiskId(1), reply->Record.MutableVDiskID());
                SyncStateFromSyncState(TSyncState(), reply->Record.MutableNewSyncState());
                char record[NSyncLog::MaxRecFullSize];
                const auto size = NSyncLog::TSerializeRoutines::SetBlock(record, 1, 1, 1, 0);
                reply->Record.SetData(TString(record, size));
                TEvBlobStorage::TEvVSyncResult::TPtr response(
                    static_cast<TEvBlobStorage::TEvVSyncResult::THandle*>(
                        new IEventHandle(parent, parent, reply.release())));
                auto write = task.Handle(response, parent);
                UNIT_ASSERT(write.Ev || write.ActorActivity);
                UNIT_ASSERT(!task.Terminate(TSyncerJobTask::TSyncStatusVal::DroppedConnection).Die);
                UNIT_ASSERT(!task.Terminate(TSyncerJobTask::TSyncStatusVal::Timeout).Die);
                TEvLocalSyncDataResult::TPtr localResult(static_cast<TEvLocalSyncDataResult::THandle*>(
                    new IEventHandle(parent, parent, new TEvLocalSyncDataResult(NKikimrProto::OK,
                        runtime.GetClock(), nullptr, nullptr))));
                const auto outcome = task.Handle(localResult);
                UNIT_ASSERT(outcome.Die);
                UNIT_ASSERT(!outcome.Ev && !outcome.ActorActivity);
                UNIT_ASSERT_VALUES_EQUAL(task.GetCurrent().LastSyncStatus,
                    TSyncerJobTask::TSyncStatusVal::DroppedConnection);
            }
        });
        runtime.Stop();
    }

    Y_UNIT_TEST(FailedRealmSyncJobCancellation) {
        using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
        using namespace NSyncer;
        for (auto type : {TSyncerJobTask::EJustSync, TSyncerJobTask::EFullRecover}) {
            for (bool initiallyFailed : {false, true}) {
                TTestActorSystem runtime(1);
                runtime.Start();
                const auto remote = runtime.AllocateEdgeActor(1);
                const auto parent = runtime.AllocateEdgeActor(1);
                const TVDiskIdShort self(0, 0, 0), target(1, 0, 0);
                auto makeInfo = [&](ui32 generation, R::EState state) {
                    TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::Zero(), generation);
                    dynamic.FailRealmStates = {R::N0, state, R::N0};
                    for (ui32 i = 0; i < 9; ++i) {
                        dynamic.PushBackActorId(remote);
                    }
                    return MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupInfo::TTopology(
                        TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1), std::move(dynamic), "");
                };
                auto info = makeInfo(1, initiallyFailed ? R::F : R::N0);
                auto vctx = MakeIntrusive<TVDiskContext>(parent, info->PickTopology(),
                    MakeIntrusive<::NMonitoring::TDynamicCounters>(), info->GetVDiskId(self),
                    nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
                auto config = MakeIntrusive<TVDiskConfig>(TVDiskConfig::TBaseInfo::SampleForTests());
                auto context = MakeIntrusive<TSyncerContext>(vctx, MakeIntrusive<TLsnMngr>(ui64(0), ui64(0), false),
                    std::make_shared<TPDiskCtx>(nullptr, parent, "test"),
                    parent, parent, parent, parent, parent, nullptr, nullptr, nullptr, config);
                ui32 requests = 0;
                runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                    if (ev->Recipient == remote && (ev->GetTypeRewrite() == TEvBlobStorage::TEvVSync::EventType
                            || ev->GetTypeRewrite() == TEvBlobStorage::TEvVSyncFull::EventType)) {
                        ++requests;
                        return false;
                    }
                    return true;
                };
                TActorId job;
                runtime.WrapInActorContext(parent, [&](IActor*) {
                    auto task = std::make_unique<TSyncerJobTask>(type, info->GetVDiskId(target), remote,
                        TActorId(), TPeerSyncState(), TSjCtx::Create(context, info));
                    job = runtime.Register(CreateSyncerJob(context, std::move(task), parent), 1);
                });
                if (!initiallyFailed) {
                    runtime.Sim([&] { return !requests; });
                    auto failed = makeInfo(2, R::F);
                    runtime.Send(new IEventHandle(job, parent,
                        new TEvVGenerationChange(failed->GetVDiskId(self), failed)), 1);
                }
                auto done = runtime.WaitForEdgeActorEvent<TEvSyncerJobDone>(parent);
                UNIT_ASSERT_VALUES_EQUAL(done->Get()->Task->GetCurrent().LastSyncStatus,
                    NKikimrVDiskData::TSyncerVDiskEntry::DroppedConnection);
                UNIT_ASSERT_VALUES_EQUAL(requests, initiallyFailed ? 0 : 1);
                runtime.Stop();
            }
        }
    }

    Y_UNIT_TEST(FailedRealmGuidExchangeLifecycle) {
        using R = NKikimrBlobStorage::TGroupInfo::TFailRealm;
        for (bool write : {false, true}) {
            TTestActorSystem runtime(1);
            runtime.Start();
            const auto remote = runtime.AllocateEdgeActor(1);
            const auto parent = runtime.AllocateEdgeActor(1);
            const TVDiskIdShort target(1, 0, 0);
            const TVDiskIdShort self(0, 0, 0);
            auto makeInfo = [&](ui32 generation, R::EState state) {
                TBlobStorageGroupInfo::TDynamicInfo dynamic(TGroupId::Zero(), generation);
                dynamic.FailRealmStates = {R::N0, state, R::N0};
                for (ui32 i = 0; i < 9; ++i) {
                    dynamic.PushBackActorId(remote);
                }
                return MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupInfo::TTopology(
                    TBlobStorageGroupType::ErasureMirror3dc, 3, 3, 1), std::move(dynamic), "");
            };
            const auto info = makeInfo(1, R::F);
            auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
            auto context = MakeIntrusive<TVDiskContext>(parent, info->PickTopology(), counters,
                info->GetVDiskId(self), nullptr, NPDisk::DEVICE_TYPE_UNKNOWN);
            ui32 requests = 0;
            runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->Recipient == remote && ev->GetTypeRewrite() == TEvBlobStorage::TEvVSyncGuid::EventType) {
                    ++requests;
                    return false; // Keep the exchange in flight until the next group update.
                }
                return true;
            };
            IActor* actor = write
                ? CreateProxyForWritingVDiskGuid(context, info, info->GetVDiskId(self), info->GetVDiskId(target),
                    remote, parent, NKikimrBlobStorage::TSyncGuidInfo::Final, TVDiskEternalGuid(1))
                : CreateProxyForObtainingVDiskGuid(context, info, info->GetVDiskId(self), info->GetVDiskId(target),
                    remote, parent);
            const auto proxy = runtime.Register(actor, 1);
            auto advance = [&] {
                const auto barrier = runtime.GetClock() + TDuration::Seconds(1);
                runtime.Schedule(barrier, nullptr, new TFakeSchedulerCookie, 1);
                runtime.Sim([&] { return runtime.GetClock() < barrier; });
            };
            auto configure = [&](ui32 generation, R::EState state) {
                auto next = makeInfo(generation, state);
                runtime.Send(new IEventHandle(proxy, parent,
                    new TEvVGenerationChange(next->GetVDiskId(self), next)), 1);
                advance();
            };
            advance();
            UNIT_ASSERT_VALUES_EQUAL(requests, 0);
            configure(2, R::R);
            UNIT_ASSERT_VALUES_EQUAL(requests, 1);
            configure(3, R::F);
            // A stale update and a previously scheduled retry must not contact F.
            runtime.Send(new IEventHandle(proxy, parent, new TEvents::TEvWakeup), 1);
            configure(2, R::N0);
            UNIT_ASSERT_VALUES_EQUAL(requests, 1);
            configure(4, R::N0);
            UNIT_ASSERT_VALUES_EQUAL(requests, 2);
            runtime.Send(new IEventHandle(proxy, parent, new TEvents::TEvPoisonPill), 1);
            advance();
            runtime.Stop();
        }
    }
}

} // namespace NKikimr
