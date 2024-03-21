#include "test_synclog.h"
#include "prepare.h"
#include "helpers.h"

// FIXME
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_db.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogrecovery.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_config.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_recoverylogwriter.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogkeeper.h>

#define STR Cerr

using namespace NKikimr;

// FIXME: backpressure

class TTestContext {
public:
    TPDiskCtxPtr PDiskCtx;
    TIntrusivePtr<TLsnMngr> LsnMngr;
    TVDiskID SelfVDiskId;
    TActorId LoggerId;
    TActorId SyncLogId;
};

///////////////////////////////////////////////////////////////////////////////////////////////
// TDataWriterActor
///////////////////////////////////////////////////////////////////////////////////////////////
class TDataWriterActor : public TActorBootstrapped<TDataWriterActor> {
    friend class TActorBootstrapped<TDataWriterActor>;
    const TDuration Period = TDuration::MilliSeconds(100);
    std::shared_ptr<TTestContext> TestCtx;
    TActorId ParentId;
    ui32 Generation = 0;
    ui32 Iterations = 10;

    void Bootstrap(const TActorContext &ctx) {
        SendBlockMessages(ctx);
        Become(&TThis::StateWorking);
        ctx.Schedule(Period, new TEvents::TEvWakeup());
    }

    void HandleWakeup(const TActorContext &ctx) {
        Y_ABORT_UNLESS(Iterations);
        if (--Iterations == 0) {
            ctx.Send(ParentId, new TEvents::TEvCompleted);
            Die(ctx);
        } else {
            SendBlockMessages(ctx);
            ctx.Schedule(Period, new TEvents::TEvWakeup());
        }
    }

    void SendBlockMessages(const TActorContext &ctx) {
        STR << "SendData iteration\n";
        ui64 tabletId = 123;
        for (unsigned counter = 0; counter < 10; counter++) {
            TLsnSeg seg = TestCtx->LsnMngr->AllocLsnForHullAndSyncLog();
            ++Generation;
            TEvBlobStorage::TEvVBlock logCmd(tabletId, Generation, TestCtx->SelfVDiskId, TInstant::Max());
            TAllocChunkSerializer serializer;
            logCmd.SerializeToArcadiaStream(&serializer);
            TIntrusivePtr<TEventSerializedData> buffers = serializer.Release(logCmd.CreateSerializationInfo());
            ctx.Send(TestCtx->LoggerId,
                     new NPDisk::TEvLog(TestCtx->PDiskCtx->Dsk->Owner, TestCtx->PDiskCtx->Dsk->OwnerRound,
                                        TLogSignature::SignatureBlock, TRcBuf(buffers->GetString()), seg, nullptr));
            // FIXME: problems on reboot
            ctx.Send(TestCtx->SyncLogId, new NSyncLog::TEvSyncLogPut(seg.Point(), tabletId, Generation, 0));
        }
    }

    STRICT_STFUNC(StateWorking,
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
        IgnoreFunc(NPDisk::TEvLogResult);
    )

public:
    TDataWriterActor(std::shared_ptr<TTestContext> testCtxt, TActorId parentId)
        : TActorBootstrapped<TDataWriterActor>()
        , TestCtx(testCtxt)
        , ParentId(parentId)
    {}
};

///////////////////////////////////////////////////////////////////////////////////////////////
// TSyncerActor
///////////////////////////////////////////////////////////////////////////////////////////////
class TSyncerActor : public TActorBootstrapped<TSyncerActor> {
    friend class TActorBootstrapped<TSyncerActor>;
    const TDuration Period = TDuration::MilliSeconds(200);
    std::shared_ptr<TTestContext> TestCtx;
    TSyncState SyncState;
    TVDiskID SourceVDisk;
    TVDiskID TargetVDisk;

    void Bootstrap(const TActorContext &ctx) {
        ScheduleNextSync(ctx);
        Become(&TThis::StateWorking);
    }

    void ScheduleNextSync(const TActorContext &ctx) {
        // Schedule next sync
        ctx.Schedule(Period, new TEvents::TEvWakeup());
    }

    void Sync(const TActorContext &ctx) {
        ctx.Send(TestCtx->SyncLogId, new TEvBlobStorage::TEvVSync(SyncState, SourceVDisk, TargetVDisk));
    }

    void Handle(TEvBlobStorage::TEvVSyncResult::TPtr &ev, const TActorContext &ctx) {
        const NKikimrBlobStorage::TEvVSyncResult &record = ev->Get()->Record;
        TVDiskID fromVDisk = VDiskIDFromVDiskID(record.GetVDiskID());
        Y_ABORT_UNLESS(SourceVDisk.SameGroupAndGeneration(fromVDisk));
        NKikimrProto::EReplyStatus status = record.GetStatus();
        Y_ABORT_UNLESS(status == NKikimrProto::OK);

        SyncState = SyncStateFromSyncState(record.GetNewSyncState());


        // Schedule next sync
        ScheduleNextSync(ctx);
    }

    void HandleWakeup(const TActorContext &ctx) {
        Sync(ctx);
    }

    STRICT_STFUNC(StateWorking,
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        IgnoreFunc(TEvBlobStorage::TEvVSyncResult);
    )

public:
    TSyncerActor(std::shared_ptr<TTestContext> testCtx, const TVDiskID &sourceVDisk)
        : TActorBootstrapped<TSyncerActor>()
        , TestCtx(std::move(testCtx))
        , SourceVDisk(sourceVDisk)
        , TargetVDisk(TestCtx->SelfVDiskId)
    {}
};

///////////////////////////////////////////////////////////////////////////////////////////////
// TSyncLogTestWriteActor
///////////////////////////////////////////////////////////////////////////////////////////////
class TSyncLogTestWriteActor : public TActorBootstrapped<TSyncLogTestWriteActor> {
    TConfiguration *Conf;
    std::shared_ptr<TTestContext> TestCtx = std::make_shared<TTestContext>();
    TVDiskContextPtr VCtx;
    TIntrusivePtr<TVDiskConfig> VDiskConfig;
    TActorId LogCutterId;
    TActorId DataWriterId;

    TIntrusivePtr<NKikimr::TDb> Db;

    friend class TActorBootstrapped<TSyncLogTestWriteActor>;

    void Bootstrap(const TActorContext &ctx) {
        STR << "RUN TEST\n";
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters);
        auto &vDiskInstance = Conf->VDisks->Get(0);
        auto &groupInfo = Conf->GroupInfo;
        VCtx = MakeIntrusive<TVDiskContext>(ctx.SelfID, groupInfo->PickTopology(), counters, vDiskInstance.VDiskID,
            ctx.ExecutorThread.ActorSystem, NPDisk::DEVICE_TYPE_UNKNOWN);
        VDiskConfig = vDiskInstance.Cfg;
        TestCtx->SelfVDiskId = groupInfo->GetVDiskId(VCtx->ShortSelfVDisk);

        Db = MakeIntrusive<TDb>(VDiskConfig, VCtx);

        ctx.Send(VDiskConfig->BaseInfo.PDiskActorID,
            new NPDisk::TEvYardInit(2, TestCtx->SelfVDiskId, VDiskConfig->BaseInfo.PDiskGuid));
        Become(&TThis::StateInitialize);
    }



    void Handle(NPDisk::TEvYardInitResult::TPtr &ev, const TActorContext &ctx) {
        NKikimrProto::EReplyStatus status = ev->Get()->Status;
        Y_ABORT_UNLESS(ev->Get()->Status == status != NKikimrProto::OK);

        const auto &m = ev->Get();
        TestCtx->PDiskCtx = TPDiskCtx::Create(m->PDiskParams, VDiskConfig);
        Db->LocalRecoveryInfo = nullptr;
        Db->LsnMngr = MakeIntrusive<TLsnMngr>(ui64(0), ui64(0), true);
        TestCtx->LsnMngr = Db->LsnMngr;

        // RecoveryLogWriter
        TestCtx->LoggerId = ctx.ExecutorThread.RegisterActor(CreateRecoveryLogWriter(
                    VDiskConfig->BaseInfo.PDiskActorID,
                    ctx.SelfID,
                    TestCtx->PDiskCtx->Dsk->Owner,
                    TestCtx->PDiskCtx->Dsk->OwnerRound,
                    TestCtx->LsnMngr->GetLsn(),
                    Db->VCtx->VDiskCounters));

        // RecoveryLogCutter
        TLogCutterCtx logCutterCtx = {VCtx, TestCtx->PDiskCtx, TestCtx->LsnMngr, VDiskConfig, TestCtx->LoggerId};
        LogCutterId = ctx.ExecutorThread.RegisterActor(CreateRecoveryLogCutter(std::move(logCutterCtx)));

        // Repaired SyncLog State
        NSyncLog::TSyncLogParams params = {
            VDiskConfig->BaseInfo.PDiskGuid,
            TestCtx->PDiskCtx->Dsk->ChunkSize,
            TestCtx->PDiskCtx->Dsk->AppendBlockSize,
            VDiskConfig->SyncLogAdvisedIndexedBlockSize,
            VCtx->SyncLogCache
        };
        TString explanation;
        std::unique_ptr<NSyncLog::TSyncLogRepaired> repaired =
            NSyncLog::TSyncLogRepaired::Construct(std::move(params), TString(), 0, explanation);
        Y_ABORT_UNLESS(repaired);

        // SyncLogActor
        auto slCtx = MakeIntrusive<NSyncLog::TSyncLogCtx>(
                VCtx,
                TestCtx->LsnMngr,
                TestCtx->PDiskCtx,
                TestCtx->LoggerId,
                LogCutterId,
                VDiskConfig->SyncLogMaxDiskAmount,
                VDiskConfig->SyncLogMaxEntryPointSize,
                VDiskConfig->SyncLogMaxMemAmount,
                VDiskConfig->MaxResponseSize,
                Db->SyncLogFirstLsnToKeep,
                false);
        TestCtx->SyncLogId = ctx.Register(CreateSyncLogActor(slCtx, Conf->GroupInfo, TestCtx->SelfVDiskId, std::move(repaired)));
        // Send Db birth lsn
        ui64 dbBirthLsn = 0;
        ctx.Send(TestCtx->SyncLogId, new NSyncLog::TEvSyncLogDbBirthLsn(dbBirthLsn));

        // run data writer
        DataWriterId = ctx.Register(new TDataWriterActor(TestCtx, ctx.SelfID));
        // run syncers
        for (ui32 i = 1; i < Conf->VDisks->GetSize(); ++i) {
            ctx.Register(new TSyncerActor(TestCtx, Conf->VDisks->Get(i).VDiskID));
        }
        Become(&TThis::StateWait);
    }

    void HandleCompleted(TEvents::TEvCompleted::TPtr& ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Finish(ctx);
    }

    void Finish(const TActorContext &ctx) {
        AtomicIncrement(Conf->SuccessCount);
        Conf->SignalDoneEvent();
        Die(ctx);
    }

    STRICT_STFUNC(StateInitialize,
        HFunc(NPDisk::TEvYardInitResult, Handle);
    )

    STRICT_STFUNC(StateWait,
        HFunc(TEvents::TEvCompleted, HandleCompleted);
    )

public:
    TSyncLogTestWriteActor(TConfiguration *conf)
        : TActorBootstrapped<TSyncLogTestWriteActor>()
        , Conf(conf)
    {}
};


void TSyncLogTestWrite::operator()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TSyncLogTestWriteActor(conf));
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
