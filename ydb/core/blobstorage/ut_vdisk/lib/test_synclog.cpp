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
#include <ydb/core/blobstorage/vdisk/vdisk_actor.h>

#include <library/cpp/monlib/service/mon_service_http_request.h>

#include <atomic>
#include <util/generic/set.h>

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
// TObservedLogWriterProxyActor
///////////////////////////////////////////////////////////////////////////////////////////////
class TObservedLogWriterProxyActor : public TActorBootstrapped<TObservedLogWriterProxyActor> {
    friend class TActorBootstrapped<TObservedLogWriterProxyActor>;

    const TActorId RealLoggerId;
    const TActorId ParentId;
    const ui64 ExpectedFreeUpToLsn;
    const ui32 ExpectedMinSyncLogIdxCommits;
    ui32 SyncLogIdxCommits = 0;
    bool CutLogObserved = false;

    void Bootstrap(const TActorContext&) {
        Become(&TThis::StateWorking);
    }

    void Handle(NPDisk::TEvLog::TPtr& ev, const TActorContext& ctx) {
        NPDisk::TEvLog *msg = ev->Get();
        const TLogSignature signature = msg->Signature.GetUnmasked();
        bool notifyParent = false;

        if (signature == TLogSignature::SignatureSyncLogIdx) {
            ++SyncLogIdxCommits;
        } else if (signature == TLogSignature::SignatureHullCutLog && !CutLogObserved) {
            CutLogObserved = true;
            Y_ABORT_UNLESS(msg->CommitRecord.FirstLsnToKeep > ExpectedFreeUpToLsn,
                "LogCutter generated stale cut; FirstLsnToKeep# %" PRIu64
                " ExpectedFreeUpToLsn# %" PRIu64 " SyncLogIdxCommits# %" PRIu32,
                msg->CommitRecord.FirstLsnToKeep, ExpectedFreeUpToLsn, SyncLogIdxCommits);
            Y_ABORT_UNLESS(SyncLogIdxCommits >= ExpectedMinSyncLogIdxCommits,
                "LogCutter cut the recovery log before bounded SyncLog checkpoints were visible; "
                "SyncLogIdxCommits# %" PRIu32 " ExpectedMinSyncLogIdxCommits# %" PRIu32
                " FirstLsnToKeep# %" PRIu64 " ExpectedFreeUpToLsn# %" PRIu64,
                SyncLogIdxCommits, ExpectedMinSyncLogIdxCommits,
                msg->CommitRecord.FirstLsnToKeep, ExpectedFreeUpToLsn);
            notifyParent = true;
        }

        ctx.Send(ev->Forward(RealLoggerId).Release());
        if (notifyParent) {
            ctx.Send(ParentId, new TEvents::TEvCompleted);
        }
    }

    void Handle(NPDisk::TEvMultiLog::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(ev->Forward(RealLoggerId).Release());
    }

    STRICT_STFUNC(StateWorking,
        HFunc(NPDisk::TEvLog, Handle);
        HFunc(NPDisk::TEvMultiLog, Handle);
    )

public:
    TObservedLogWriterProxyActor(
            TActorId realLoggerId,
            TActorId parentId,
            ui64 expectedFreeUpToLsn,
            ui32 expectedMinSyncLogIdxCommits)
        : TActorBootstrapped<TObservedLogWriterProxyActor>()
        , RealLoggerId(realLoggerId)
        , ParentId(parentId)
        , ExpectedFreeUpToLsn(expectedFreeUpToLsn)
        , ExpectedMinSyncLogIdxCommits(expectedMinSyncLogIdxCommits)
    {}
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
            ctx.ActorSystem(), NPDisk::DEVICE_TYPE_UNKNOWN);
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
        TestCtx->LoggerId = ctx.Register(CreateRecoveryLogWriter(
                    VDiskConfig->BaseInfo.PDiskActorID,
                    ctx.SelfID,
                    TestCtx->PDiskCtx->Dsk->Owner,
                    TestCtx->PDiskCtx->Dsk->OwnerRound,
                    TestCtx->LsnMngr->GetLsn(),
                    Db->VCtx->VDiskCounters));

        // RecoveryLogCutter
        TLogCutterCtx logCutterCtx = {VCtx, TestCtx->PDiskCtx, TestCtx->LsnMngr, VDiskConfig, TestCtx->LoggerId};
        LogCutterId = ctx.Register(CreateRecoveryLogCutter(std::move(logCutterCtx)));

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
// TSyncLogCutLogProgressActor
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TSyncLogCutLogProgressActor : public TActorBootstrapped<TSyncLogCutLogProgressActor> {
    TConfiguration *Conf;
    std::shared_ptr<TTestContext> TestCtx = std::make_shared<TTestContext>();
    TVDiskContextPtr VCtx;
    TIntrusivePtr<TVDiskConfig> VDiskConfig;
    TActorId LogCutterId;
    TActorId RealLoggerId;

    TIntrusivePtr<NKikimr::TDb> Db;

    friend class TActorBootstrapped<TSyncLogCutLogProgressActor>;

    static void PutBlock(NSyncLog::TSyncLogPtr syncLog, ui64 lsn, ui32 gen) {
        char buf[NSyncLog::MaxRecFullSize];
        const ui32 size = NSyncLog::TSerializeRoutines::SetBlock(buf, lsn, 123, gen, 0);
        syncLog->PutOne(reinterpret_cast<const NSyncLog::TRecordHdr *>(buf), size);
    }

    static ui64 FillRecoveredMemTail(NSyncLog::TSyncLogPtr syncLog, ui32 pagesToRecover, ui64 *lastPageFirstLsn) {
        ui64 lsn = 1;
        ui32 gen = 0;
        while (syncLog->GetNumberOfPagesInMemory() < pagesToRecover) {
            PutBlock(syncLog, lsn++, ++gen);
        }

        const NSyncLog::TSyncLogSnapshotPtr snap = syncLog->GetSnapshot();
        Y_ABORT_UNLESS(snap->MemSnapPtr && snap->MemSnapPtr->Size() == pagesToRecover);
        *lastPageFirstLsn = (*snap->MemSnapPtr)[pagesToRecover - 1].GetFirstLsn();

        // Make the last page contain records beyond lastPageFirstLsn + 1. The cut request below uses that
        // value so that the last page is included in the cut-log SwapSnap and LogCutter can still observe a
        // strictly higher SyncLog boundary after the checkpoint sequence completes.
        for (ui32 i = 0; i < 8; ++i) {
            PutBlock(syncLog, lsn++, ++gen);
        }

        return lsn - 1;
    }

    void Bootstrap(const TActorContext &ctx) {
        STR << "RUN SYNCLOG CUT LOG PROGRESS TEST\n";
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(new ::NMonitoring::TDynamicCounters);
        auto &vDiskInstance = Conf->VDisks->Get(0);
        auto &groupInfo = Conf->GroupInfo;
        VCtx = MakeIntrusive<TVDiskContext>(ctx.SelfID, groupInfo->PickTopology(), counters, vDiskInstance.VDiskID,
            ctx.ActorSystem(), NPDisk::DEVICE_TYPE_UNKNOWN);
        VDiskConfig = vDiskInstance.Cfg;
        TestCtx->SelfVDiskId = groupInfo->GetVDiskId(VCtx->ShortSelfVDisk);

        Db = MakeIntrusive<TDb>(VDiskConfig, VCtx);

        ctx.Send(VDiskConfig->BaseInfo.PDiskActorID,
            new NPDisk::TEvYardInit(2, TestCtx->SelfVDiskId, VDiskConfig->BaseInfo.PDiskGuid));
        Become(&TThis::StateInitialize);
    }

    void Handle(NPDisk::TEvYardInitResult::TPtr &ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK);

        const auto &m = ev->Get();
        TestCtx->PDiskCtx = TPDiskCtx::Create(m->PDiskParams, VDiskConfig);
        Db->LocalRecoveryInfo = nullptr;

        const ui32 pagesInChunk = TestCtx->PDiskCtx->Dsk->ChunkSize / TestCtx->PDiskCtx->Dsk->AppendBlockSize;
        const ui32 pagesToRecover = pagesInChunk * 3 + 1;

        VDiskConfig->SyncLogMaxMemAmount = ui64(TestCtx->PDiskCtx->Dsk->AppendBlockSize) * pagesToRecover * 2;
        VDiskConfig->SyncLogMaxDiskAmount = ui64(TestCtx->PDiskCtx->Dsk->ChunkSize) * 16;

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

        ui64 lastPageFirstLsn = 0;
        const ui64 recoveredLastLsn = FillRecoveredMemTail(repaired->SyncLogPtr, pagesToRecover, &lastPageFirstLsn);
        const ui64 freeUpToLsn = lastPageFirstLsn + 1;
        const ui32 expectedMinSyncLogIdxCommits = 4;

        Db->LsnMngr = MakeIntrusive<TLsnMngr>(recoveredLastLsn, recoveredLastLsn, false);
        TestCtx->LsnMngr = Db->LsnMngr;

        RealLoggerId = ctx.Register(CreateRecoveryLogWriter(
                    VDiskConfig->BaseInfo.PDiskActorID,
                    ctx.SelfID,
                    TestCtx->PDiskCtx->Dsk->Owner,
                    TestCtx->PDiskCtx->Dsk->OwnerRound,
                    TestCtx->LsnMngr->GetLsn(),
                    Db->VCtx->VDiskCounters));
        TestCtx->LoggerId = ctx.Register(new TObservedLogWriterProxyActor(
                    RealLoggerId,
                    ctx.SelfID,
                    freeUpToLsn,
                    expectedMinSyncLogIdxCommits));

        TLogCutterCtx logCutterCtx = {VCtx, TestCtx->PDiskCtx, TestCtx->LsnMngr, VDiskConfig, TestCtx->LoggerId};
        LogCutterId = ctx.Register(CreateRecoveryLogCutter(std::move(logCutterCtx)));

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
        ctx.Send(TestCtx->SyncLogId, new NSyncLog::TEvSyncLogDbBirthLsn(0));

        const ui64 otherComponentsLsnToKeep = recoveredLastLsn + 1000;
        ctx.Send(LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::Hull, otherComponentsLsnToKeep));
        ctx.Send(LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::Syncer, otherComponentsLsnToKeep));
        ctx.Send(LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::HugeKeeper, otherComponentsLsnToKeep));
        ctx.Send(LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::Scrub, otherComponentsLsnToKeep));

        ctx.Send(TestCtx->SyncLogId, new NPDisk::TEvCutLog(TestCtx->PDiskCtx->Dsk->Owner,
            TestCtx->PDiskCtx->Dsk->OwnerRound, freeUpToLsn, 0, 0, 0, 0));
        ctx.Send(LogCutterId, new NPDisk::TEvCutLog(TestCtx->PDiskCtx->Dsk->Owner,
            TestCtx->PDiskCtx->Dsk->OwnerRound, freeUpToLsn, 0, 0, 0, 0));

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
    TSyncLogCutLogProgressActor(TConfiguration *conf)
        : TActorBootstrapped<TSyncLogCutLogProgressActor>()
        , Conf(conf)
    {}
};

void TSyncLogCutLogProgress::operator()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TSyncLogCutLogProgressActor(conf));
}

class TSyncLogSeedNormalVDiskBeforeRescueActor : public TSyncTestBase {
    void Scenario(const TActorContext &ctx) override {
        auto msgPacks = std::make_shared<TVector<TMsgPackInfo>>();
        msgPacks->emplace_back(1024, 100);
        auto badSteps = std::make_shared<TSet<ui32>>();
        auto handleClass = std::make_shared<TPutHandleClassGenerator>(NKikimrBlobStorage::EPutHandleClass::TabletLog);

        SyncRunner->Run(ctx, CreateManyPuts(Conf, SyncRunner->NotifyID(), Conf->VDisks->Get(0), msgPacks,
            DefaultTestTabletId, 0, 1, handleClass, badSteps, TDuration::Seconds(0)));
        Y_ABORT_UNLESS(badSteps->size() < 100,
            "seed phase did not write any blob to the selected VDisk; badSteps# %" PRIu64,
            ui64(badSteps->size()));
    }

public:
    explicit TSyncLogSeedNormalVDiskBeforeRescueActor(TConfiguration *conf)
        : TSyncTestBase(conf)
    {}
};

void TSyncLogSeedNormalVDiskBeforeRescue::operator()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TSyncLogSeedNormalVDiskBeforeRescueActor(conf));
}

struct TRescueWriteGateState {
    std::atomic<bool> ExpectOneRescueCycle = false;
    std::atomic<ui32> Owner = 0;
    std::atomic<ui64> OwnerRound = 0;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSyncLogRescueWriteGateE2EActor
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
struct TRescueHttpRequestMock : NMonitoring::IHttpRequest {
    TCgiParameters CgiParameters;
    THttpHeaders HttpHeaders;

    const char* GetURI() const override {
        return "";
    }

    const char* GetPath() const override {
        return "";
    }

    const TCgiParameters& GetParams() const override {
        return CgiParameters;
    }

    const TCgiParameters& GetPostParams() const override {
        return CgiParameters;
    }

    TStringBuf GetPostContent() const override {
        return TStringBuf();
    }

    HTTP_METHOD GetMethod() const override {
        return HTTP_METHOD_GET;
    }

    const THttpHeaders& GetHeaders() const override {
        return HttpHeaders;
    }

    TString GetRemoteAddr() const override {
        return TString();
    }
};

class TRescueObservedPDiskProxyActor : public TActorBootstrapped<TRescueObservedPDiskProxyActor> {
    friend class TActorBootstrapped<TRescueObservedPDiskProxyActor>;

    enum class EMode {
        Blocked,
        SyncLogCommitInProgress,
        SyncLogCommitObserved,
        RescueCutObserved,
    };

    const TActorId RealPDiskId;
    const TActorId ParentId;
    const std::shared_ptr<TRescueWriteGateState> GateState;
    EMode Mode = EMode::Blocked;
    ui32 ObservedMutations = 0;
    TActorId YardInitSender;
    ui64 YardInitCookie = 0;

    void Bootstrap(const TActorContext&) {
        Become(&TThis::StateWorking);
    }

    void Handle(NPDisk::TEvYardInit::TPtr& ev, const TActorContext& ctx) {
        Y_ABORT_UNLESS(!YardInitSender);
        YardInitSender = ev->Sender;
        YardInitCookie = ev->Cookie;
        ctx.Send(RealPDiskId, ev->Release().Release(), 0, ev->Cookie);
    }

    void Handle(NPDisk::TEvYardInitResult::TPtr& ev, const TActorContext& ctx) {
        Y_ABORT_UNLESS(YardInitSender);
        if (ev->Get()->Status == NKikimrProto::OK) {
            GateState->Owner.store(ui32(ev->Get()->PDiskParams->Owner));
            GateState->OwnerRound.store(ev->Get()->PDiskParams->OwnerRound);
        }
        ctx.Send(YardInitSender, ev->Release().Release(), 0, YardInitCookie);
    }

    void Handle(NPDisk::TEvLog::TPtr& ev, const TActorContext& ctx) {
        NPDisk::TEvLog *msg = ev->Get();
        const TLogSignature signature = msg->Signature.GetUnmasked();

        switch (Mode) {
            case EMode::Blocked:
                if (!GateState->ExpectOneRescueCycle.exchange(false)) {
                    Y_ABORT("rescue mode allowed a recovery-log write before one-shot cycle permission; "
                        "Signature# %" PRIu32 " FirstLsnToKeep# %" PRIu64,
                        ui32(signature), msg->CommitRecord.FirstLsnToKeep);
                }
                [[fallthrough]];

            case EMode::SyncLogCommitInProgress:
                Y_ABORT_UNLESS(signature == TLogSignature::SignatureSyncLogIdx,
                    "one-shot rescue cycle was consumed by a non-SyncLog commit; "
                    "Signature# %" PRIu32 " ExpectedSignature# %" PRIu32,
                    ui32(signature), ui32(TLogSignature::SignatureSyncLogIdx));
                ++ObservedMutations;
                Mode = EMode::SyncLogCommitObserved;
                ctx.Send(ev->Forward(RealPDiskId).Release());
                return;

            case EMode::SyncLogCommitObserved:
                Y_ABORT_UNLESS(signature == TLogSignature::SignatureHullCutLog,
                    "rescue SyncLog checkpoint was not followed by a recovery-log cut; "
                    "Signature# %" PRIu32 " ExpectedSignature# %" PRIu32,
                    ui32(signature), ui32(TLogSignature::SignatureHullCutLog));
                ++ObservedMutations;
                Mode = EMode::RescueCutObserved;
                ctx.Send(ev->Forward(RealPDiskId).Release());
                ctx.Send(ParentId, new TEvents::TEvCompleted);
                return;

            case EMode::RescueCutObserved:
                Y_ABORT("rescue mode allowed more recovery-log writes after the allowed checkpoint/cut cycle; "
                    "ObservedMutations# %" PRIu32 " Signature# %" PRIu32 " FirstLsnToKeep# %" PRIu64,
                    ObservedMutations, ui32(signature), msg->CommitRecord.FirstLsnToKeep);
                break;
        }
    }

    void Handle(NPDisk::TEvChunkWrite::TPtr& ev, const TActorContext& ctx) {
        switch (Mode) {
            case EMode::Blocked:
                if (!GateState->ExpectOneRescueCycle.exchange(false)) {
                    FailUnexpectedDiskMutation("TEvChunkWrite", ev->Sender);
                }
                Mode = EMode::SyncLogCommitInProgress;
                [[fallthrough]];

            case EMode::SyncLogCommitInProgress:
                ++ObservedMutations;
                ctx.Send(ev->Forward(RealPDiskId).Release());
                return;

            case EMode::SyncLogCommitObserved:
            case EMode::RescueCutObserved:
                FailUnexpectedDiskMutation("TEvChunkWrite", ev->Sender);
                break;
        }
    }

    void FailUnexpectedDiskMutation(const char *eventName, const TActorId& sender) const {
        const TString senderString = sender.ToString();
        Y_ABORT("rescue mode allowed unexpected disk mutation; Event# %s Sender# %s Mode# %" PRIu32
            " ObservedMutations# %" PRIu32,
            eventName, senderString.data(), ui32(Mode), ObservedMutations);
    }

    STATEFN(StateWorking) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NPDisk::TEvYardInit, Handle);
            HFunc(NPDisk::TEvYardInitResult, Handle);
            HFunc(NPDisk::TEvLog, Handle);
            HFunc(NPDisk::TEvChunkWrite, Handle);
            case NPDisk::TEvMultiLog::EventType:
                FailUnexpectedDiskMutation("TEvMultiLog", ev->Sender);
                break;
            case NPDisk::TEvChunkReserve::EventType:
                FailUnexpectedDiskMutation("TEvChunkReserve", ev->Sender);
                break;
            case NPDisk::TEvChunkForget::EventType:
                FailUnexpectedDiskMutation("TEvChunkForget", ev->Sender);
                break;

            default:
                TActivationContext::Send(ev->Forward(RealPDiskId));
                break;
        }
    }

public:
    TRescueObservedPDiskProxyActor(
            TActorId realPDiskId,
            TActorId parentId,
            std::shared_ptr<TRescueWriteGateState> gateState)
        : TActorBootstrapped<TRescueObservedPDiskProxyActor>()
        , RealPDiskId(realPDiskId)
        , ParentId(parentId)
        , GateState(std::move(gateState))
    {}
};

class TSyncLogRescueWriteGateE2EActor : public TActorBootstrapped<TSyncLogRescueWriteGateE2EActor> {
    TConfiguration *Conf;
    TIntrusivePtr<TVDiskConfig> VDiskConfig;
    TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
    TActorId RealPDiskId;
    TActorId ObservedPDiskId;
    TActorId VDiskActorId;
    std::shared_ptr<TRescueWriteGateState> RescueGateState = std::make_shared<TRescueWriteGateState>();
    TRescueHttpRequestMock HttpRequest;
    std::unique_ptr<NMonitoring::TMonService2HttpRequest> MonRequest;

    TVDiskID SelfVDiskId;
    ui64 FreeUpToLsn = 0;
    enum class EWakeupTag : ui64 {
        WaitReadyTimeout = 1,
        WaitSyncLogStatusTimeout = 2,
        WaitBlockedSilence = 3,
        AllowOneHttpTimeout = 4,
        AllowedCommitTimeout = 5,
        NoSecondWrite = 6,
    };

    friend class TActorBootstrapped<TSyncLogRescueWriteGateE2EActor>;

    void Bootstrap(const TActorContext &ctx) {
        STR << "RUN SYNCLOG RESCUE WRITE GATE E2E TEST\n";
        auto &vDiskInstance = Conf->VDisks->Get(0);
        const auto& sourceConfig = *vDiskInstance.Cfg;
        const auto& oldBaseInfo = sourceConfig.BaseInfo;
        RealPDiskId = oldBaseInfo.PDiskActorID;
        ObservedPDiskId = ctx.Register(new TRescueObservedPDiskProxyActor(
                    RealPDiskId,
                    ctx.SelfID,
                    RescueGateState));

        TVDiskConfig::TBaseInfo baseInfo(
            oldBaseInfo.VDiskIdShort,
            ObservedPDiskId,
            oldBaseInfo.PDiskGuid,
            oldBaseInfo.PDiskId,
            oldBaseInfo.DeviceType,
            oldBaseInfo.VDiskSlotId,
            oldBaseInfo.Kind,
            oldBaseInfo.InitOwnerRound,
            oldBaseInfo.StoragePoolName,
            oldBaseInfo.DonorMode,
            oldBaseInfo.DonorDiskIds,
            oldBaseInfo.ScrubCookie,
            oldBaseInfo.WhiteboardInstanceGuid,
            oldBaseInfo.ReadOnly);
        VDiskConfig = MakeIntrusive<TVDiskConfig>(baseInfo);
        // The rescue config is rebuilt only to swap PDisk actor id; keep the relevant knobs from UT setup.
        VDiskConfig->MaxLogoBlobDataSize = sourceConfig.MaxLogoBlobDataSize;
        VDiskConfig->MinHugeBlobInBytes = sourceConfig.MinHugeBlobInBytes;
        VDiskConfig->MilestoneHugeBlobInBytes = sourceConfig.MilestoneHugeBlobInBytes;
        VDiskConfig->UseCostTracker = sourceConfig.UseCostTracker;
        VDiskConfig->CostMetricsParametersByMedia = sourceConfig.CostMetricsParametersByMedia;
        VDiskConfig->ForceLogRescueMode = true;
        VDiskConfig->RunSyncer = true;
        VDiskConfig->RunRepl = true;
        VDiskConfig->RecoveryLogCutterFirstDuration = TDuration::MilliSeconds(100);
        VDiskConfig->RecoveryLogCutterRegularDuration = TDuration::MilliSeconds(100);
        VDiskConfig->AdvanceEntryPointTimeout = TDuration::MilliSeconds(100);

        GroupInfo = Conf->GroupInfo;
        SelfVDiskId = GroupInfo->GetVDiskId(VDiskConfig->BaseInfo.VDiskIdShort);
        VDiskActorId = ctx.Register(CreateVDisk(VDiskConfig, GroupInfo, Conf->Counters));

        SendReadyProbe(ctx);
        Become(&TThis::StateWaitReady);
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup(ui64(EWakeupTag::WaitReadyTimeout)));
    }

    void SendReadyProbe(const TActorContext &ctx) {
        TLogoBlobID id(0, 1, 1, 0, 0, 0);
        auto req = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(SelfVDiskId, TInstant::Max(),
            NKikimrBlobStorage::EGetHandleClass::FastRead,
            TEvBlobStorage::TEvVGet::EFlags::NotifyIfNotReady, {}, {id});
        ctx.Send(VDiskActorId, req.release());
    }

    void HandleReadyProbe(TEvBlobStorage::TEvVGetResult::TPtr& ev, const TActorContext &ctx) {
        if (ev->Get()->Record.GetStatus() != NKikimrProto::NOTREADY) {
            RequestSyncLogStatus(ctx);
        }
    }

    void HandleReadyNotify(TEvBlobStorage::TEvVReadyNotify::TPtr&, const TActorContext &ctx) {
        RequestSyncLogStatus(ctx);
    }

    void RequestSyncLogStatus(const TActorContext &ctx) {
        ctx.Send(VDiskActorId, new TEvBlobStorage::TEvVStatus(SelfVDiskId));
        Become(&TThis::StateWaitSyncLogStatus);
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup(ui64(EWakeupTag::WaitSyncLogStatusTimeout)));
    }

    void HandleSyncLogStatus(TEvBlobStorage::TEvVStatusResult::TPtr& ev, const TActorContext &ctx) {
        const NKikimrBlobStorage::TEvVStatusResult& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.GetStatus() == NKikimrProto::OK,
            "rescue VDisk returned unexpected status; Status# %s",
            NKikimrProto::EReplyStatus_Name(record.GetStatus()).data());

        const NKikimrBlobStorage::TSyncLogStatus& syncLogStatus = record.GetSyncLogStatus();
        Y_ABORT_UNLESS(!syncLogStatus.GetMemLogEmpty(),
            "rescue test expected recovered SyncLog memory tail; SyncLogStatus# %s",
            syncLogStatus.ShortDebugString().data());
        Y_ABORT_UNLESS(syncLogStatus.GetLastMemLsn() > syncLogStatus.GetFirstMemLsn(),
            "rescue test needs a non-trivial recovered SyncLog memory tail; SyncLogStatus# %s",
            syncLogStatus.ShortDebugString().data());

        Y_ABORT_UNLESS(syncLogStatus.GetLastMemLsn() != Max<ui64>(),
            "rescue test cannot build a cut request above max SyncLog memory LSN; SyncLogStatus# %s",
            syncLogStatus.ShortDebugString().data());
        FreeUpToLsn = syncLogStatus.GetLastMemLsn() + 1;
        WaitForBlockedSilence(ctx);
    }

    void WaitForBlockedSilence(const TActorContext &ctx) {
        Y_ABORT_UNLESS(RescueGateState->Owner.load());
        Become(&TThis::StateWaitBlocked);
        ctx.Schedule(TDuration::MilliSeconds(500), new TEvents::TEvWakeup(ui64(EWakeupTag::WaitBlockedSilence)));
    }

    void SendAllowOneHttp(const TActorContext &ctx) {
        HttpRequest.CgiParameters.clear();
        HttpRequest.CgiParameters.emplace("type", "logrescuewriteonce");
        MonRequest = std::make_unique<NMonitoring::TMonService2HttpRequest>(
            nullptr, &HttpRequest, nullptr, nullptr, "", nullptr);
        ctx.Send(VDiskActorId, new NMon::TEvHttpInfo(*MonRequest));
    }

    void SendCutLog(const TActorContext &ctx, ui64 freeUpToLsn) {
        const ui32 owner = RescueGateState->Owner.load();
        const ui64 ownerRound = RescueGateState->OwnerRound.load();
        Y_ABORT_UNLESS(owner);
        auto ev = std::make_unique<IEventHandle>(VDiskActorId, ctx.SelfID,
            new NPDisk::TEvCutLog(NPDisk::TOwner(owner), ownerRound, freeUpToLsn, 0, 0, 0, 0));
        ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, VDiskActorId);
        ctx.Send(ev.release());
    }

    void AllowOneRescueCommit(const TActorContext &ctx) {
        SendAllowOneHttp(ctx);
        Become(&TThis::StateWaitAllowOneHttp);
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup(ui64(EWakeupTag::AllowOneHttpTimeout)));
    }

    void HandleAllowOneHttp(NMon::TEvHttpInfoRes::TPtr&, const TActorContext &ctx) {
        MonRequest.reset();
        RescueGateState->ExpectOneRescueCycle.store(true);
        SendCutLog(ctx, FreeUpToLsn);
        Become(&TThis::StateWaitAllowedCommit);
        ctx.Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup(ui64(EWakeupTag::AllowedCommitTimeout)));
    }

    void HandleBlockedTimeout(const TActorContext &ctx) {
        AllowOneRescueCommit(ctx);
    }

    void HandleAllowedCommit(TEvents::TEvCompleted::TPtr&, const TActorContext &ctx) {
        Y_ABORT_UNLESS(!RescueGateState->ExpectOneRescueCycle.load());

        SendCutLog(ctx, FreeUpToLsn);

        Become(&TThis::StateWaitNoSecondWrite);
        ctx.Schedule(TDuration::MilliSeconds(500), new TEvents::TEvWakeup(ui64(EWakeupTag::NoSecondWrite)));
    }

    void HandleWaitReadyWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext&) {
        if (ev->Get()->Tag == ui64(EWakeupTag::WaitReadyTimeout)) {
            Y_ABORT("rescue VDisk did not become ready before timeout; Owner# %" PRIu32
                " OwnerRound# %" PRIu64 " ExpectOneRescueCycle# %s",
                RescueGateState->Owner.load(), RescueGateState->OwnerRound.load(),
                RescueGateState->ExpectOneRescueCycle.load() ? "true" : "false");
        }
    }

    void HandleSyncLogStatusWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext&) {
        if (ev->Get()->Tag == ui64(EWakeupTag::WaitSyncLogStatusTimeout)) {
            Y_ABORT("rescue VDisk did not return SyncLog status before timeout; Owner# %" PRIu32
                " OwnerRound# %" PRIu64 " ExpectOneRescueCycle# %s",
                RescueGateState->Owner.load(), RescueGateState->OwnerRound.load(),
                RescueGateState->ExpectOneRescueCycle.load() ? "true" : "false");
        }
    }

    void HandleBlockedWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext &ctx) {
        if (ev->Get()->Tag == ui64(EWakeupTag::WaitBlockedSilence)) {
            HandleBlockedTimeout(ctx);
        }
    }

    void HandleAllowOneHttpWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext&) {
        if (ev->Get()->Tag == ui64(EWakeupTag::AllowOneHttpTimeout)) {
            Y_ABORT("rescue VDisk did not respond to one-shot HTTP request before timeout; Owner# %" PRIu32
                " OwnerRound# %" PRIu64 " ExpectOneRescueCycle# %s",
                RescueGateState->Owner.load(), RescueGateState->OwnerRound.load(),
                RescueGateState->ExpectOneRescueCycle.load() ? "true" : "false");
        }
    }

    void HandleAllowedCommitWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext&) {
        if (ev->Get()->Tag == ui64(EWakeupTag::AllowedCommitTimeout)) {
            Y_ABORT("VDisk did not issue the allowed rescue checkpoint/cut cycle before timeout; Owner# %" PRIu32
                " OwnerRound# %" PRIu64 " ExpectOneRescueCycle# %s",
                RescueGateState->Owner.load(), RescueGateState->OwnerRound.load(),
                RescueGateState->ExpectOneRescueCycle.load() ? "true" : "false");
        }
    }

    void HandleNoSecondWriteWakeup(TEvents::TEvWakeup::TPtr& ev, const TActorContext &ctx) {
        if (ev->Get()->Tag == ui64(EWakeupTag::NoSecondWrite)) {
            HandleNoSecondWriteTimeout(ctx);
        }
    }

    void HandleNoSecondWriteTimeout(const TActorContext &ctx) {
        Finish(ctx);
    }

    void Finish(const TActorContext &ctx) {
        AtomicIncrement(Conf->SuccessCount);
        Conf->SignalDoneEvent();
        Die(ctx);
    }

    STRICT_STFUNC(StateWaitReady,
        HFunc(TEvBlobStorage::TEvVGetResult, HandleReadyProbe);
        HFunc(TEvBlobStorage::TEvVReadyNotify, HandleReadyNotify);
        HFunc(TEvents::TEvWakeup, HandleWaitReadyWakeup);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

    STRICT_STFUNC(StateWaitSyncLogStatus,
        HFunc(TEvBlobStorage::TEvVStatusResult, HandleSyncLogStatus);
        HFunc(TEvents::TEvWakeup, HandleSyncLogStatusWakeup);
        IgnoreFunc(TEvBlobStorage::TEvVGetResult);
        IgnoreFunc(TEvBlobStorage::TEvVReadyNotify);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

    STRICT_STFUNC(StateWaitBlocked,
        HFunc(TEvents::TEvWakeup, HandleBlockedWakeup);
    )

    STRICT_STFUNC(StateWaitAllowOneHttp,
        HFunc(NMon::TEvHttpInfoRes, HandleAllowOneHttp);
        HFunc(TEvents::TEvWakeup, HandleAllowOneHttpWakeup);
    )

    STRICT_STFUNC(StateWaitAllowedCommit,
        HFunc(TEvents::TEvCompleted, HandleAllowedCommit);
        HFunc(TEvents::TEvWakeup, HandleAllowedCommitWakeup);
    )

    STRICT_STFUNC(StateWaitNoSecondWrite,
        HFunc(TEvents::TEvWakeup, HandleNoSecondWriteWakeup);
    )

public:
    TSyncLogRescueWriteGateE2EActor(TConfiguration *conf)
        : TActorBootstrapped<TSyncLogRescueWriteGateE2EActor>()
        , Conf(conf)
    {}
};

void TSyncLogRescueWriteGateE2E::operator()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TSyncLogRescueWriteGateE2EActor(conf));
}
