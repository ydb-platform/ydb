#include "test_huge.h"
#include "prepare.h"
#include "helpers.h"

#include <ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_recoverylogwriter.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_lsnmngr.h>

using namespace NKikimr;
using namespace NKikimr::NHuge;

#define STR     Cerr


class IScenary {
public:
    // returns true if finished
    virtual bool GenerateTask(const TActorContext &ctx) = 0;
    virtual void Handle(TEvHullLogHugeBlob::TPtr &ev, const TActorContext &ctx) = 0;
    virtual void SetHugeKeeperId(const TActorId &hugeKeeperId) {
        HugeKeeperId = hugeKeeperId;
    }

    IScenary(const TAllVDisks::TVDiskInstance &vDiskInstance)
        : VDiskInstance(vDiskInstance)
        , HugeKeeperId()
    {}

    virtual ~IScenary() {
    }

protected:
    const TAllVDisks::TVDiskInstance &VDiskInstance;
    TActorId HugeKeeperId;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSimpleScenary
/////////////////////////////////////////////////////////////////////////////////////////////////////////
class TSimpleScenary : public IScenary {
    int State;

public:
    TSimpleScenary(const TAllVDisks::TVDiskInstance &vDiskInstance)
        : IScenary(vDiskInstance)
        , State(0)
    {}

    // returns true if finished
    bool GenerateTask(const TActorContext &ctx) override {
        switch (State) {
            case 0: {
                const ui32 minHugeBlobSize = 65 << 10;
                TString abcdefghkj(CreateData("abcdefghkj", minHugeBlobSize, true));
                const TLogoBlobID logoBlobId(DefaultTestTabletId, 1, 10, 0, abcdefghkj.size(), 0, 1);
                ctx.Send(HugeKeeperId,
                         new TEvHullWriteHugeBlob(TActorId(), 0, logoBlobId, TIngress(),
                                TRope(abcdefghkj),
                                false, NKikimrBlobStorage::EPutHandleClass::AsyncBlob,
                                std::make_unique<TEvBlobStorage::TEvVPutResult>(), nullptr));
                State = 1;
                return false;
            }
            case 1:
                Y_ABORT();
            case 2:
                return true;
            default:
                Y_ABORT();
        }
    }

    void Handle(TEvHullLogHugeBlob::TPtr &ev, const TActorContext &ctx) override {
        Y_ABORT_UNLESS(State == 1);
        // FIXME: log

        const auto *msg = ev->Get();

        const bool slotIsUsed = true;
        const ui64 recLsn = 100500; // FIXME: write to log actually
        ctx.Send(HugeKeeperId, new TEvHullHugeBlobLogged(msg->WriteId, msg->HugeBlob, recLsn, slotIsUsed));
        State = 2;
    }
};


/////////////////////////////////////////////////////////////////////////////////////////////////////////
// THugeModuleContext
/////////////////////////////////////////////////////////////////////////////////////////////////////////
struct THugeModuleContext {
    TConfiguration *Conf;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TIntrusivePtr<TLsnMngr> LsnMngr;
    TIntrusivePtr<NKikimr::TVDiskConfig> Config;
    TVDiskContextPtr VCtx;
    TPDiskCtxPtr PDiskCtx;
    TActorId MainID;
    TActorId LoggerID;
    TActorId LogCutterID;
    TActorId HugeKeeperID;

    THugeModuleContext(TConfiguration *conf)
        : Conf(conf)
        , Counters(new ::NMonitoring::TDynamicCounters)
    {}
};


/////////////////////////////////////////////////////////////////////////////////////////////////////////
// THugeModuleRecoveryActor
/////////////////////////////////////////////////////////////////////////////////////////////////////////
class THugeModuleRecoveryActor : public TActorBootstrapped<THugeModuleRecoveryActor> {
    using TStartingPoints = TMap<TLogSignature, NPDisk::TLogRecord>;

    std::shared_ptr<THugeModuleContext> HmCtx;
    ui64 Lsn = 0;
    std::shared_ptr<THullHugeKeeperPersState> RepairedHuge;

    friend class TActorBootstrapped<THugeModuleRecoveryActor>;

    void Bootstrap(const TActorContext &ctx) {
        auto &vDiskInstance = HmCtx->Conf->VDisks->Get(0);
        HmCtx->Config = vDiskInstance.Cfg;
        HmCtx->VCtx.Reset(new TVDiskContext(ctx.SelfID, HmCtx->Conf->GroupInfo->PickTopology(), HmCtx->Counters,
                vDiskInstance.VDiskID, ctx.ExecutorThread.ActorSystem, NPDisk::DEVICE_TYPE_UNKNOWN));

        TVDiskID selfVDiskID = HmCtx->Conf->GroupInfo->GetVDiskId(HmCtx->VCtx->ShortSelfVDisk);
        ctx.Send(HmCtx->Config->BaseInfo.PDiskActorID,
            new NPDisk::TEvYardInit(HmCtx->Config->BaseInfo.InitOwnerRound + 50, selfVDiskID,
                HmCtx->Config->BaseInfo.PDiskGuid));
        Become(&TThis::StateFunc);
    }

    bool InitHugeBlobKeeper(const TActorContext &ctx, const TStartingPoints &startingPoints) {
        Y_UNUSED(ctx);
        const ui32 oldMinHugeBlobInBytes = 64 << 10;
        const ui32 milestoneHugeBlobInBytes = 64 << 10;
        const ui32 maxBlobInBytes = 128 << 10;
        auto logFunc = [] (const TString) { /* empty */ };

        TStartingPoints::const_iterator it;
        it = startingPoints.find(TLogSignature::SignatureHugeBlobEntryPoint);
        if (it == startingPoints.end()) {
            RepairedHuge = std::make_shared<THullHugeKeeperPersState>(
                        HmCtx->VCtx,
                        HmCtx->PDiskCtx->Dsk->ChunkSize,
                        HmCtx->PDiskCtx->Dsk->AppendBlockSize,
                        HmCtx->PDiskCtx->Dsk->AppendBlockSize,
                        oldMinHugeBlobInBytes,
                        milestoneHugeBlobInBytes,
                        maxBlobInBytes,
                        HmCtx->Config->HugeBlobOverhead,
                        HmCtx->Config->HugeBlobsFreeChunkReservation,
                        logFunc);
        } else {
            // read existing one
            const ui64 lsn = it->second.Lsn;
            const TRcBuf &entryPoint = it->second.Data;
            if (!THullHugeKeeperPersState::CheckEntryPoint(entryPoint)) {
                return false;
            }

            RepairedHuge = std::make_shared<THullHugeKeeperPersState>(
                        HmCtx->VCtx,
                        HmCtx->PDiskCtx->Dsk->ChunkSize,
                        HmCtx->PDiskCtx->Dsk->AppendBlockSize,
                        HmCtx->PDiskCtx->Dsk->AppendBlockSize,
                        oldMinHugeBlobInBytes,
                        milestoneHugeBlobInBytes,
                        maxBlobInBytes,
                        HmCtx->Config->HugeBlobOverhead,
                        HmCtx->Config->HugeBlobsFreeChunkReservation,
                        lsn, entryPoint, logFunc);
        }

        return true;
    }

    void Handle(NPDisk::TEvYardInitResult::TPtr &ev, const TActorContext &ctx) {
        const auto &m = ev->Get();
        NKikimrProto::EReplyStatus status = m->Status;
        Y_ABORT_UNLESS(status == NKikimrProto::OK, "Status# %s ErrorReason# %s",
                NKikimrProto::EReplyStatus_Name(status).c_str(), m->ErrorReason.c_str());
        HmCtx->PDiskCtx = std::make_shared<TPDiskCtx>(m->PDiskParams, HmCtx->Config->BaseInfo.PDiskActorID, TString());

        // prepare starting points
        const TStartingPoints &startingPoints = ev->Get()->StartingPoints;
        bool result = InitHugeBlobKeeper(ctx, startingPoints);
        Y_ABORT_UNLESS(result);

        // start reading log
        ctx.Send(HmCtx->PDiskCtx->PDiskId,
            new NPDisk::TEvReadLog(HmCtx->PDiskCtx->Dsk->Owner, HmCtx->PDiskCtx->Dsk->OwnerRound));
    }

    void Handle(NPDisk::TEvReadLogResult::TPtr &ev, const TActorContext &ctx) {
        const auto &m = ev->Get();
        NKikimrProto::EReplyStatus status = m->Status;
        Y_ABORT_UNLESS(status == NKikimrProto::OK, "Status# %s ErrorReason# %s",
                NKikimrProto::EReplyStatus_Name(status).c_str(), m->ErrorReason.c_str());
        if (m->Results) {
            const ui64 lsn = m->Results.back().Lsn;
            Y_ABORT_UNLESS(lsn > Lsn);
            Lsn = lsn;
        }

        if (!m->IsEndOfLog) {
            return (void)ctx.Send(HmCtx->PDiskCtx->PDiskId,
                new NPDisk::TEvReadLog(HmCtx->PDiskCtx->Dsk->Owner, HmCtx->PDiskCtx->Dsk->OwnerRound,
                        m->NextPosition));
        }

        Finish(ctx);
    }

    void Finish(const TActorContext &ctx) {
        HmCtx->LsnMngr = MakeIntrusive<TLsnMngr>(Lsn, Lsn, false);
        HmCtx->LoggerID = ctx.ExecutorThread.RegisterActor(CreateRecoveryLogWriter(
                        HmCtx->PDiskCtx->PDiskId,
                        ctx.SelfID,
                        HmCtx->PDiskCtx->Dsk->Owner,
                        HmCtx->PDiskCtx->Dsk->OwnerRound,
                        HmCtx->LsnMngr->GetLsn(),
                        HmCtx->Counters));
        TLogCutterCtx logCutterCtx = {HmCtx->VCtx, HmCtx->PDiskCtx, HmCtx->LsnMngr, HmCtx->Config, HmCtx->LoggerID};
        HmCtx->LogCutterID = ctx.ExecutorThread.RegisterActor(CreateRecoveryLogCutter(std::move(logCutterCtx)));
        RepairedHuge->FinishRecovery(ctx);
        auto hugeKeeperCtx = std::make_shared<THugeKeeperCtx>(HmCtx->VCtx, HmCtx->PDiskCtx, HmCtx->LsnMngr,
                HmCtx->MainID, HmCtx->LoggerID, HmCtx->LogCutterID, "{}", false);
        TAutoPtr<IActor> hugeKeeperActor(CreateHullHugeBlobKeeper(hugeKeeperCtx, RepairedHuge));
        HmCtx->HugeKeeperID = ctx.ExecutorThread.RegisterActor(hugeKeeperActor.Release());

        // report and die
        ctx.Send(HmCtx->MainID, new TEvents::TEvCompleted());
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(NPDisk::TEvYardInitResult, Handle);
        HFunc(NPDisk::TEvReadLogResult, Handle);
    )

public:
    THugeModuleRecoveryActor(std::shared_ptr<THugeModuleContext> hmCtx)
        : TActorBootstrapped<THugeModuleRecoveryActor>()
        , HmCtx(hmCtx)
    {}
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////
// THugeModuleTestActor
/////////////////////////////////////////////////////////////////////////////////////////////////////////
class THugeModuleTestActor : public TActorBootstrapped<THugeModuleTestActor> {
    std::shared_ptr<THugeModuleContext> HmCtx;
    TAutoPtr<IScenary> Scenary;

    friend class TActorBootstrapped<THugeModuleTestActor>;

    void Bootstrap(const TActorContext &ctx) {
        HmCtx->MainID = ctx.SelfID;
        ctx.Register(new THugeModuleRecoveryActor(HmCtx));
        Become(&TThis::StateWaitForRecovery);
    }

    void HandleRecoveryCompletion(TEvents::TEvCompleted::TPtr& ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        Become(&TThis::StateWorking);
        Scenary->SetHugeKeeperId(HmCtx->HugeKeeperID);
        Work(ctx);
    }

    void Work(const TActorContext &ctx) {
        bool finished = Scenary->GenerateTask(ctx);
        if (finished)
            Finish(ctx);
    }

    void Finish(const TActorContext &ctx) {
        AtomicIncrement(HmCtx->Conf->SuccessCount);
        HmCtx->Conf->SignalDoneEvent();
        Die(ctx);
    }

    void Handle(TEvHullLogHugeBlob::TPtr &ev, const TActorContext &ctx) {
        Scenary->Handle(ev, ctx);
        Work(ctx);
    }

    STRICT_STFUNC(StateWaitForRecovery,
        HFunc(TEvents::TEvCompleted, HandleRecoveryCompletion);
    )

    STRICT_STFUNC(StateWorking,
        HFunc(TEvHullLogHugeBlob, Handle);
    )

public:
    THugeModuleTestActor(TConfiguration *conf, TAutoPtr<IScenary> scenary)
        : TActorBootstrapped<THugeModuleTestActor>()
        , HmCtx(std::make_shared<THugeModuleContext>(conf))
        , Scenary(scenary)
    {}
};


void THugeModuleTest::operator()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new THugeModuleTestActor(conf, new TSimpleScenary(conf->VDisks->Get(0))));
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
