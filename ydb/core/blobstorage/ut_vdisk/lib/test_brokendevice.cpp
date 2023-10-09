#include "test_brokendevice.h"
#include "helpers.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>

using namespace NKikimr;

#define STR Cnull


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TWriteUntilDeviceDeathActor : public TActorBootstrapped<TWriteUntilDeviceDeathActor> {
protected:
    class TSender {
    private:
        const TAllVDisks::TVDiskInstance VDiskInfo;
        IDataSetPtr DataSetPtr;
        const TActorId PDiskId;
        TAutoPtr<IDataSet::TIterator> It;

        void Send(const TActorContext &ctx) {
            Y_ABORT_UNLESS(It->IsValid());
            const auto &x = *It->Get();
            PutLogoBlobToVDisk(ctx, VDiskInfo.ActorID, VDiskInfo.VDiskID, x.Id, x.Data, x.HandleClass);
        }

    public:
        TSender(const TAllVDisks::TVDiskInstance vdiskInfo, IDataSetPtr dataSetPtr, const TActorId &pdiskId)
            : VDiskInfo(vdiskInfo)
            , DataSetPtr(dataSetPtr)
            , PDiskId(pdiskId)
            , It(DataSetPtr->First())
        {}

        void GoodState_Put(const TActorContext &ctx) {
            Send(ctx);
            It->Next();
        }

        void GoodState_BrakeDevice(const TActorContext &ctx) {
            Y_UNUSED(ctx);
            ctx.Send(PDiskId, new NPDisk::TEvYardControl(NPDisk::TEvYardControl::Brake, nullptr));
        }

        void BrokenState_Put(const TActorContext &ctx) {
            Send(ctx);
            ctx.Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup());
        }
    };


    TConfiguration *Conf;
    TSender Sender;

private:
    friend class TActorBootstrapped<TWriteUntilDeviceDeathActor>;

    void Bootstrap(const TActorContext &ctx) {
        STR << "GoodState_Put\n";
        Sender.GoodState_Put(ctx);
        Become(&TThis::GoodState);
    }

    void GoodState_Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev, const TActorContext &ctx) {
        STR << "GoodState_Handle\n";
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK, "Status=%s",
               NKikimrProto::EReplyStatus_Name(ev->Get()->Record.GetStatus()).data());
        LOG_NOTICE(ctx, NActorsServices::TEST, "  TEvVPutResult succeded");

        Become(&TThis::BrokenState);
        STR << "GoodState_BrakeDevice\n";
        Sender.GoodState_BrakeDevice(ctx);
        STR << "GoodState_Put\n";
        Sender.BrokenState_Put(ctx);
    }

    void BrokenState_Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev, const TActorContext &ctx) {
        STR << "BrokenState_Handle\n";

        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::VDISK_ERROR_STATE, "Status=%s",
               NKikimrProto::EReplyStatus_Name(ev->Get()->Record.GetStatus()).data());
        LOG_NOTICE(ctx, NActorsServices::TEST, "  TEvVPutResult succeded");

        STR << "BrokenState_Finish\n";
        Finish(ctx);
    }

    void BrokenState_Timeout(const TActorContext &ctx) {
        STR << "BrokenState_Timeout\n";
        Sender.BrokenState_Put(ctx);
    }

    void Finish(const TActorContext &ctx) {
        AtomicIncrement(Conf->SuccessCount);
        Conf->SignalDoneEvent();
        Die(ctx);
    }

    STRICT_STFUNC(GoodState,
        HFunc(TEvBlobStorage::TEvVPutResult, GoodState_Handle);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
    )

    STRICT_STFUNC(BrokenState,
        HFunc(TEvBlobStorage::TEvVPutResult, BrokenState_Handle);
        CFunc(TEvents::TSystem::Wakeup, BrokenState_Timeout);
        IgnoreFunc(TEvBlobStorage::TEvVWindowChange);
        IgnoreFunc(NPDisk::TEvYardControlResult);
        IgnoreFunc(TEvents::TEvUndelivered);
    )

public:
    TWriteUntilDeviceDeathActor(TConfiguration *conf)
        : TActorBootstrapped<TWriteUntilDeviceDeathActor>()
        , Conf(conf)
        , Sender(
            Conf->VDisks->Get(0),
            new T3PutDataSet(NKikimrBlobStorage::EPutHandleClass::TabletLog, 64<<10, false),
            Conf->PDisks->Get(1).PDiskActorID)
    {}
};


using namespace NKikimr;
void TWriteUntilDeviceDeath::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TWriteUntilDeviceDeathActor(conf));
}

