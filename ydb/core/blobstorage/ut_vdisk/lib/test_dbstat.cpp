#include "test_dbstat.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <util/stream/null.h>

using namespace NKikimr;

#define STR Cnull

///////////////////////////////////////////////////////////////////////////
class TDbStatTestActor : public NActors::TActorBootstrapped<TDbStatTestActor> {
    TConfiguration *Conf;
    NKikimrBlobStorage::EDbStatAction StatAction = NKikimrBlobStorage::DumpDb;
    const NKikimrBlobStorage::EDbStatType StatType = NKikimrBlobStorage::StatLogoBlobs;
    const ui64 TabletId;

    friend class NActors::TActorBootstrapped<TDbStatTestActor>;

    void Bootstrap(const NActors::TActorContext &ctx) {
        Become(&TThis::StateFunc);
        auto &instance = Conf->VDisks->Get(0);
        auto action = NKikimrBlobStorage::StatDb;
        // could be 3 different databases
        auto type = NKikimrBlobStorage::StatLogoBlobs;
        ctx.Send(instance.ActorID,
                 new TEvBlobStorage::TEvVDbStat(instance.VDiskID, action, type, true));
    }

    void Send_DumpDb(const TActorContext &ctx) {
        StatAction = NKikimrBlobStorage::DumpDb;
        auto &instance = Conf->VDisks->Get(0);
        ctx.Send(instance.ActorID,
                 new TEvBlobStorage::TEvVDbStat(instance.VDiskID, StatAction, StatType, true));
    }

    void Send_StatDb(const TActorContext &ctx) {
        StatAction = NKikimrBlobStorage::StatDb;
        auto &instance = Conf->VDisks->Get(0);
        ctx.Send(instance.ActorID,
                 new TEvBlobStorage::TEvVDbStat(instance.VDiskID, StatAction, StatType, true));
    }

    void Send_StatTabletAction(const TActorContext &ctx) {
        StatAction = NKikimrBlobStorage::StatTabletAction;
        auto &instance = Conf->VDisks->Get(0);
        ctx.Send(instance.ActorID,
                 new TEvBlobStorage::TEvVDbStat(instance.VDiskID, TabletId, true));
    }

    void Send_StatHugeAction(const TActorContext &ctx) {
        StatAction = NKikimrBlobStorage::StatHugeAction;
        auto &instance = Conf->VDisks->Get(0);
        ctx.Send(instance.ActorID,
                 new TEvBlobStorage::TEvVDbStat(instance.VDiskID, StatAction, NKikimrBlobStorage::StatHugeType, true));
    }

    void Handle(TEvBlobStorage::TEvVDbStatResult::TPtr &ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::OK);

        switch (StatAction) {
            case NKikimrBlobStorage::DumpDb:
                STR << "DumpDb DONE\n";
                Send_StatDb(ctx);
                break;
            case NKikimrBlobStorage::StatDb:
                STR << "StatDb DONE\n";
                Send_StatTabletAction(ctx);
                break;
            case NKikimrBlobStorage::StatTabletAction:
                STR << "StatTabletAction DONE\n";
                Send_StatHugeAction(ctx);
                break;
            case NKikimrBlobStorage::StatHugeAction:
                STR << "StatHugeAction DONE\n";
                Finish(ctx);
                break;
        }
    }

    void Finish(const TActorContext &ctx) {
        AtomicIncrement(Conf->SuccessCount);
        Conf->SignalDoneEvent();
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVDbStatResult, Handle);
    )

public:
    TDbStatTestActor(TConfiguration *conf, ui64 tabletId)
        : TActorBootstrapped<TDbStatTestActor>()
        , Conf(conf)
        , TabletId(tabletId)
    {}
};

void TDbStatTest::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TDbStatTestActor(conf, TabletId));
}

