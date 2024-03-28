#include "test_bad_blobid.h"
#include "helpers.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_pdiskctx.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>

using namespace NKikimr;

#define STR Cnull


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TWriteAndExpectErrorActor : public TActorBootstrapped<TWriteAndExpectErrorActor> {
protected:
    TConfiguration* Conf;
    const TAllVDisks::TVDiskInstance VDiskInfo;
    IDataSetPtr DataSetPtr;

public:
    TWriteAndExpectErrorActor(TConfiguration *conf)
        : TActorBootstrapped<TWriteAndExpectErrorActor>()
        , Conf(conf)
        , VDiskInfo(conf->VDisks->Get(0))
        , DataSetPtr(new TBadIdsDataSet(NKikimrBlobStorage::EPutHandleClass::TabletLog, 64<<10, false))
    {}

    void Bootstrap(const TActorContext &ctx) {
        for (auto it = DataSetPtr->First(); it->IsValid(); it->Next()) {
            const auto &x = *it->Get();
            ctx.Send(VDiskInfo.ActorID, new TEvBlobStorage::TEvVPut(x.Id, TRope(x.Data),
                    VDiskInfo.VDiskID, false, nullptr, TInstant::Max(), x.HandleClass));
        }
        Become(&TThis::StateFunc);
    }

private:
    void Handle(TEvBlobStorage::TEvVPutResult::TPtr &ev, const TActorContext &ctx) {
        Y_ABORT_UNLESS(ev->Get()->Record.GetStatus() == NKikimrProto::ERROR, "Status=%s",
               NKikimrProto::EReplyStatus_Name(ev->Get()->Record.GetStatus()).data());
        LOG_NOTICE(ctx, NActorsServices::TEST, " TEvVPut failed successfully");
        AtomicIncrement(Conf->SuccessCount);
        Conf->SignalDoneEvent();
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvBlobStorage::TEvVPutResult, Handle);
    )
};


using namespace NKikimr;
void TWriteAndExpectError::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TWriteAndExpectErrorActor(conf));
}

