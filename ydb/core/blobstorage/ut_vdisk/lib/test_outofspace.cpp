#include "test_outofspace.h"
#include "helpers.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteUntilOrangeZoneActor
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TWriteUntilOrangeZoneActor : public TSyncTestBase {
protected:
    virtual void Scenario(const TActorContext &ctx) {
        // load data
        const ui64 maxDataSize = 1 << 30;
        const ui32 maxBlobs = -1;
        const ui32 minBlobSize = 128;
        const ui32 maxBlobSize = 4096;

        TIntrusivePtr<TBlobStorageGroupInfo> ginfo = Conf->GroupInfo;
        TVector<TVDiskID> vdisks = Conf->VDisks->GetVDiskIds();
        TAutoPtr<IDataGenerator> generator;
        generator.Reset(CreateBlobGenerator(maxDataSize, maxBlobs, minBlobSize, maxBlobSize, 0, 1, ginfo, vdisks));
        TGeneratedDataSet dataSet(generator);

        TSyncRunner::TReturnValue ret;
        TPDiskPutStatusHandler hndl = PDiskPutStatusHandlerErrorAware;
        ret = SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &dataSet, hndl));
        UNIT_ASSERT_VALUES_EQUAL(ret.Id, 0);
        UNIT_ASSERT_EQUAL(ret.Status, NKikimrProto::OUT_OF_SPACE);
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded until ORANGE ZONE");
    }

public:
    TWriteUntilOrangeZoneActor(TConfiguration *conf)
        : TSyncTestBase(conf)
    {}
};

void TWriteUntilOrangeZone::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TWriteUntilOrangeZoneActor(conf));
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TWriteUntilYellowZoneActor
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TWriteUntilYellowZoneActor : public TSyncTestBase {
protected:
    virtual void Scenario(const TActorContext &ctx) {
        // load data
        const ui64 maxDataSize = 1 << 30;
        const ui32 maxBlobs = -1;
        const ui32 minBlobSize = 128;
        const ui32 maxBlobSize = 4096;

        TIntrusivePtr<TBlobStorageGroupInfo> ginfo = Conf->GroupInfo;
        TVector<TVDiskID> vdisks = Conf->VDisks->GetVDiskIds();
        TAutoPtr<IDataGenerator> generator;
        generator.Reset(CreateBlobGenerator(maxDataSize, maxBlobs, minBlobSize, maxBlobSize, 0, 1, ginfo, vdisks));
        TGeneratedDataSet dataSet(generator);

        TSyncRunner::TReturnValue ret;
        TPDiskPutStatusHandler hndl = PDiskPutStatusHandlerYellowMoveZone;
        ret = SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &dataSet, hndl));
        UNIT_ASSERT_VALUES_EQUAL(ret.Id, 0);
        UNIT_ASSERT_VALUES_EQUAL(ret.Status, 0x28733642);
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded until YELLOW ZONE");
    }

public:
    TWriteUntilYellowZoneActor(TConfiguration *conf)
        : TSyncTestBase(conf)
    {}
};

void TWriteUntilYellowZone::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TWriteUntilYellowZoneActor(conf));
}
