#include "test_load.h"
#include "helpers.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

/////////////////////////////////////////////////////////////////////////////////////////////
// TAllVDisksParallelWriteActor
/////////////////////////////////////////////////////////////////////////////////////////////
class TAllVDisksParallelWriteActor : public TSyncTestBase {
protected:
    virtual void Scenario(const TActorContext &ctx) {
        // load data
        const ui64 maxDataSize = 300ull << 30ull;
        const ui32 maxBlobs = -1;
        const ui32 minBlobSize = 1u << 20u - 1;
        const ui32 maxBlobSize = 1u << 20u;

        TIntrusivePtr<TBlobStorageGroupInfo> ginfo = Conf->GroupInfo;
        TVector<TVDiskID> vdisks = Conf->VDisks->GetVDiskIds();
        TAutoPtr<IDataGenerator> generator;
        generator.Reset(CreateBlobGenerator(maxDataSize, maxBlobs, minBlobSize, maxBlobSize, 1, 1, ginfo,
                vdisks, true));
        TGeneratedDataSet dataSet(generator);

        TSyncRunner::TReturnValue ret;
        ret = SyncRunner->Run(ctx, ManyPutsToCorrespondingVDisks(SyncRunner->NotifyID(), Conf, &dataSet,
                              PDiskPutStatusHandlerDefault, 100));
        LOG_NOTICE(ctx, NActorsServices::TEST, "  Data is loaded");
    }

public:
    TAllVDisksParallelWriteActor(TConfiguration *conf)
        : TSyncTestBase(conf)
    {}
};

void TAllVDisksParallelWrite::operator ()(TConfiguration *conf) {
    conf->ActorSystem1->Register(new TAllVDisksParallelWriteActor(conf));
}

