#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>

Y_UNIT_TEST_SUITE(PhantomFlagStorageFullSync) {

    // A full sync with the unordered data protocol ends with the phantom flags stage, which asks the VDisk's phantom
    // flag storage for its flags. With the persistent storage turned on and the chunk keeper turned off, the storage
    // cannot keep its chunks. Full syncs served by such a VDisk must still finish: here a VDisk that lost its data
    // recovers it by full syncs from its peers.
    Y_UNIT_TEST(FullSyncFinishesWithoutChunkKeeper) {
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .EnablePhantomFlagStorage = true,
            .EnableChunkKeeper = false,
            .EnablePersistentPhantomFlagStorage = true,
        });
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));
        const ui32 groupId = env.GetGroups().front();
        const auto info = env.GetGroupInfo(groupId);
        for (ui32 step = 1; step <= 10; ++step) {
            const TString data = TStringBuilder() << "blob " << step;
            env.PutBlob(groupId, TLogoBlobID(1, 1, step, 0, data.size(), 0), data);
        }
        env.Sim(TDuration::Seconds(10));

        const TVDiskIdShort self(info->GetVDiskId(1)); // the VDisk that loses its data
        const ui32 selfNode = info->GetActorId(1).NodeId();

        ui32 unorderedAnswers = 0;
        ui32 finishedAnswers = 0;
        bool recovered = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvVSyncFullResult:
                    if (nodeId == selfNode) {
                        const auto& record = ev->Get<TEvBlobStorage::TEvVSyncFullResult>()->Record;
                        if (record.GetProtocol() == NKikimrBlobStorage::EFullSyncProtocol::UnorderedData) {
                            ++unorderedAnswers;
                        }
                        if (record.GetFinished()) {
                            ++finishedAnswers;
                        }
                    }
                    break;

                case TEvBlobStorage::EvSyncerLostDataRecovered:
                    if (nodeId == selfNode) {
                        recovered = true;
                    }
                    break;
            }
            return true;
        };

        // the VDisk loses its data and gets a new incarnation guid
        const auto baseConfig = env.FetchBaseConfig();
        bool wiped = false;
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (vslot.GetGroupId() == groupId && vslot.GetFailRealmIdx() == self.FailRealm &&
                    vslot.GetFailDomainIdx() == self.FailDomain && vslot.GetVDiskIdx() == self.VDisk) {
                const auto& v = vslot.GetVSlotId();
                env.Wipe(v.GetNodeId(), v.GetPDiskId(), v.GetVSlotId(), TVDiskID(groupId,
                    vslot.GetGroupGeneration(), self.FailRealm, self.FailDomain, self.VDisk));
                wiped = true;
            }
        }
        UNIT_ASSERT(wiped);

        for (ui32 i = 0; i != 1200 && !recovered; ++i) {
            env.Sim(TDuration::MilliSeconds(100));
        }
        env.Runtime->FilterFunction = {};
        Cerr << "unorderedAnswers# " << unorderedAnswers << " finishedAnswers# " << finishedAnswers
            << " recovered# " << recovered << Endl;
        UNIT_ASSERT_C(unorderedAnswers, "setup: the peers must serve the full syncs with the unordered data protocol");
        UNIT_ASSERT_C(recovered, "The lost data recovery must finish");
    }

}
