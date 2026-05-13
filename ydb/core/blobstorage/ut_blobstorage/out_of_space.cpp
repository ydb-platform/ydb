#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/util/lz4_data_generator.h>

Y_UNIT_TEST_SUITE(OutOfSpace) {

    Y_UNIT_TEST(HugeBlobWriteError) {
        TEnvironmentSetup env{{
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .PDiskSize = 2_GB,
        }};
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1, 1, NKikimrBlobStorage::EPDiskType::NVME, std::nullopt);
        env.Sim(TDuration::Seconds(30));

        const ui32 groupId = env.GetGroups().front();

        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        size_t size = 65536;
        size_t index = 0;
        std::vector<TLogoBlobID> success;
        while (size <= 10_MB) {
            Cerr << size << Endl;
            TString buffer = FastGenDataForLZ4(size);
            TLogoBlobID id(1, 1, 1, 0, size, 0);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            if (index < 17) {
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                success.push_back(id);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::ERROR);
            }
            size = size * 9 / 8;
            ++index;
        }
        for (const TLogoBlobID& id : success) {
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
            UNIT_ASSERT_EQUAL(res->Get()->Responses[0].Buffer.ConvertToString(), FastGenDataForLZ4(id.BlobSize()));
        }
    }

}
