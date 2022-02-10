#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(ProxyEncryption) {
    Y_UNIT_TEST(CorrectlyFailOnNoKeys) {
        const ui8 keyData[32] = "Hello, I'm your new key";
        const TEncryptionKey key{
            .Key = {keyData, sizeof(keyData)},
            .Version = 1,
            .Id = "tenant key"
        };
        auto preprocess = [&](ui32 nodeId, TNodeWardenConfig& config) {
            if (nodeId == 9 || nodeId == 10) {
                config.TenantKey = key;
            }
        };
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 11,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
            .Encryption = true,
            .ConfigPreprocessor = preprocess
        });
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool(1, 1);
        env.Sim(TDuration::Minutes(1));

        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);

        TString data = "hello";
        TLogoBlobID id(1, 1, 1, 0, data.size(), 0);

        // node with keys: write data
        {
            TActorId edge = runtime->AllocateEdgeActor(9);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groups[0], new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        auto tryToRead = [&](ui32 nodeId) {
            TActorId edge = runtime->AllocateEdgeActor(nodeId);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groups[0], new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
            auto *msg = res->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 1);
            return msg->Responses[0].Status;
        };

        for (ui32 i = 1; i <= 11; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(tryToRead(i), (i == 9 || i == 10) ? NKikimrProto::OK : NKikimrProto::ERROR);
        }
    }
}
