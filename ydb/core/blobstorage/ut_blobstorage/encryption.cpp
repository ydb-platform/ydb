#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

Y_UNIT_TEST_SUITE(ProxyEncryption) {
    Y_UNIT_TEST(CorrectlyFailOnNoKeys) {
        const ui8 keyData[32] = "Hello, I'm your new key";
        const TEncryptionKey key{
            .Key = {keyData, sizeof(keyData)},
            .Version = 1,
            .Id = "tenant key"
        };

        const ui8 key2Data[32] = "Hello! I'm your new key";
        const TEncryptionKey key2{
            .Key = {key2Data, sizeof(key2Data)},
            .Version = 2,
            .Id = "tenant key"
        };

        auto preprocess = [&](ui32 nodeId, TNodeWardenConfig& config) {
            if (nodeId == 9) {
                UNIT_ASSERT(config.TenantKeys.Init(&key, &key + 1));
            } else if (nodeId == 10) {
                auto keys = {key, key2};
                UNIT_ASSERT(config.TenantKeys.Init(std::begin(keys), std::end(keys)));
                UNIT_ASSERT_VALUES_EQUAL(config.TenantKeys.GetCurrentEncryptionKey().Id, "tenant key");
            }
        };
        TEnvironmentSetup env(TEnvironmentSetup::TSettings{
            .NodeCount = 11,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
            .Encryption = true,
            .ConfigPreprocessor = preprocess
        });
        auto& runtime = env.Runtime;
        env.CreateBoxAndPool(1, 2);
        env.Sim(TDuration::Minutes(1));

        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 2);

        TString data = "hello";
        TLogoBlobID id(2, 1, 1, 0, data.size(), 0);

        // node with keys: write data
        auto write = [&](ui32 nodeId, ui32 group, const TLogoBlobID& id, const TString& data) {
            TActorId edge = runtime->AllocateEdgeActor(nodeId);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, group, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        };

        write(9, groups[0], id, data); // write with key version 1

        auto tryToRead = [&](ui32 nodeId, ui32 group, const TLogoBlobID& id) {
            TActorId edge = runtime->AllocateEdgeActor(nodeId);
            runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, group, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge);
            auto *msg = res->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 1);
            return msg->Responses[0].Status;
        };

        for (ui32 i = 1; i <= 11; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(tryToRead(i, groups[0], id), (i == 9 || i == 10) ? NKikimrProto::OK : NKikimrProto::ERROR, i);
        }

        TLogoBlobID id2(1, 2, 2, 0, data.size(), 0);
        write(10, groups[1], id2, data); // write with key version 2

        for (ui32 i = 1; i <= 11; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(tryToRead(i, groups[1], id2), (i == 10) ? NKikimrProto::OK : NKikimrProto::ERROR, i);
        }
    }
}
