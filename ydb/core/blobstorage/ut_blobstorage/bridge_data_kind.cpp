#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>

namespace {

TNodeLocation GetLocation(ui32 nodeId) {
    NActorsInterconnect::TNodeLocation location;
    if (1 <= nodeId && nodeId <= 8) {
        location.SetBridgePileName("pile_1");
    } else if (9 <= nodeId && nodeId <= 16) {
        location.SetBridgePileName("pile_2");
    } else if (17 <= nodeId && nodeId <= 24) {
        location.SetBridgePileName("pile_3");
    } else {
        Y_ABORT();
    }
    location.SetDataCenter("my_dc");
    location.SetRack(TStringBuilder() << "rack_" << nodeId);
    location.SetUnit(TStringBuilder() << "unit_" << nodeId);
    return TNodeLocation(location);
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeDataKind) {

    Y_UNIT_TEST(StorageInfoVersion) {
        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();
        const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        auto block = [&](ui32 generation, ui32 version) {
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvBlock(100500, generation,
                    TInstant::Max(), 1, TWriteSource::Unknown, version));
            });
            return env.WaitForEdgeActorEvent<TEvBlobStorage::TEvBlockResult>(sender, false);
        };

        auto result = block(10, 1);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::OK);

        result = block(20, 0);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Status, NKikimrProto::ERROR);
        UNIT_ASSERT(result->Get()->IsTabletStorageInfoVersionObsolete);
    }

    Y_UNIT_TEST(EncryptionWorksWithInterpileTrafficOptimization) {
        TFeatureFlags featureFlags;
        featureFlags.SetEnableInterpileTrafficOptimization(true);

        const ui8 keyData[32] = "Hello, I'm your new key";
        const TEncryptionKey key{
            .Key = {keyData, sizeof(keyData)},
            .Version = 1,
            .Id = "tenant key",
        };

        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .Encryption = true,
            .ConfigPreprocessor = [key](ui32, TNodeWardenConfig& config) { config.TenantKey = key; },
            .LocationGenerator = GetLocation,
            .FeatureFlags = featureFlags,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();

        ui32 step = 1;
        auto put = [&](const TString& data) {
            const TLogoBlobID id(100500, 1, step++, 0, data.size(), 0);
            const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            return id;
        };

        put("warmup");
        env.Sim(TDuration::Seconds(5));

        size_t encryptedInterpilePuts = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvInterpilePut) {
                for (const auto& item : ev->Get<TEvInterpilePut>()->Record.GetItems()) {
                    encryptedInterpilePuts += item.GetAlreadyEncrypted();
                }
            }
            return true;
        };

        const TString data(1_MB + 1, 'x');
        const TLogoBlobID id = put(data);

        env.Runtime->FilterFunction = nullptr;

        const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Buffer.ConvertToString(), data);
        UNIT_ASSERT_C(encryptedInterpilePuts, "no encrypted write reached a remote pile the interpile way");
    }

    // With interpile traffic optimization the bridge proxy hands the blob over to a node inside the
    // remote pile instead of writing it across the link itself. That leg rebuilds the message and
    // then passes it through a protobuf, so the data kind has to survive both hops -- otherwise a
    // system tablet writing to a bridged group is admitted as user data in every pile but its own.
    Y_UNIT_TEST(SurvivesInterpileTrafficOptimization) {
        TFeatureFlags featureFlags;
        featureFlags.SetEnableInterpileTrafficOptimization(true);

        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetLocation,
            .FeatureFlags = featureFlags,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();

        ui32 step = 1;
        auto put = [&](NKikimrBlobStorage::TDataKind::E dataKind) {
            const TString data = "hello";
            const TLogoBlobID id(100500, 1, step++, 0, data.size(), 0);
            const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(TEvBlobStorage::TEvPut::TParameters{
                    .BlobId = id,
                    .Buffer = TRope(data),
                    .Deadline = TInstant::Max(),
                    .DataKind = dataKind,
                }));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        };

        // The interpile option is only taken once the queues towards the remote piles have
        // connected, so warm them up first -- otherwise the measured writes may quietly fall back
        // to going across the link and the test would prove nothing.
        put(NKikimrBlobStorage::TDataKind::USER);
        env.Sim(TDuration::Seconds(5));

        for (const auto dataKind : {NKikimrBlobStorage::TDataKind::USER, NKikimrBlobStorage::TDataKind::SYSTEM}) {
            std::vector<NKikimrBlobStorage::TDataKind::E> seen;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvInterpilePut) {
                    for (const auto& item : ev->Get<TEvInterpilePut>()->Record.GetItems()) {
                        seen.push_back(item.GetDataKind());
                    }
                }
                return true;
            };

            put(dataKind);

            env.Runtime->FilterFunction = nullptr;

            UNIT_ASSERT_C(!seen.empty(), "no write reached a remote pile the interpile way");
            for (const auto kind : seen) {
                UNIT_ASSERT_EQUAL(kind, dataKind);
            }
        }
    }

    // A MustRestoreFirst read of a blob that only one pile has makes the bridge proxy copy it into
    // the other piles. That copy is a write the reader cannot avoid, so it has to be admitted the
    // same way the read itself is -- otherwise a system tablet cannot boot off a bridged group that
    // is short of space.
    Y_UNIT_TEST(RestorePutInheritsTheKindOfTheRead) {
        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);

        const TActorId sender = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString data = "hello";

        ui32 step = 1;
        for (const auto dataKind : {NKikimrBlobStorage::TDataKind::USER, NKikimrBlobStorage::TDataKind::SYSTEM}) {
            // Write straight into the group backing the first pile, leaving the other two without
            // the blob.
            const TLogoBlobID id(100500, 1, step++, 0, data.size(), 0);
            env.Runtime->WrapInActorContext(sender, [&] {
                auto put = std::make_unique<TEvBlobStorage::TEvPut>(id, data, TInstant::Max());
                put->ForceGroupGeneration = 1;
                SendToBSProxy(sender, groupId + 1, put.release());
            });
            auto putRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);

            std::vector<NKikimrBlobStorage::TDataKind::E> seen;
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                    const auto& put = *ev->Get<TEvBlobStorage::TEvPut>();
                    if (put.WriteSource == TWriteSource::BridgeProxyRestorePut) {
                        seen.push_back(put.DataKind);
                    }
                }
                return true;
            };

            env.Runtime->WrapInActorContext(sender, [&] {
                auto get = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::FastRead, true /*mustRestoreFirst*/);
                get->DataKind = dataKind;
                SendToBSProxy(sender, info->GroupID, get.release());
            });
            auto getRes = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
            UNIT_ASSERT_VALUES_EQUAL(getRes->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(getRes->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL(getRes->Get()->Responses[0].Status, NKikimrProto::OK);

            env.Runtime->FilterFunction = nullptr;

            UNIT_ASSERT_C(!seen.empty(), "the read restored nothing, so it proves nothing");
            for (const auto kind : seen) {
                UNIT_ASSERT_EQUAL(kind, dataKind);
            }
        }
    }

}
