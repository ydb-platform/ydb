#include <ydb/core/blobstorage/vdisk/common/capnp/protos.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
    Y_UNIT_TEST_SUITE(CapnpTests) {
        Y_UNIT_TEST(TEvVGetBasic) {
                // Create a Cap'n Proto object and fill some fields
                NKikimrCapnProto::TEvVGet::Builder originalObject;
                originalObject.SetAcquireBlockedGeneration(true);
                originalObject.SetCookie(42);
                originalObject.SetTabletId(1234);
                originalObject.SetSnapshotId("some_snapshot_id");

                auto originalMsgQoS = originalObject.MutableMsgQoS();
                originalMsgQoS.SetCost(4242);
                originalMsgQoS.SetProxyNodeId(91);
                originalMsgQoS.MutableCostSettings().SetMinREALHugeBlobInBytes(101);
                originalMsgQoS.MutableCostSettings().SetSeekTimeUs(100500);
                originalMsgQoS.MutableCostSettings().SetReadSpeedBps(500100);

                originalObject.AddExtremeQueries().SetSize(6767);
                originalObject.AddExtremeQueries().SetSize(8989);


                // Serialize the object into bytes
                NActors::TAllocChunkSerializer output;
                UNIT_ASSERT(originalObject.SerializeToZeroCopyStream(&output));
                auto data = output.Release({});

                // Deserialize the bytes into a new object
                NActors::TRopeStream input(data->GetBeginIter(), data->GetSize());
                NKikimrCapnProto::TEvVGet::Reader deserializedObject;
                UNIT_ASSERT(deserializedObject.ParseFromZeroCopyStream(&input));

                // Check that the new object has the correct fields
                UNIT_ASSERT(deserializedObject.GetAcquireBlockedGeneration() == true);
                UNIT_ASSERT(deserializedObject.GetCookie() == 42);
                UNIT_ASSERT(deserializedObject.GetTabletId() == 1234);
                UNIT_ASSERT(deserializedObject.GetSnapshotId() == "some_snapshot_id");

                auto deserializedMsgQoS = originalObject.MutableMsgQoS();
                UNIT_ASSERT(deserializedMsgQoS.GetCost() == 4242);
                UNIT_ASSERT(deserializedMsgQoS.GetProxyNodeId() == 91);
                UNIT_ASSERT(deserializedMsgQoS.GetCostSettings().GetMinREALHugeBlobInBytes() == 101);
                UNIT_ASSERT(deserializedMsgQoS.GetCostSettings().GetSeekTimeUs() == 100500);
                UNIT_ASSERT(deserializedMsgQoS.GetCostSettings().GetReadSpeedBps() == 500100);

                UNIT_ASSERT(deserializedObject.ExtremeQueriesSize() == 2);
                UNIT_ASSERT(deserializedObject.GetExtremeQueries(0).GetSize() == 6767);
                UNIT_ASSERT(deserializedObject.GetExtremeQueries(1).GetSize() == 8989);
        }

        Y_UNIT_TEST(HasMsgQoS) {
            // Create a Cap'n Proto object and fill MsgQoS
            NKikimrCapnProto::TEvVGet::Builder originalObject;

            auto originalMsgQoS = originalObject.MutableMsgQoS();
            originalMsgQoS.SetCost(4242);
            originalMsgQoS.SetProxyNodeId(91);
            originalMsgQoS.MutableCostSettings().SetMinREALHugeBlobInBytes(101);
            originalMsgQoS.MutableCostSettings().SetSeekTimeUs(100500);

            auto& id = *originalMsgQoS.MutableMsgId();
            id.SetMsgId(1234);
            id.SetSequenceId(1234);

            // Serialize the object into bytes
            NActors::TAllocChunkSerializer output;
            UNIT_ASSERT(originalObject.SerializeToZeroCopyStream(&output));
            auto data = output.Release({});

            // Deserialize the bytes into a new object
            NActors::TRopeStream input(data->GetBeginIter(), data->GetSize());
            NKikimrCapnProto::TEvVGet::Reader deserializedObject;
            UNIT_ASSERT(deserializedObject.ParseFromZeroCopyStream(&input));

            // Check that deserializedObject.HasMsgQoS() == true
            UNIT_ASSERT(deserializedObject.HasMsgQoS());
            UNIT_ASSERT(deserializedObject.GetMsgQoS().HasMsgId());
            UNIT_ASSERT(deserializedObject.GetMsgQoS().GetMsgId().GetMsgId() == 1234);
            UNIT_ASSERT(deserializedObject.GetMsgQoS().GetMsgId().GetSequenceId() == 1234);
        }

        Y_UNIT_TEST(HasMsgQoSOnlyEnumSet) {
            // Create a Cap'n Proto object and fill MsgQoS
            NKikimrCapnProto::TEvVGet::Builder originalObject;

            auto originalMsgQoS = originalObject.MutableMsgQoS();
            originalMsgQoS.SetExtQueueId(NKikimrCapnProto::EVDiskQueueId::GetDiscover);

            // Serialize the object into bytes
            NActors::TAllocChunkSerializer output;
            UNIT_ASSERT(originalObject.SerializeToZeroCopyStream(&output));
            auto data = output.Release({});

            // Deserialize the bytes into a new object
            NActors::TRopeStream input(data->GetBeginIter(), data->GetSize());
            NKikimrCapnProto::TEvVGet::Reader deserializedObject;
            UNIT_ASSERT(deserializedObject.ParseFromZeroCopyStream(&input));

            // Check that deserializedObject.HasMsgQoS() == true
            UNIT_ASSERT(deserializedObject.HasMsgQoS());
        }
    };
};
