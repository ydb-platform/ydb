#include <ydb/core/blobstorage/vdisk/common/capnp/protos.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
    Y_UNIT_TEST_SUITE(TEvVGetTests) {
        Y_UNIT_TEST(Basic) {
                // Create a Cap'n Proto object and fill some fields
                NKikimrCapnProto::TEvVGet::Builder originalObject;
                originalObject.SetAcquireBlockedGeneration(true);
                originalObject.SetCookie(42);
                originalObject.SetTabletId(1234);
                originalObject.SetSnapshotId("some_snapshot_id");

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
        }
    };
};
