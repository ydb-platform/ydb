#include <gtest/gtest.h>
#include <ydb/core/blobstorage/vdisk/common/capnp/protos.h>


TEST(CapnpObjectTest, SerializeAndDeserialize) {
// Create a Cap'n Proto object and fill some fields
NKikimrCapnProto::TEvVGet::Builder originalObject;
originalObject.SetAcquireBlockedGeneration(true);
originalObject.SetCookie(42);
originalObject.SetTabletId(1234);
originalObject.SetSnapshotId("some_snapshot_id");

// Serialize the object into bytes
NActors::TAllocChunkSerializer output;
ASSERT_TRUE(originalObject.SerializeToZeroCopyStream(&output));

auto data = output.Release({});

// Deserialize the bytes into a new object
NActors::TRopeStream input(data->GetBeginIter(), data->GetSize());
NKikimrCapnProto::TEvVGet::Reader deserializedObject;
ASSERT_TRUE(deserializedObject.ParseFromZeroCopyStream(&input));

// Check that the new object has the correct fields
ASSERT_EQ(deserializedObject.GetAcquireBlockedGeneration(), true);
ASSERT_EQ(deserializedObject.GetCookie(), 42);
ASSERT_EQ(deserializedObject.GetTabletId(), 1234);
ASSERT_EQ(deserializedObject.GetSnapshotId(), "some_snapshot_id");
}
