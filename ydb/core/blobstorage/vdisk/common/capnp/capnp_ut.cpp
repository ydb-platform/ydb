#include <ydb/core/blobstorage/vdisk/common/capnp/protos.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/blobstorage/backpressure/event.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>


namespace NKikimr {
    NKikimrCapnProto::TEvVGet::Builder reserialize(const NKikimrCapnProto::TEvVGet::Builder & original) {
        NActors::TAllocChunkSerializer output;
        UNIT_ASSERT(original.SerializeToZeroCopyStream(&output));
        auto data = output.Release({});

        // Deserialize the bytes into a new object
        NActors::TRopeStream input(data->GetBeginIter(), data->GetSize());
        NKikimrCapnProto::TEvVGet::Builder deserializedObject;
        deserializedObject.ParseFromZeroCopyStream(&input);

        return deserializedObject;
    }


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


                NKikimrCapnProto::TEvVGet::Reader deserializedObject = reserialize(originalObject);

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

            NKikimrCapnProto::TEvVGet::Reader deserializedObject = reserialize(originalObject);

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

            NKikimrCapnProto::TEvVGet::Reader deserializedObject = reserialize(originalObject);

            // Check that deserializedObject.HasMsgQoS() == true
            UNIT_ASSERT(deserializedObject.HasMsgQoS());
        }

        Y_UNIT_TEST(CopyFrom) {
            NKikimrCapnProto::TEvVGet::Builder from, to;

            from.MutableMsgQoS().MutableMsgId().SetMsgId(1234);
            from.MutableMsgQoS().MutableMsgId().SetSequenceId(4321);

            to.CopyFrom(from);

            UNIT_ASSERT(to.GetMsgQoS().GetMsgId().GetMsgId() == 1234);
            UNIT_ASSERT(to.GetMsgQoS().GetMsgId().GetSequenceId() == 4321);

            UNIT_ASSERT(from.GetMsgQoS().GetMsgId().GetMsgId() == 1234);
            UNIT_ASSERT(from.GetMsgQoS().GetMsgId().GetSequenceId() == 4321);

            NKikimrCapnProto::TEvVGet::Reader reserializedFrom = reserialize(from), reserializedTo = reserialize(to);

            UNIT_ASSERT(reserializedFrom.GetMsgQoS().GetMsgId().GetMsgId() == 1234);
            UNIT_ASSERT(reserializedFrom.GetMsgQoS().GetMsgId().GetSequenceId() == 4321);

            UNIT_ASSERT(reserializedTo.GetMsgQoS().GetMsgId().GetMsgId() == 1234);
            UNIT_ASSERT(reserializedTo.GetMsgQoS().GetMsgId().GetSequenceId() == 4321);
        }

        Y_UNIT_TEST(SendToVDiskScenario) {
            auto tevvget = std::make_unique<TEvBlobStorage::TEvVGet>();
            uint64_t msgId = 1234;
            uint64_t sequenceId = 4321;

            auto processMsgQoS = [&](auto& record) {
                // prepare extra buffer with some changed params
                auto& msgQoS = *record.MutableMsgQoS();
                auto& id = *msgQoS.MutableMsgId();
                id.SetMsgId(msgId);
                id.SetSequenceId(sequenceId);
            };

            auto callback = [&](auto *ev) -> std::unique_ptr<NActors::IEventBase> {
                using T = std::remove_pointer_t<decltype(ev)>;
                processMsgQoS(ev->Record);
                auto clone = std::make_unique<T>();
                clone->Record.CopyFrom(ev->Record);
                for (ui32 i = 0, count = ev->GetPayloadCount(); i < count; ++i) {
                    clone->AddPayload(TRope(ev->GetPayload(i)));
                }
                return clone;
            };

            auto tevvgetClone = callback(static_cast<TEvBlobStorage::TEvVGet*>(tevvget.get()));

            NActors::TAllocChunkSerializer output;
            UNIT_ASSERT(tevvgetClone->SerializeToArcadiaStream(&output));
            auto data = output.Release({});
            NActors::TRopeStream input(data->GetBeginIter(), data->GetSize());
            NKikimrCapnProto::TEvVGet::Reader tevvgetCloneDeserialized;
            tevvgetCloneDeserialized.ParseFromZeroCopyStream(&input);

            UNIT_ASSERT(tevvgetCloneDeserialized.GetMsgQoS().GetMsgId().GetMsgId() == msgId);
            UNIT_ASSERT(tevvgetCloneDeserialized.GetMsgQoS().GetMsgId().GetSequenceId() == sequenceId);

            UNIT_ASSERT(tevvget->Record.GetMsgQoS().GetMsgId().GetMsgId() == msgId);
            UNIT_ASSERT(tevvget->Record.GetMsgQoS().GetMsgId().GetSequenceId() == sequenceId);
        }

        Y_UNIT_TEST(AccessFieldThroughMutable) {
            auto tevvget = std::make_unique<TEvBlobStorage::TEvVGet>();
            uint32_t groupId = 1234;
            auto queueId = NKikimrCapnProto::EVDiskQueueId::GetDiscover;

            // set
            tevvget->Record.MutableVDiskID().SetGroupID(groupId);
            tevvget->Record.MutableMsgQoS().SetExtQueueId(queueId);

            // reserialize
            auto tevvgetReserialized = reserialize(tevvget->Record);

            // check
            auto vdisk = tevvgetReserialized.MutableVDiskID();
            auto qos = tevvgetReserialized.MutableMsgQoS();
            UNIT_ASSERT(!vdisk->HasDomain());
            UNIT_ASSERT(vdisk->HasGroupID());
            UNIT_ASSERT(qos->HasExtQueueId());
            UNIT_ASSERT(vdisk->GetGroupID() == groupId);
            UNIT_ASSERT(qos->GetExtQueueId() == queueId);
        }
    };
};
