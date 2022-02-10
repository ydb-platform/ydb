#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <google/protobuf/text_format.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TRtmrTestReboots) {
    Y_UNIT_TEST(CreateRtmrVolumeWithReboots) { //+
        TTestWithReboots t;
        t.Run([&](TTestActorRuntime& runtime, bool& activeZone) {


            AsyncMkDir(runtime, ++t.TxId, "/MyRoot", "DirRtmr");

            const ui64 partitionsCount = 42;
            NKikimrSchemeOp::TRtmrVolumeDescription description;
            description.SetName("rtmr1");
            description.SetPartitionsCount(partitionsCount);
            for (ui64 i = 0; i < description.GetPartitionsCount(); ++i) {
                TGUID partitionId;
                CreateGuid(&partitionId);
                auto& part = *description.AddPartitions();
                part.SetBusKey(i);
                part.SetPartitionId((char*)partitionId.dw, sizeof(TGUID));
            }

            TString textDescription;
            ::google::protobuf::TextFormat::PrintToString(description, &textDescription);
            AsyncCreateRtmrVolume(runtime, ++t.TxId, "/MyRoot/DirRtmr", textDescription);

            t.TestEnv->TestWaitNotification(runtime, {t.TxId, t.TxId-1});

            {
                TInactiveZone inactive(activeZone);
                auto describeResult =  DescribePath(runtime, "/MyRoot/DirRtmr/rtmr1");
                TestDescribeResult(describeResult,
                                   {NLs::Finished});

                UNIT_ASSERT(describeResult.GetPathDescription().HasRtmrVolumeDescription());
                const auto& volumeDescription = describeResult.GetPathDescription().GetRtmrVolumeDescription();
                UNIT_ASSERT_EQUAL(partitionsCount, volumeDescription.GetPartitionsCount());
                UNIT_ASSERT_VALUES_EQUAL(volumeDescription.GetName(), "rtmr1");
                for (ui64 i = 0; i < partitionsCount; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(volumeDescription.GetPartitions(i).GetPartitionId(), description.GetPartitions(i).GetPartitionId());
                    UNIT_ASSERT_EQUAL(volumeDescription.GetPartitions(i).GetBusKey(), description.GetPartitions(i).GetBusKey());
                    UNIT_ASSERT(volumeDescription.GetPartitions(i).HasTabletId());
                    UNIT_ASSERT(volumeDescription.GetPartitions(i).GetTabletId() != (ui64)-1);
                }
            }
        });
    }
}
