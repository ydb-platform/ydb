#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/driver_lib/version/version.h>
#include <ydb/core/driver_lib/version/ut/ut_helpers.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(VDiskRecovery) {
    using TCurrent = NKikimrConfig::TCurrentCompatibilityInfo;
    using TYdbVersion = TCompatibilityInfo::TProtoConstructor::TYdbVersion;
    using TCurrentConstructor = TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;

    void TestVersion(TCurrent oldInfo, TCurrent newInfo, bool expected) {
        TCompatibilityInfoTest::Reset(&oldInfo);
        TEnvironmentSetup env{{
            .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
        }};
        env.CreateBoxAndPool(1, 1);

        // Get group info from BSC
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = env.Invoke(request);

        const auto& base = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);
        ui32 groupId = base.GetGroup(0).GetGroupId();

        ui32 puts = 0;

        auto put = [&](bool expected) {
            TActorId sender = env.Runtime->AllocateEdgeActor(1);
            TString data = "Test";
            auto ev = new TEvBlobStorage::TEvPut(TLogoBlobID(1, 1, 1, 1, data.size(), puts++), data, TInstant::Max());

            env.Runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, groupId, ev, 0);
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, true, TInstant::Max());

            if (expected) {
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            } else {
                UNIT_ASSERT_VALUES_UNEQUAL(res->Get()->Status, NKikimrProto::OK);
            }
        };

        env.Sim(TDuration::Seconds(30));
        put(true);
        env.Cleanup();
        TCompatibilityInfoTest::Reset(&newInfo);
        env.Initialize();
        env.Sim(TDuration::Seconds(30));
        put(expected);
    }

    Y_UNIT_TEST(OldCompatibleVersion) {
        TCurrentConstructor oldInfo = {
            .Build = "ydb",
            .YdbVersion = TYdbVersion{ .Year = 23, .Major = 1, .Minor = 22, .Hotfix = 0 }
        };
        TCurrentConstructor newInfo = {
            .Build = "ydb",
            .YdbVersion = TYdbVersion{ .Year = 23, .Major = 2, .Minor = 1, .Hotfix = 0 }
        };
        TestVersion(oldInfo.ToPB(), newInfo.ToPB(), true);
    }

    Y_UNIT_TEST(OldIncompatibleVersion) {
        TCurrentConstructor oldInfo = {
            .Build = "ydb",
            .YdbVersion = TYdbVersion{ .Year = 23, .Major = 1, .Minor = 22, .Hotfix = 0 }
        };
        TCurrentConstructor newInfo = {
            .Build = "ydb",
            .YdbVersion = TYdbVersion{ .Year = 23, .Major = 3, .Minor = 1, .Hotfix = 0 }
        };
        TestVersion(oldInfo.ToPB(), newInfo.ToPB(), false);
    }
}