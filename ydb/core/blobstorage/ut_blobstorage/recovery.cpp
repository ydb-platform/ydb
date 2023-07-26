#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/driver_lib/version/version.h>
#include <ydb/core/driver_lib/version/ut/ut_helpers.h>
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>

Y_UNIT_TEST_SUITE(CompatibilityInfo) {
    using EComponentId = NKikimrConfig::TCompatibilityRule::EComponentId;

    using TCurrent = NKikimrConfig::TCurrentCompatibilityInfo;
    using TYdbVersion = TCompatibilityInfo::TProtoConstructor::TYdbVersion;
    using TCompatibilityRule = TCompatibilityInfo::TProtoConstructor::TCompatibilityRule;
    using TCurrentConstructor = TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;

    using TValidateCallback = std::function<bool(TEnvironmentSetup&)>;
    using TVersion = std::tuple<ui32, ui32, ui32, ui32>;

    std::vector<EComponentId> Components = {
        NKikimrConfig::TCompatibilityRule::PDisk,
        NKikimrConfig::TCompatibilityRule::VDisk,
        NKikimrConfig::TCompatibilityRule::BlobStorageController
    };

    void TesCompatibilityForComponent(TVersion oldVersion, TVersion newVersion, EComponentId componentId,
            bool isCompatible, TValidateCallback validateCallback) {
        const TString build = "ydb";
    
        auto oldInfoConstructor = TCurrentConstructor{
            .Build = build,
            .YdbVersion = TYdbVersion{
                .Year = std::get<0>(oldVersion),
                .Major = std::get<1>(oldVersion),
                .Minor = std::get<2>(oldVersion),
                .Hotfix = std::get<3>(oldVersion)
            },
        };

        auto newInfoConstructor = TCurrentConstructor{
            .Build = "ydb",
            .YdbVersion = TYdbVersion{
                .Year = std::get<0>(newVersion),
                .Major = std::get<1>(newVersion),
                .Minor = std::get<2>(newVersion),
                .Hotfix = std::get<3>(newVersion)
            },
        };

        // Disable compatibility checks for all other components
        for (auto component : Components) {
            if (component != componentId) {
                auto newRule = TCompatibilityRule{
                    .Build = build,
                    .LowerLimit = TYdbVersion{ .Year = 0, .Major = 0, .Minor = 0, .Hotfix = 0 },
                    .UpperLimit = TYdbVersion{ .Year = 1000, .Major = 1000, .Minor = 1000, .Hotfix = 1000 },
                    .ComponentId = component,
                };
                oldInfoConstructor.CanLoadFrom.push_back(newRule);
                oldInfoConstructor.StoresReadableBy.push_back(newRule);
                newInfoConstructor.CanLoadFrom.push_back(newRule);
                newInfoConstructor.StoresReadableBy.push_back(newRule);
            }
        }

        auto oldInfo = oldInfoConstructor.ToPB();
        auto newInfo = newInfoConstructor.ToPB();

        TCompatibilityInfoTest::Reset(&oldInfo);

        TEnvironmentSetup env{{
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
        }};
        env.CreateBoxAndPool(1, 1);

        env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT(validateCallback(env));

        // Recreate cluster with different YDB version
        env.Cleanup();
        TCompatibilityInfoTest::Reset(&newInfo);
        env.Initialize();
        env.Sim(TDuration::Seconds(30));

        UNIT_ASSERT(validateCallback(env) == isCompatible);
    }

    bool ValidateForVDisk(TEnvironmentSetup& env) {
        static ui32 puts = 0;
        // Get group info from BSC
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = env.Invoke(request);

        const auto& base = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);
        ui32 groupId = base.GetGroup(0).GetGroupId();

        TActorId sender = env.Runtime->AllocateEdgeActor(1);
        TString data = "Test";
        auto ev = new TEvBlobStorage::TEvPut(TLogoBlobID(1, 1, 1, 1, data.size(), puts++), data, TInstant::Max());

        env.Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev, 0);
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, true, TInstant::Max());

        return res->Get()->Status == NKikimrProto::OK;
    };

    auto componentVDisk = NKikimrConfig::TCompatibilityRule::VDisk;

    Y_UNIT_TEST(VDiskComaptible) {
        TesCompatibilityForComponent({ 23, 1, 19, 0 }, { 23, 2, 1, 0 }, componentVDisk, true, ValidateForVDisk);
    }

    Y_UNIT_TEST(VDiskIncomaptible) {
        TesCompatibilityForComponent({ 23, 1, 19, 0 }, { 23, 3, 1, 0 }, componentVDisk, false, ValidateForVDisk);
    }

    Y_UNIT_TEST(VDiskIncompatibleWithDefault) {
        TesCompatibilityForComponent({ 24, 2, 1, 0 }, { 24, 2, 1, 0 }, componentVDisk, true, ValidateForVDisk);
    }
}
