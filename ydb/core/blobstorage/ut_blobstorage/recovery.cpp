#include <ydb/core/base/statestorage.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/driver_lib/version/version.h>
#include <ydb/core/driver_lib/version/ut/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

Y_UNIT_TEST_SUITE(CompatibilityInfo) {
    using EComponentId = NKikimrConfig::TCompatibilityRule::EComponentId;

    using TCurrent = NKikimrConfig::TCurrentCompatibilityInfo;
    using TYdbVersion = TCompatibilityInfo::TProtoConstructor::TVersion;
    using TCompatibilityRule = TCompatibilityInfo::TProtoConstructor::TCompatibilityRule;
    using TCurrentConstructor = TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;

    using TValidateCallback = std::function<bool(TEnvironmentSetup&, TString&)>;
    using TVersion = std::tuple<ui32, ui32, ui32, ui32>;

    std::vector<EComponentId> Components = {
        NKikimrConfig::TCompatibilityRule::PDisk,
        NKikimrConfig::TCompatibilityRule::VDisk,
        NKikimrConfig::TCompatibilityRule::BlobStorageController,
    };

    ui32 passPoisons = 1000;

    void TesCompatibilityForComponent(TVersion oldVersion, TVersion newVersion, EComponentId componentId,
            bool isCompatible, TValidateCallback validateCallback) {
        passPoisons = 1000;
        const TString build = "ydb";
    
        auto oldInfoConstructor = TCurrentConstructor{
            .Application = build,
            .Version = TYdbVersion{
                .Year = std::get<0>(oldVersion),
                .Major = std::get<1>(oldVersion),
                .Minor = std::get<2>(oldVersion),
                .Hotfix = std::get<3>(oldVersion)
            },
        };

        auto newInfoConstructor = TCurrentConstructor{
            .Application = "ydb",
            .Version = TYdbVersion{
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
                    .Application = build,
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
        TString debugInfo;
        UNIT_ASSERT_C(validateCallback(env, debugInfo), debugInfo);

        using TFilterFunction = std::function<bool(ui32, std::unique_ptr<IEventHandle>&)>;
        TFilterFunction ff;
        ff = std::exchange(env.Runtime->FilterFunction, {});

        // Recreate cluster with different YDB version
        env.Cleanup();
        TCompatibilityInfoTest::Reset(&newInfo);
        env.Initialize();
        env.Runtime->FilterFunction = std::exchange(ff, {});

        env.Sim(TDuration::Seconds(30));

        UNIT_ASSERT_C(validateCallback(env, debugInfo) == isCompatible, debugInfo);
    }

    bool ValidateForVDisk(TEnvironmentSetup& env, TString& debugInfo) {
        Y_UNUSED(debugInfo);
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

    bool ValidateForBSController(TEnvironmentSetup& env, TString& debugInfo) {
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvents::TSystem::PoisonPill) {
                if (passPoisons > 0) {
                    passPoisons--;
                    return true;
                }
                return false;
            }
            return true;
        };

        auto getTabletGen = [&]() -> ui32 {
            const TActorId getGenEdge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);
            const TActorId stateStorageProxyId = MakeStateStorageProxyID(StateStorageGroupFromTabletID(env.TabletId));
            env.Runtime->WrapInActorContext(getGenEdge, [&] {
                TActivationContext::Send(new IEventHandle(stateStorageProxyId, getGenEdge,
                    new TEvStateStorage::TEvLookup(env.TabletId, 0), 0, 0)
                );
            });
            auto response = env.WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(getGenEdge, true);
            return response->Get()->CurrentGeneration;
        };
        
        const ui32 gen1 = getTabletGen();
        env.Sim(TDuration::Seconds(30));
        const ui32 gen2 = getTabletGen();

        debugInfo = (TStringBuilder() << "gen1# " << gen1 << " gen2# " << gen2 << " passPoisons# " << passPoisons);
        return gen1 == gen2 && passPoisons > 0;
    };

    auto componentVDisk = NKikimrConfig::TCompatibilityRule::VDisk;
    auto componentBSController = NKikimrConfig::TCompatibilityRule::BlobStorageController;

    Y_UNIT_TEST(VDiskCompatible) {
        TesCompatibilityForComponent({ 23, 1, 19, 0 }, { 23, 2, 1, 0 }, componentVDisk, true, ValidateForVDisk);
    }

    Y_UNIT_TEST(VDiskIncompatible) {
        TesCompatibilityForComponent({ 23, 1, 19, 0 }, { 23, 3, 1, 0 }, componentVDisk, false, ValidateForVDisk);
    }

    Y_UNIT_TEST(VDiskIncompatibleWithDefault) {
        TesCompatibilityForComponent({ 24, 2, 1, 0 }, { 24, 2, 1, 0 }, componentVDisk, true, ValidateForVDisk);
    }

    Y_UNIT_TEST(BSControllerCompatible) {
        TesCompatibilityForComponent({ 23, 1, 19, 0 }, { 23, 2, 1, 0 }, componentBSController, true, ValidateForBSController);
    }

    Y_UNIT_TEST(BSControllerIncompatible) {
        TesCompatibilityForComponent({ 23, 1, 19, 0 }, { 23, 3, 1, 0 }, componentBSController, false, ValidateForBSController);
    }

    Y_UNIT_TEST(BSControllerIncompatibleWithDefault) {
        TesCompatibilityForComponent({ 24, 2, 1, 0 }, { 24, 2, 1, 0 }, componentBSController, true, ValidateForBSController);
    }
}
