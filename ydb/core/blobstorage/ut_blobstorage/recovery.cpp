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

    using TValidateCallback = std::function<bool(std::unique_ptr<TEnvironmentSetup>&, TString&)>;
    using TVersion = std::tuple<ui32, ui32, ui32, ui32>;

    std::vector<EComponentId> Components = {
        NKikimrConfig::TCompatibilityRule::PDisk,
        NKikimrConfig::TCompatibilityRule::VDisk,
        NKikimrConfig::TCompatibilityRule::BlobStorageController,
    };

    ui32 passPoisons = 1000;

    NKikimrConfig::TCurrentCompatibilityInfo MakeCurrent(const std::optional<TVersion>& version, EComponentId componentId) {
        TString build = "ydb";
        TCurrentConstructor ctor;
        if (version) {
            ctor = TCurrentConstructor{
                .Application = build,
                .Version = TYdbVersion{
                    .Year = std::get<0>(*version),
                    .Major = std::get<1>(*version),
                    .Minor = std::get<2>(*version),
                    .Hotfix = std::get<3>(*version)
                }
            };
        } else {
            ctor = TCurrentConstructor{
                .Application = "trunk",
            };
        }

        // Disable compatibility checks for all other components
        for (auto component : Components) {
            if (component != componentId) {
                auto newRule = TCompatibilityRule{
                    .Application = build,
                    .LowerLimit = TYdbVersion{ .Year = 0, .Major = 0, .Minor = 0, .Hotfix = 0 },
                    .UpperLimit = TYdbVersion{ .Year = 1000, .Major = 1000, .Minor = 1000, .Hotfix = 1000 },
                    .ComponentId = component,
                };
                ctor.CanLoadFrom.push_back(newRule);
                ctor.StoresReadableBy.push_back(newRule);
            }
        }
        return ctor.ToPB();
    }

    void SetupEnv(std::unique_ptr<TEnvironmentSetup>& env, bool suppressCompatibilityCheck = false) {
        env.reset();
        env = std::make_unique<TEnvironmentSetup>(TEnvironmentSetup::TSettings{
            .NodeCount = 1,
            .Erasure = TBlobStorageGroupType::ErasureNone,
            .SuppressCompatibilityCheck = suppressCompatibilityCheck,
        });
        env->CreateBoxAndPool(1, 1);
        env->Sim(TDuration::Seconds(30));
    }

    void RestartEnv(std::unique_ptr<TEnvironmentSetup>& env, NKikimrConfig::TCurrentCompatibilityInfo* newInfo) {
        using TFilterFunction = std::function<bool(ui32, std::unique_ptr<IEventHandle>&)>;
        TFilterFunction ff = std::exchange(env->Runtime->FilterFunction, {});
        env->Cleanup();
        TCompatibilityInfoTest::Reset(newInfo);
        env->Initialize();
        env->Runtime->FilterFunction = std::exchange(ff, {});
        env->Sim(TDuration::Seconds(30));
    }

    void TestCompatibilityForComponent(std::optional<TVersion> oldVersion, std::optional<TVersion> newVersion, EComponentId componentId,
            bool isCompatible, TValidateCallback validateCallback, bool suppressCompatibilityCheck = false) {
        passPoisons = 1000;

        auto oldInfo = MakeCurrent(oldVersion, componentId);
        auto newInfo = MakeCurrent(newVersion, componentId);
    
        TCompatibilityInfoTest::Reset(&oldInfo);

        std::unique_ptr<TEnvironmentSetup> env;
        SetupEnv(env, suppressCompatibilityCheck);
        TString debugInfo;
    
        bool success = validateCallback(env, debugInfo);
        UNIT_ASSERT_C(success, debugInfo);

        RestartEnv(env, &newInfo);

        success = validateCallback(env, debugInfo);
        UNIT_ASSERT_VALUES_EQUAL_C(success, isCompatible, debugInfo);
    }

    bool ValidateForVDisk(std::unique_ptr<TEnvironmentSetup>& env, TString& debugInfo) {
        Y_UNUSED(debugInfo);
        static ui32 puts = 0;
        // Get group info from BSC
        NKikimrBlobStorage::TConfigRequest request;
        request.AddCommand()->MutableQueryBaseConfig();
        auto response = env->Invoke(request);

        const auto& base = response.GetStatus(0).GetBaseConfig();
        UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);
        ui32 groupId = base.GetGroup(0).GetGroupId();

        TActorId sender = env->Runtime->AllocateEdgeActor(1);
        TString data = "Test";
        auto ev = new TEvBlobStorage::TEvPut(TLogoBlobID(1, 1, 1, 1, data.size(), puts++), data, TInstant::Max());

        env->Runtime->WrapInActorContext(sender, [&] {
            SendToBSProxy(sender, groupId, ev, 0);
        });
        auto res = env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, true, TInstant::Max());

        return res->Get()->Status == NKikimrProto::OK;
    };

    bool ValidateForBSController(std::unique_ptr<TEnvironmentSetup>& env, TString& debugInfo) {
        env->Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
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
            const TActorId getGenEdge = env->Runtime->AllocateEdgeActor(env->Settings.ControllerNodeId, __FILE__, __LINE__);
            const TActorId stateStorageProxyId = MakeStateStorageProxyID();
            env->Runtime->WrapInActorContext(getGenEdge, [&] {
                TActivationContext::Send(new IEventHandle(stateStorageProxyId, getGenEdge,
                    new TEvStateStorage::TEvLookup(env->TabletId, 0), 0, 0)
                );
            });
            auto response = env->WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(getGenEdge, true);
            return response->Get()->CurrentGeneration;
        };

        const ui32 gen1 = getTabletGen();
        env->Sim(TDuration::Seconds(30));
        const ui32 gen2 = getTabletGen();

        debugInfo = (TStringBuilder() << "gen1# " << gen1 << " gen2# " << gen2 << " passPoisons# " << passPoisons);
        return gen1 == gen2 && passPoisons > 0;
    };

    auto componentVDisk = NKikimrConfig::TCompatibilityRule::VDisk;
    auto componentBSController = NKikimrConfig::TCompatibilityRule::BlobStorageController;

    Y_UNIT_TEST(VDiskCompatible) {
        TestCompatibilityForComponent(TVersion{ 23, 1, 19, 0 }, TVersion{ 23, 2, 1, 0 }, componentVDisk, true, ValidateForVDisk);
    }

    Y_UNIT_TEST(VDiskIncompatible) {
        TestCompatibilityForComponent(TVersion{ 23, 1, 19, 0 }, TVersion{ 23, 3, 1, 0 }, componentVDisk, false, ValidateForVDisk);
    }

    Y_UNIT_TEST(VDiskIncompatibleWithDefault) {
        TestCompatibilityForComponent(TVersion{ 24, 2, 1, 0 }, TVersion{ 24, 2, 1, 0 }, componentVDisk, true, ValidateForVDisk);
    }

    Y_UNIT_TEST(VDiskSuppressCompatibilityCheck) {
        TestCompatibilityForComponent(std::nullopt, TVersion{ 23, 3, 8, 0 }, componentVDisk, true, ValidateForVDisk, true);
    }

    Y_UNIT_TEST(BSControllerCompatible) {
        TestCompatibilityForComponent(TVersion{ 23, 1, 19, 0 }, TVersion{ 23, 2, 1, 0 }, componentBSController, true, ValidateForBSController);
    }

    Y_UNIT_TEST(BSControllerIncompatible) {
        TestCompatibilityForComponent(TVersion{ 23, 1, 19, 0 }, TVersion{ 23, 3, 1, 0 }, componentBSController, false, ValidateForBSController);
    }

    Y_UNIT_TEST(BSControllerIncompatibleWithDefault) {
        TestCompatibilityForComponent(TVersion{ 24, 2, 1, 0 }, TVersion{ 24, 2, 1, 0 }, componentBSController, true, ValidateForBSController);
    }

    Y_UNIT_TEST(BSControllerSuppressCompatibilityCheck) {
        TestCompatibilityForComponent(std::nullopt, TVersion{ 23, 3, 8, 0 }, componentBSController, true, ValidateForBSController, true);
    }

    void TestMigration(std::optional<TVersion> oldVersion, std::optional<TVersion> intermediateVersion, std::optional<TVersion> newVersion,
            EComponentId componentId, TValidateCallback validateCallback) {
        passPoisons = 1000;

        auto oldInfo = MakeCurrent(oldVersion, componentId);
        auto intermediateInfo = MakeCurrent(intermediateVersion, componentId);
        auto newInfo = MakeCurrent(newVersion, componentId);
    
        TCompatibilityInfoTest::Reset(&oldInfo);

        std::unique_ptr<TEnvironmentSetup> env;
        SetupEnv(env);
        TString debugInfo;
    
        bool success = validateCallback(env, debugInfo);
        UNIT_ASSERT_C(success, debugInfo);

        RestartEnv(env, &intermediateInfo);

        success = validateCallback(env, debugInfo);
        UNIT_ASSERT_C(success, debugInfo);

        RestartEnv(env, &newInfo);

        success = validateCallback(env, debugInfo);
        UNIT_ASSERT_C(success, debugInfo);
    }

    Y_UNIT_TEST(VDiskMigration) {
        TestMigration(TVersion{ 23, 3, 13, 0 }, TVersion{ 23, 4, 1, 0 }, TVersion{ 23, 5, 1, 0 }, componentVDisk, ValidateForVDisk);
    }

    Y_UNIT_TEST(BSControllerMigration) {
        TestMigration(TVersion{ 23, 3, 13, 0 }, TVersion{ 23, 4, 1, 0 }, TVersion{ 23, 5, 1, 0 }, componentBSController, ValidateForBSController);
    }

}
