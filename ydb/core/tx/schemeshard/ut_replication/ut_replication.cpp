#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/replication/controller/public_events.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/ydb_convert/replication_description.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/lib/ydb_cli/dump/util/replication_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>

#include <yql/essentials/public/udf/udf_data_type.h>

#include <util/string/hex.h>

using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TReplicationTests) {
    static TString DefaultScheme(const TString& name, const TVector<TString>& destinations = {}) {
        NKikimrSchemeOp::TReplicationDescription description;
        description.SetName(name);
        auto* config = description.MutableConfig();
        auto* credentials = config->MutableSrcConnectionParams()->MutableStaticCredentials();
        credentials->SetUser("user");
        credentials->SetPassword("pwd");

        const auto paths = destinations.empty() ? TVector<TString>{"/MyRoot2/" + name} : destinations;
        for (const auto& path : paths) {
            auto* target = config->MutableSpecific()->AddTargets();
            target->SetSrcPath("/MyRoot1/Table");
            target->SetDstPath(path);
        }

        return description.DebugString();
    }

    static TString StateScheme(const TString& name, const TString& state) {
        return Sprintf(R"(Name: "%s" State { %s {} })", name.c_str(), state.c_str());
    }

    void SetLegacyReplicationDestination(TTestActorRuntime& runtime,
            const TString& name, const TString& destination)
    {
        const auto path = DescribePath(runtime, "/MyRoot/" + name);
        const auto pathId = path.GetPathDescription().GetSelf().GetPathId();
        const auto row = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"((
            (let key '('('PathId (Uint64 '%lu))))
            (return (AsList (SetResult 'Replication (SelectRow 'Replications key '('Description)))))
        ))", pathId));

        NKikimrSchemeOp::TReplicationDescription description;
        UNIT_ASSERT(description.ParseFromString(row.GetValue().GetStruct(0).GetOptional()
            .GetOptional().GetStruct(0).GetOptional().GetBytes()));
        auto* targets = description.MutableConfig()->MutableSpecific();
        UNIT_ASSERT_VALUES_EQUAL(targets->TargetsSize(), 1);
        targets->MutableTargets(0)->SetDstPath(destination);

        // Emulate metadata written before CREATE rejected conflicting paths.
        // The caller reboots SchemeShard to load the modified configuration.
        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, Sprintf(R"((
            (let key '('('PathId (Uint64 '%lu))))
            (let row '('('Description (String 'x"%s"))))
            (return (AsList (UpdateRow 'Replications key row)))
        ))", pathId, HexEncode(description.SerializeAsString()).c_str()));
    }

    void SetupLogging(TTestActorRuntimeBase& runtime) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::REPLICATION_CONTROLLER, NActors::NLog::PRI_TRACE);
    }

    ui64 ExtractControllerId(const NKikimrSchemeOp::TPathDescription& desc) {
        UNIT_ASSERT(desc.HasReplicationDescription());
        const auto& r = desc.GetReplicationDescription();
        UNIT_ASSERT(r.HasControllerId());
        return r.GetControllerId();
    }

    Y_UNIT_TEST(Create) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::PathExist,
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kStandBy),
        });
    }

    TString AvailabilityScheme(const TString& name, const TString& period) {
        auto scheme = DefaultScheme(name);
        scheme.insert(scheme.find("Config {") + TStringBuf("Config {").size(),
            TStringBuilder() << " AvailabilityPeriod { " << period << " } ");
        return scheme;
    }

    Y_UNIT_TEST(AvailabilityPeriodCreateAndReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;
        const TVector<TString> periods = {"", "Unlimited {}", "Duration { seconds: 10 nanos: 1 }", "Duration { nanos: 1 }"};
        for (ui32 i = 0; i < periods.size(); ++i) {
            const auto name = TStringBuilder() << "Replication" << i;
            TestCreateReplication(runtime, ++txId, "/MyRoot",
                i == 0 ? DefaultScheme(name) : AvailabilityScheme(name, periods[i]));
            env.TestWaitNotification(runtime, txId);
        }

        const auto check = [&] {
            for (ui32 i = 0; i < periods.size(); ++i) {
                const auto desc = DescribePath(runtime, TStringBuilder() << "/MyRoot/Replication" << i);
                const auto& path = desc.GetPathDescription();
                const auto& period = path.GetReplicationDescription().GetConfig().GetAvailabilityPeriod();
                UNIT_ASSERT_VALUES_EQUAL(period.HasUnlimited(), i < 2);
                if (i >= 2) {
                    UNIT_ASSERT(period.HasDuration());
                    UNIT_ASSERT_VALUES_EQUAL(period.GetDuration().seconds(), i == 2 ? 10 : 0);
                    UNIT_ASSERT_VALUES_EQUAL(period.GetDuration().nanos(), 1000000);
                }
                using namespace NKikimr::NReplication;
                const auto edge = runtime.AllocateEdgeActor();
                auto request = MakeHolder<TEvController::TEvDescribeReplication>();
                request->Record.MutablePathId()->SetOwnerId(path.GetSelf().GetSchemeshardId());
                request->Record.MutablePathId()->SetLocalId(path.GetSelf().GetPathId());
                runtime.SendToPipe(ExtractControllerId(path), edge, request.Release());
                const auto result = runtime.GrabEdgeEvent<TEvController::TEvDescribeReplicationResult>(edge);
                UNIT_ASSERT(result->Get()->Record.HasAvailabilityPeriod());
                UNIT_ASSERT_VALUES_EQUAL(result->Get()->Record.GetAvailabilityPeriod().SerializeAsString(), period.SerializeAsString());
            }
        };
        check();
        for (ui32 i = 0; i < periods.size(); ++i) {
            const TString name = TStringBuilder() << "Replication" << i;
            TestAlterReplication(runtime, ++txId, "/MyRoot",
                Sprintf(R"(Name: "%s" State { Paused {} })", name.c_str()));
            env.TestWaitNotification(runtime, txId);
            TestAlterReplication(runtime, ++txId, "/MyRoot", Sprintf(R"(
                Name: "%s"
                Config { SrcConnectionParams { StaticCredentials { Password: "new-password" } } }
            )", name.c_str()));
            env.TestWaitNotification(runtime, txId);
        }
        check();
        // Both tablets must reload the persisted, normalized policy.
        TVector<ui64> controllers;
        for (ui32 i = 0; i < periods.size(); ++i) {
            controllers.push_back(ExtractControllerId(DescribePath(runtime,
                TStringBuilder() << "/MyRoot/Replication" << i).GetPathDescription()));
        }
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        for (const auto id : controllers) {
            RebootTablet(runtime, id, runtime.AllocateEdgeActor());
        }
        check();
    }

    Y_UNIT_TEST_FLAG(AvailabilityPeriodLegacyControllerConfig, CustomDefault) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        if (CustomDefault) {
            runtime.GetAppData().ReplicationConfig.SetConsumerAvailabilityPeriodSeconds(7201);
        }
        const auto seconds = runtime.GetAppData().ReplicationConfig.GetConsumerAvailabilityPeriodSeconds();
        using namespace NKikimr::NReplication;
        auto observer = runtime.AddObserver<TEvController::TEvCreateReplication>([](const auto& ev) {
            ev->Get()->Record.MutableConfig()->ClearAvailabilityPeriod();
        });
        TestCreateReplication(runtime, 101, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, 101);
        observer.Remove();
        const auto desc = DescribePath(runtime, "/MyRoot/Replication");
        const auto& path = desc.GetPathDescription();
        const auto controller = ExtractControllerId(path);
        const auto check = [&] {
            const auto edge = runtime.AllocateEdgeActor();
            auto request = MakeHolder<TEvController::TEvDescribeReplication>();
            request->Record.MutablePathId()->SetOwnerId(path.GetSelf().GetSchemeshardId());
            request->Record.MutablePathId()->SetLocalId(path.GetSelf().GetPathId());
            runtime.SendToPipe(controller, edge, request.Release());
            const auto result = runtime.GrabEdgeEvent<TEvController::TEvDescribeReplicationResult>(edge);
            UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
            const auto& record = result->Get()->Record;
            UNIT_ASSERT(record.HasAvailabilityPeriod());
            UNIT_ASSERT(record.GetAvailabilityPeriod().HasDuration());
            UNIT_ASSERT_VALUES_EQUAL(record.GetAvailabilityPeriod().GetDuration().seconds(), seconds);

            Ydb::Replication::DescribeReplicationResult proto;
            FillReplicationDescription(proto, record);
            const NYdb::NReplication::TReplicationDescription description(proto);
            UNIT_ASSERT(description.GetAvailabilityPeriodKind() ==
                NYdb::NReplication::TReplicationDescription::EAvailabilityPeriodKind::Finite);
            UNIT_ASSERT_VALUES_EQUAL(*description.GetAvailabilityPeriod(), TDuration::Seconds(seconds));
            const auto query = NYdb::NDump::BuildCreateReplicationQuery("/MyRoot", "/MyRoot", "Replication", description);
            UNIT_ASSERT_STRING_CONTAINS(query, TStringBuilder() << "AVAILABILITY_PERIOD = Interval('PT" << seconds << "S')");

            const auto stored = ReadSystemTable(runtime, controller, "Replications", {"Id"}, {"Config"});
            const auto& rows = stored.GetValue().GetStruct(0).GetOptional().GetStruct(0);
            UNIT_ASSERT_VALUES_EQUAL(rows.ListSize(), 1);
            NKikimrReplication::TReplicationConfig config;
            UNIT_ASSERT(config.ParseFromString(rows.GetList(0).GetStruct(0).GetOptional().GetBytes()));
            UNIT_ASSERT(!config.HasAvailabilityPeriod());
        };
        check();
        RebootTablet(runtime, controller, runtime.AllocateEdgeActor());
        check();
    }

    Y_UNIT_TEST(AvailabilityPeriodInvalidCreate) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;
        for (const TString period : {"", "Duration {}", "Duration { seconds: -1 }", "Duration { nanos: -1 }",
                "Duration { seconds: 1 nanos: -1 }", "Duration { nanos: 1000000000 }",
                "Duration { seconds: 315576000000 nanos: 999999999 }", "Duration { seconds: 315576000001 }", "Duration { seconds: 9223372036854775807 }"})
        {
            TestCreateReplication(runtime, ++txId, "/MyRoot", AvailabilityScheme("Replication", period),
                {NKikimrScheme::StatusInvalidParameter});
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
        }
        // Failed validation must not reserve the name or leave a partial object.
        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AvailabilityPeriodSqlIntervalBoundary) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;
        const auto lastValid = TDuration::MilliSeconds((NYql::NUdf::MAX_TIMESTAMP - 1) / 1000);
        const auto scheme = [&](const TString& name, TDuration value) {
            return AvailabilityScheme(name, TStringBuilder() << "Duration { seconds: " << value.Seconds()
                << " nanos: " << value.NanoSecondsOfSecond() << " }");
        };

        for (const auto value : {lastValid - TDuration::MicroSeconds(1), lastValid}) {
            const TString name = TStringBuilder() << "Valid" << txId;
            TestCreateReplication(runtime, ++txId, "/MyRoot", scheme(name, value));
            env.TestWaitNotification(runtime, txId);
            const auto desc = DescribePath(runtime, "/MyRoot/" + name);
            const auto& duration = desc.GetPathDescription().GetReplicationDescription()
                .GetConfig().GetAvailabilityPeriod().GetDuration();
            UNIT_ASSERT_VALUES_EQUAL(duration.seconds(), lastValid.Seconds());
            UNIT_ASSERT_VALUES_EQUAL(duration.nanos(), lastValid.NanoSecondsOfSecond());
        }

        for (const auto value : {lastValid + TDuration::MicroSeconds(1),
                TDuration::MicroSeconds(NYql::NUdf::MAX_TIMESTAMP), lastValid + TDuration::Days(1)})
        {
            TestCreateReplication(runtime, ++txId, "/MyRoot", scheme("Replication", value),
                {NKikimrScheme::StatusInvalidParameter});
            env.TestWaitNotification(runtime, txId);
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
        }

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(AvailabilityPeriodCreateOnly) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;
        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            State { Paused {} }
        )");
        env.TestWaitNotification(runtime, txId);
        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config { AvailabilityPeriod { Duration { seconds: 10 } } }
        )", {NKikimrScheme::StatusInvalidParameter});
        env.TestWaitNotification(runtime, txId);
        const auto desc = DescribePath(runtime, "/MyRoot/Replication");
        UNIT_ASSERT(desc.GetPathDescription().GetReplicationDescription().GetConfig().GetAvailabilityPeriod().HasUnlimited());
    }

    Y_UNIT_TEST(CreateWithoutInitialScan) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::PathExist,
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kPaused),
        });

        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            State { StandBy {} }
        )");
        env.TestWaitNotification(runtime, txId);
    }

    Y_UNIT_TEST(CreateWithoutInitialScanWithSameDestination) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        const auto scheme = [](const TString& name) {
            return Sprintf(R"(
                Name: "%s"
                Config {
                    SrcConnectionParams {
                        StaticCredentials { User: "user" Password: "pwd" }
                    }
                    ConsistencySettings { Global {} }
                    Specific {
                        Targets {
                            SrcPath: "/MyRoot/Source"
                            DstPath: "/MyRoot/Replica"
                        }
                    }
                }
            )", name.c_str());
        };

        TestCreateReplication(runtime, ++txId, "/MyRoot", scheme("Replication"));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::PathExist,
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kPaused),
        });

        // KIKIMR-27457: the paused replication must reserve its destination
        // even though the table has not been restored yet.
        TestCreateReplication(runtime, ++txId, "/MyRoot", scheme("Replication2"),
            {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST_TWIN(CreateWithSameDestinationInFlight, initialScan) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(initialScan);
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication", {"/MyRoot/Replica"}));
        const auto createTxId = txId;
        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication2", {"/MyRoot/Replica"}),
            {{NKikimrScheme::StatusPreconditionFailed, "/MyRoot/Replica"}});
        env.TestWaitNotification(runtime, createTxId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(CreateWithSameDestinationAfterRestart) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication", {"/MyRoot//Replica/"}));
        env.TestWaitNotification(runtime, txId);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication2", {"/MyRoot/Replica"}),
            {{NKikimrScheme::StatusPreconditionFailed, "/MyRoot/Replication"}});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(CreateWithDuplicateDestinations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication", {"/MyRoot/Replica", "/MyRoot//Replica/"}),
            {{NKikimrScheme::StatusInvalidParameter, "Duplicate replication destination path '/MyRoot/Replica'"}});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(CreateWithOverlappingDestinations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication", {"/MyRoot/Other", "/MyRoot/Replica"}));
        env.TestWaitNotification(runtime, txId);

        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication2", {"/MyRoot/Fresh", "/MyRoot/Replica"}),
            {{NKikimrScheme::StatusPreconditionFailed, "/MyRoot/Replica"}});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {NLs::PathNotExist});

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication3", {"/MyRoot/Fresh"}));
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication3"), {NLs::PathExist});
    }

    Y_UNIT_TEST_QUAD(CreateWithNestedDestinations, parentFirst, initialScan) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(initialScan);
        ui64 txId = 100;

        const TString parent = "/MyRoot/Dir";
        const TString child = "/MyRoot//Dir/sub/Table/";
        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication", {parentFirst ? parent : child}));
        const auto createTxId = txId;

        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication2", {parentFirst ? child : parent}),
            {{NKikimrScheme::StatusPreconditionFailed, "overlaps destination path"}});
        env.TestWaitNotification(runtime, createTxId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST_TWIN(CreateWithNestedDestinationAfterRestart, parentFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        const TString parent = "/MyRoot/Dir";
        const TString child = "/MyRoot/Dir/Table";
        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication", {parentFirst ? parent : child}));
        env.TestWaitNotification(runtime, txId);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication2", {parentFirst ? child : parent}),
            {{NKikimrScheme::StatusPreconditionFailed, "overlaps destination path"}});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST_TWIN(CreateWithNestedTargets, parentFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        const TString parent = "/MyRoot/Dir";
        const TString child = "/MyRoot//Dir/Table/";
        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication", parentFirst
                ? TVector<TString>{parent, child} : TVector<TString>{child, parent}),
            {{NKikimrScheme::StatusInvalidParameter, "Overlapping replication destination paths"}});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(CreateWithSiblingDestinations) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication", {"/MyRoot/Dir", "/MyRoot/Copy2", "/MyRoot/Shared/a"}));
        env.TestWaitNotification(runtime, txId);

        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication2", {"/MyRoot/Dir2", "/MyRoot/Copy", "/MyRoot/Shared/b"}));
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {NLs::PathExist});
    }

    Y_UNIT_TEST(CreateWithSameDestinationAfterDrop) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication", {"/MyRoot/Replica"}));
        env.TestWaitNotification(runtime, txId);
        TestDropReplication(runtime, ++txId, "/MyRoot", "Replication");
        env.TestWaitNotification(runtime, txId);

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication2", {"/MyRoot/Replica"}));
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {NLs::PathExist});
    }

    Y_UNIT_TEST(Disabled) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true).EnableReplication(false));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"),
            {NKikimrScheme::StatusPreconditionFailed});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::PathNotExist,
        });
    }

    Y_UNIT_TEST(CreateSequential) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        THashSet<ui64> controllerIds;

        for (int i = 0; i < 2; ++i) {
            const auto name = Sprintf("Replication%d", i);

            TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme(name));
            env.TestWaitNotification(runtime, txId);

            const auto desc = DescribePath(runtime, "/MyRoot/" + name);
            TestDescribeResult(desc, {
                NLs::PathExist,
                NLs::Finished,
            });

            controllerIds.insert(ExtractControllerId(desc.GetPathDescription()));
        }

        UNIT_ASSERT_VALUES_EQUAL(controllerIds.size(), 2);
    }

    Y_UNIT_TEST(CreateInParallel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        THashSet<ui64> controllerIds;

        for (int i = 0; i < 2; ++i) {
            TVector<TString> names;
            TVector<ui64> txIds;

            for (int j = 0; j < 2; ++j) {
                auto name = Sprintf("Replication%d-%d", i, j);

                TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme(name));

                names.push_back(std::move(name));
                txIds.push_back(txId);
            }

            env.TestWaitNotification(runtime, txIds);
            for (const auto& name : names) {
                const auto desc = DescribePath(runtime, "/MyRoot/" + name);
                TestDescribeResult(desc, {
                    NLs::PathExist,
                    NLs::Finished,
                });

                controllerIds.insert(ExtractControllerId(desc.GetPathDescription()));
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(controllerIds.size(), 4);
    }

    Y_UNIT_TEST(CreateDropRecreate) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);
        ui64 controllerId = 0;

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication");
            TestDescribeResult(desc, {NLs::PathExist});
            controllerId = ExtractControllerId(desc.GetPathDescription());
        }

        TestDropReplication(runtime, ++txId, "/MyRoot", "Replication");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication");
            TestDescribeResult(desc, {NLs::PathExist});
            UNIT_ASSERT_VALUES_UNEQUAL(controllerId, ExtractControllerId(desc.GetPathDescription()));
        }
    }

    Y_UNIT_TEST(CreateWithoutCredentials) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        const auto desc = DescribePath(runtime, "/MyRoot/Replication");
        const auto& params = desc.GetPathDescription().GetReplicationDescription().GetConfig().GetSrcConnectionParams();
        UNIT_ASSERT_VALUES_UNEQUAL("root@builtin", params.GetOAuthToken().GetToken());
    }

    Y_UNIT_TEST(ConsistencyLevel) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table1"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication1");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasRow());
        }

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication2"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table2"
                }
              }
              ConsistencySettings {
                Row {}
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication2");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasRow());
        }

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication3"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table3"
                }
              }
              ConsistencySettings {
                Global {
                  CommitIntervalMilliSeconds: 10000
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication3");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasGlobal());
            UNIT_ASSERT_VALUES_EQUAL(config.GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds(), 10000);
        }
    }

    Y_UNIT_TEST(CommitInterval) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table1"
                }
              }
              ConsistencySettings {
                Global {}
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication1");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasGlobal());
            UNIT_ASSERT_VALUES_EQUAL(config.GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds(), 10000);
        }

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication2"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table2"
                }
              }
              ConsistencySettings {
                Global {
                  CommitIntervalMilliSeconds: 15000
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication2");
            const auto& config = desc.GetPathDescription().GetReplicationDescription().GetConfig();
            UNIT_ASSERT(config.GetConsistencySettings().HasGlobal());
            UNIT_ASSERT_VALUES_EQUAL(config.GetConsistencySettings().GetGlobal().GetCommitIntervalMilliSeconds(), 15000);
        }
    }

    Y_UNIT_TEST(SecureMode) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config {
              SrcConnectionParams {
                CaCert: "-----BEGIN CERTIFICATE-----"
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config {
              SrcConnectionParams {
                EnableSsl: false
                CaCert: "-----BEGIN CERTIFICATE-----"
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )", {NKikimrScheme::StatusInvalidParameter});

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication1"
            Config {
              SrcConnectionParams {
                EnableSsl: true
                CaCert: "-----BEGIN CERTIFICATE-----"
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table1"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication1");
            const auto& params = desc.GetPathDescription().GetReplicationDescription().GetConfig().GetSrcConnectionParams();
            UNIT_ASSERT(params.GetEnableSsl());
            UNIT_ASSERT_VALUES_EQUAL(params.GetCaCert(), "-----BEGIN CERTIFICATE-----");
        }

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication2"
            Config {
              SrcConnectionParams {
                EnableSsl: true
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table2"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication2");
            const auto& params = desc.GetPathDescription().GetReplicationDescription().GetConfig().GetSrcConnectionParams();
            UNIT_ASSERT(params.GetEnableSsl());
            UNIT_ASSERT(!params.HasCaCert());
        }
    }

    Y_UNIT_TEST(Alter) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication"));
        env.TestWaitNotification(runtime, txId);
        {
            const auto desc = DescribePath(runtime, "/MyRoot/Replication");
            TestDescribeResult(desc, {NLs::PathExist});
        }

        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            State {
              Paused {
              }
            }
        )");

        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
          Name: "Replication"
          State {
            StandBy {
            }
          }
        )");

        TestAlterReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            State {
              Done {
                FailoverMode: FAILOVER_MODE_FORCE
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::PathExist,
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kDone),
        });
    }

    Y_UNIT_TEST(ResumeOwnDestination) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication", {"/MyRoot/Dir"}));
        env.TestWaitNotification(runtime, txId);
        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication2", {"/MyRoot/Dir2"}));
        env.TestWaitNotification(runtime, txId);

        TestAlterReplication(runtime, ++txId, "/MyRoot", StateScheme("Replication2", "StandBy"));
        env.TestWaitNotification(runtime, txId);
        TestAlterReplication(runtime, ++txId, "/MyRoot", StateScheme("Replication", "StandBy"));
        env.TestWaitNotification(runtime, txId);
        TestAlterReplication(runtime, ++txId, "/MyRoot", StateScheme("Replication", "Paused"));
        env.TestWaitNotification(runtime, txId);
        TestAlterReplication(runtime, ++txId, "/MyRoot", StateScheme("Replication", "StandBy"));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kStandBy),
        });
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kStandBy),
        });
    }

    Y_UNIT_TEST_TWIN(ResumeWithLegacyDuplicate, otherRunning) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication", {"/MyRoot/Shared"}));
        env.TestWaitNotification(runtime, txId);
        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication2", {"/MyRoot/Other"}));
        env.TestWaitNotification(runtime, txId);

        if (otherRunning) {
            TestAlterReplication(runtime, ++txId, "/MyRoot", StateScheme("Replication", "StandBy"));
            env.TestWaitNotification(runtime, txId);
        }

        SetLegacyReplicationDestination(runtime, "Replication2", "/MyRoot//Shared/");
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        auto expectConflict = [&] {
            TestAlterReplication(runtime, ++txId, "/MyRoot", StateScheme("Replication2", "StandBy"),
                {{NKikimrScheme::StatusPreconditionFailed, "used by replication '/MyRoot/Replication'"}});
            TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {
                NLs::ReplicationState(NKikimrReplication::TReplicationState::kPaused),
            });
        };
        expectConflict();

        if (otherRunning) {
            TestAlterReplication(runtime, ++txId, "/MyRoot", StateScheme("Replication", "Paused"));
            const auto pauseTxId = txId;
            // The conflicting configuration must also be found in AlterData.
            expectConflict();
            env.TestWaitNotification(runtime, pauseTxId);
            expectConflict();
        }

        TestDropReplication(runtime, ++txId, "/MyRoot", "Replication");
        env.TestWaitNotification(runtime, txId);
        TestAlterReplication(runtime, ++txId, "/MyRoot", StateScheme("Replication2", "StandBy"));
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kStandBy),
        });
    }

    Y_UNIT_TEST_TWIN(ResumeWithLegacyNestedDestinations, parentFirst) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        runtime.GetAppData().ReplicationConfig.SetInitialScan(false);
        ui64 txId = 100;

        const TString parent = "/MyRoot/Dir";
        const TString child = "/MyRoot/Dir/Table";
        TestCreateReplication(runtime, ++txId, "/MyRoot",
            DefaultScheme("Replication", {parentFirst ? parent : child}));
        env.TestWaitNotification(runtime, txId);
        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication2", {"/MyRoot/Other"}));
        env.TestWaitNotification(runtime, txId);
        TestAlterReplication(runtime, ++txId, "/MyRoot", StateScheme("Replication", "StandBy"));
        env.TestWaitNotification(runtime, txId);

        SetLegacyReplicationDestination(runtime, "Replication2", parentFirst ? child : parent);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        TestAlterReplication(runtime, ++txId, "/MyRoot", StateScheme("Replication2", "StandBy"),
            {{NKikimrScheme::StatusPreconditionFailed, "overlaps destination path"}});
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication2"), {
            NLs::ReplicationState(NKikimrReplication::TReplicationState::kPaused),
        });
    }

    Y_UNIT_TEST(Describe) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        TestCreateReplication(runtime, ++txId, "/MyRoot", DefaultScheme("Replication")); // default with user & password
        env.TestWaitNotification(runtime, txId);

        const auto desc = DescribePath(runtime, "/MyRoot/Replication");
        const auto& params = desc.GetPathDescription().GetReplicationDescription().GetConfig().GetSrcConnectionParams();
        UNIT_ASSERT(!params.GetStaticCredentials().HasPassword());
    }

    void CreateReplicatedTable(NKikimrSchemeOp::TTableReplicationConfig::EReplicationMode mode, const TUserAttrs& attrs) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", Sprintf(R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            ReplicationConfig {
              Mode: %s
            }
        )", NKikimrSchemeOp::TTableReplicationConfig::EReplicationMode_Name(mode).c_str()));
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::ReplicationMode(mode),
            NLs::UserAttrsEqual(attrs),
        });

        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::ReplicationMode(mode),
            NLs::UserAttrsEqual(attrs),
        });
    }

    Y_UNIT_TEST(CreateReplicatedTable) {
        CreateReplicatedTable(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE, {});
        CreateReplicatedTable(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY, {{"__async_replica", "true"}});
    }

    Y_UNIT_TEST(CannotAddReplicationConfig) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            ReplicationConfig {
              Mode: REPLICATION_MODE_READ_ONLY
            }
        )", {NKikimrScheme::StatusInvalidParameter});
    }

    Y_UNIT_TEST(CannotSetAsyncReplicaAttribute) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
        )", {NKikimrScheme::StatusInvalidParameter}, AlterUserAttrs({{"__async_replica", "true"}}));

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            ReplicationConfig {
              Mode: REPLICATION_MODE_READ_ONLY
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::UserAttrsHas({{"__async_replica", "true"}}),
        });

        TestUserAttrs(runtime, ++txId, "/MyRoot", "Table", {NKikimrScheme::StatusInvalidParameter},
            AlterUserAttrs({{"__async_replica", "true"}}, {}));

        TestUserAttrs(runtime, ++txId, "/MyRoot", "Table", {NKikimrScheme::StatusInvalidParameter},
            AlterUserAttrs({}, {"__async_replica"}));
    }

    Y_UNIT_TEST(AlterReplicatedTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            ReplicationConfig {
              Mode: REPLICATION_MODE_READ_ONLY
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestAlterTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            DropColumns { Name: "value" }
        )", {NKikimrScheme::StatusSchemeError});

        TestCreateCdcStream(runtime, ++txId, "/MyRoot", R"(
            TableName: "Table"
            StreamDescription {
              Name: "Stream"
              Mode: ECdcStreamModeKeysOnly
              Format: ECdcStreamFormatProto
            }
        )", {NKikimrScheme::StatusSchemeError});

        TestBuildIndex(runtime, ++txId, TTestTxConfig::SchemeShard, "/MyRoot", "/MyRoot/Table", "by_value", {"value"},
            Ydb::StatusIds::BAD_REQUEST);

        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(AlterTableRequest(++txId, "/MyRoot", R"(
            Name: "Table"
            ReplicationConfig {
              Mode: REPLICATION_MODE_NONE
              ConsistencyLevel: CONSISTENCY_LEVEL_ROW
            }
        )")));
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusInvalidParameter});

        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(AlterTableRequest(++txId, "/MyRoot", R"(
            Name: "Table"
            ReplicationConfig {
              Mode: REPLICATION_MODE_NONE
            }
        )")));
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePath(runtime, "/MyRoot/Table"), {
            NLs::ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE),
            NLs::UserAttrsEqual({}),
        });
    }

    Y_UNIT_TEST(AlterReplicatedIndexTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(CreateIndexedTableRequest(++txId, "/MyRoot", R"(
            TableDescription {
              Name: "Table"
              Columns { Name: "key" Type: "Uint64" }
              Columns { Name: "indexed" Type: "Uint64" }
              KeyColumnNames: ["key"]
              ReplicationConfig {
                Mode: REPLICATION_MODE_READ_ONLY
              }
            }
            IndexDescription {
              Name: "Index"
              KeyColumnNames: ["indexed"]
              IndexImplTableDescriptions: [ {
                ReplicationConfig {
                  Mode: REPLICATION_MODE_READ_ONLY
                }
              } ]
            }
        )")));
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Index/indexImplTable"), {
            NLs::ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY),
        });

        AsyncSend(runtime, TTestTxConfig::SchemeShard, InternalTransaction(AlterTableRequest(++txId, "/MyRoot/Table/Index", R"(
            Name: "indexImplTable"
            ReplicationConfig {
              Mode: REPLICATION_MODE_NONE
            }
        )")));
        TestModificationResults(runtime, txId, {NKikimrScheme::StatusAccepted});
        env.TestWaitNotification(runtime, txId);

        TestDescribeResult(DescribePrivatePath(runtime, "/MyRoot/Table/Index/indexImplTable"), {
            NLs::ReplicationMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE),
        });
    }

    Y_UNIT_TEST(CopyReplicatedTable) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        ui64 txId = 100;

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "Table"
            Columns { Name: "key" Type: "Uint64" }
            Columns { Name: "value" Type: "Uint64" }
            KeyColumnNames: ["key"]
            ReplicationConfig {
              Mode: REPLICATION_MODE_READ_ONLY
            }
        )");
        env.TestWaitNotification(runtime, txId);

        TestCreateTable(runtime, ++txId, "/MyRoot", R"(
            Name: "CopyTable"
            CopyFromTable: "/MyRoot/Table"
        )");
        env.TestWaitNotification(runtime, txId);

        const auto desc = DescribePath(runtime, "/MyRoot/CopyTable");
        const auto& table = desc.GetPathDescription().GetTable();
        UNIT_ASSERT(!table.HasReplicationConfig());
    }

    Y_UNIT_TEST(DropReplicationWithInvalidCredentials) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config {
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathExist});

        TestDropReplication(runtime, ++txId, "/MyRoot", "Replication");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
    }

    Y_UNIT_TEST(DropReplicationWithUnknownSecret) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().InitYdbDriver(true));
        ui64 txId = 100;

        SetupLogging(runtime);

        TestCreateReplication(runtime, ++txId, "/MyRoot", R"(
            Name: "Replication"
            Config {
              SrcConnectionParams {
                StaticCredentials {
                  User: "user"
                  PasswordSecretName: "unknown_secret"
                }
              }
              Specific {
                Targets {
                  SrcPath: "/MyRoot1/Table"
                  DstPath: "/MyRoot2/Table"
                }
              }
            }
        )");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathExist});

        TestDropReplication(runtime, ++txId, "/MyRoot", "Replication");
        env.TestWaitNotification(runtime, txId);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Replication"), {NLs::PathNotExist});
    }

} // TReplicationTests
