#include "yql_dq_statistics_json.h"

#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <yt/yql/providers/yt/lib/ut_common/yql_ut_common.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>

#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/providers/dq/local_gateway/yql_dq_gateway_local.h>
#include <ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>

#include <yql/essentials/core/cbo/simple/cbo_simple.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/file_storage/proto/file_storage.pb.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/utils/log/log.h>

#include <util/stream/tee.h>
#include <util/string/cast.h>

using namespace NYql;

namespace {
// Runs a YQL/SQL program through the DQ engine with a YT file data source.
// maxTasksPerOperation sets the DQ limit on tasks per query.
bool RunDqProgram(
    const TString& code,
    ui32 maxTasksPerOperation,
    const THashMap<TString, TString>& tableFiles,
    TString* errorsMessage = nullptr)
{
    NLog::YqlLoggerScope logger("cerr", false);

    IOutputStream* errorsOutput = &Cerr;
    TMaybe<TStringOutput> errorsMessageOutput;
    TMaybe<TTeeOutput> tee;
    if (errorsMessage) {
        errorsMessageOutput.ConstructInPlace(*errorsMessage);
        tee.ConstructInPlace(&*errorsMessageOutput, &Cerr);
        errorsOutput = &*tee;
    }

    TGatewaysConfig gatewaysConfig;
    {
        auto& dqCfg = *gatewaysConfig.MutableDq();
        auto addSetting = [&](const TString& name, const TString& value) {
            auto* s = dqCfg.AddDefaultSettings();
            s->SetName(name);
            s->SetValue(value);
        };
        addSetting("EnableComputeActor", "1");
        addSetting("MaxTasksPerOperation", ToString(maxTasksPerOperation));
    }

    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(
        NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone();

    TVector<TDataProviderInitializer> dataProvidersInit;

    auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), tableFiles);
    auto ytGateway = CreateYtFileGateway(yqlNativeServices);
    dataProvidersInit.push_back(
        GetYtNativeDataProviderInitializer(ytGateway, MakeSimpleCBOOptimizerFactory(), {}));

    auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
        NYql::GetCommonDqFactory(),
        NKikimr::NMiniKQL::GetYqlFactory()
    });
    auto dqTaskTransformFactory = NYql::CreateCompositeTaskTransformFactory({
        NYql::CreateCommonDqTaskTransformFactory()
    });
    auto dqGateway = CreateLocalDqGateway(
        functionRegistry.Get(), dqCompFactory, dqTaskTransformFactory,
        {}, false, MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>());

    auto storage = NYql::CreateAsyncFileStorage({});
    dataProvidersInit.push_back(
        NYql::GetDqDataProviderInitializer(
            &CreateDqExecTransformer, dqGateway, dqCompFactory, {}, storage));

    TExprContext moduleCtx;
    IModuleResolver::TPtr moduleResolver;
    YQL_ENSURE(GetYqlDefaultModuleResolver(moduleCtx, moduleResolver));

    TProgramFactory factory(true, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");
    factory.SetGatewaysConfig(&gatewaysConfig);
    factory.SetModules(moduleResolver);

    auto program = factory.Create("program", code);

    NSQLTranslation::TTranslationSettings sqlSettings;
    sqlSettings.SyntaxVersion = 1;
    sqlSettings.Flags.insert("DqEngineEnable");
    sqlSettings.Flags.insert("DqEngineForce");
    sqlSettings.ClusterMapping["plato"] = TString(YtProviderName);

    if (!program->ParseSql(sqlSettings)) {
        program->PrintErrorsTo(*errorsOutput);
        return false;
    }

    if (!program->Compile("user")) {
        program->PrintErrorsTo(*errorsOutput);
        return false;
    }

    auto status = program->Run("user", nullptr, nullptr, nullptr);
    if (status == TProgram::TStatus::Error) {
        program->PrintErrorsTo(*errorsOutput);
        return false;
    }

    return true;
}
}

Y_UNIT_TEST_SUITE(TestCommon) {

Y_UNIT_TEST(ParseCounterName) {
    TString prefix = "Prefix";
    std::map<TString, TString> labels = {
        {"Label1", "Value1"},
        {"Lavel2", "Value2"}
    };
    TString name = "CounterName";

    TString counterName = TCounters::GetCounterName(prefix, labels, name);

    TString prefix2, name2;
    std::map<TString, TString> labels2;

    NCommon::ParseCounterName(
        &prefix2, &labels2, &name2, counterName
    );

    UNIT_ASSERT_EQUAL(prefix, prefix2);
    UNIT_ASSERT_EQUAL(name, name2);
    UNIT_ASSERT_EQUAL(labels, labels2);
}

Y_UNIT_TEST(CollectTaskRunnerStatisticsByStage) {
    TOperationStatistics taskRunner;

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"Input", "2"},
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter1"),
            1, 2, 3, 4, 5
        )
    );

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"Output", "3"},
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter2"),
            1, 2, 3, 4, 5
        )
    );

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter3"),
            1, 2, 3, 4, 5
        )
    );

    TStringStream result;
    {
        NYson::TYsonWriter writer(&result, NYT::NYson::EYsonFormat::Pretty);
        CollectTaskRunnerStatisticsByStage(writer, taskRunner, true);
    }
    TString expected = R"__({
    "Stage=10" = {
        "Input" = {
            "total" = {
                "Counter1" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 0;
                    "max" = 2;
                    "min" = 3
                }
            }
        };
        "Output" = {
            "total" = {
                "Counter2" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 0;
                    "max" = 2;
                    "min" = 3
                }
            }
        };
        "Source" = {
            "total" = {}
        };
        "Sink" = {
            "total" = {}
        };
        "Task" = {
            "total" = {
                "Counter3" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 0;
                    "max" = 2;
                    "min" = 3
                }
            }
        }
    }
})__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, result.Str());
}

Y_UNIT_TEST(CollectTaskRunnerStatisticsByTask) {
    TOperationStatistics taskRunner;

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"InputChannel", "2"},
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter1"),
            1, 2, 3, 4, 5
        )
    );

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"OutputChannel", "3"},
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter2"),
            1, 2, 3, 4, 5
        )
    );

    taskRunner.Entries.push_back(
        TOperationStatistics::TEntry(
            TCounters::GetCounterName(
                "Prefix",
                {
                    {"Stage", "10"},
                    {"Task", "1"},
                },
                "Counter3"),
            1, 2, 3, 4, 5
        )
    );

    TStringStream result;
    {
        NYson::TYsonWriter writer(&result, NYT::NYson::EYsonFormat::Pretty);
        CollectTaskRunnerStatisticsByTask(writer, taskRunner);
    }
    TString expected = R"__({
    "Stage=10" = {
        "Task=1" = {
            "Input=2" = {
                "Counter1" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 4;
                    "max" = 2;
                    "min" = 3
                }
            };
            "Output=3" = {
                "Counter2" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 4;
                    "max" = 2;
                    "min" = 3
                }
            };
            "Generic" = {
                "Counter3" = {
                    "sum" = 1;
                    "count" = 5;
                    "avg" = 4;
                    "max" = 2;
                    "min" = 3
                }
            }
        }
    }
})__";
    UNIT_ASSERT_STRINGS_EQUAL(expected, result.Str());
}

}

Y_UNIT_TEST_SUITE(YqlDqExecTests) {

// Reading from a YT table creates DQ source stages. The number of stages
// serves as a lower bound for the number of tasks (at least one task per
// stage). When stagesCount exceeds maxTasksPerOperation and no fallback is
// allowed (DqEngineForce), the exec transformer must report a user-friendly
// stages error message rather than crashing with YQL_ENSURE.
Y_UNIT_TEST(TooManyStagesErrorMessage) {
    const TString code = R"(
USE plato;
SELECT key FROM Input;
    )";

    TTestTablesMapping testTables;
    THashMap<TString, TString> tableFiles = {
        {"yt.plato.Input",  testTables.TmpInput.Name()},
        {"yt.plato.Output", testTables.TmpOutput.Name()},
    };

    TString errorMessage;
    bool ok = RunDqProgram(code, /*maxTasksPerOperation=*/0, tableFiles, &errorMessage);
    UNIT_ASSERT_C(!ok, "Expected failure: too many stages");
    UNIT_ASSERT_STRING_CONTAINS(errorMessage, "stages exceeds the limit");
}
}
