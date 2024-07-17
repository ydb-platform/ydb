#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>

#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>

#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>

#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <ydb/library/yql/providers/pq/gateway/dummy/yql_pq_dummy_gateway.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_provider.h>

#include <ydb/library/yql/providers/solomon/gateway/yql_solomon_gateway.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_provider.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

#include <ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>

#include <ydb/library/yql/providers/dq/local_gateway/yql_dq_gateway_local.h>

#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>

#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/stream/tee.h>
#include <util/string/cast.h>

namespace NYql {

NDq::IDqAsyncIoFactory::TPtr CreateAsyncIoFactory(const NYdb::TDriver& driver) {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterDqPqReadActorFactory(*factory, driver, nullptr);

    RegisterDqPqWriteActorFactory(*factory, driver, nullptr);
    return factory;
}

bool RunPqProgram(
    const TString& code,
    bool optimizeOnly,
    bool printExpr = false,
    bool printTrace = false,
    TString* errorsMessage = nullptr) {
    NLog::YqlLoggerScope logger("cerr", false);
    NLog::YqlLogger().SetComponentLevel(NLog::EComponent::Core, NLog::ELevel::DEBUG);
    NLog::YqlLogger().SetComponentLevel(NLog::EComponent::ProviderRtmr, NLog::ELevel::DEBUG);

    IOutputStream* errorsOutput = &Cerr;
    TMaybe<TStringOutput> errorsMessageOutput;
    TMaybe<TTeeOutput> tee;
    if (errorsMessage) {
        errorsMessageOutput.ConstructInPlace(*errorsMessage);
        tee.ConstructInPlace(&*errorsMessageOutput, &Cerr);
        errorsOutput = &*tee;
    }

    // Gateways config.
    TGatewaysConfig gatewaysConfig;
    // pq
    {
        auto& pqClusterConfig = *gatewaysConfig.MutablePq()->MutableClusterMapping()->Add();
        pqClusterConfig.SetName("lb");
        pqClusterConfig.SetClusterType(NYql::TPqClusterConfig::CT_PERS_QUEUE);
        pqClusterConfig.SetEndpoint("lb.ru");
        pqClusterConfig.SetConfigManagerEndpoint("cm.lb.ru");
        pqClusterConfig.SetTvmId(777);
    }

    // solomon
    {
        auto& solomonClusterConfig = *gatewaysConfig.MutableSolomon()->MutableClusterMapping()->Add();
        solomonClusterConfig.SetName("sol");
        solomonClusterConfig.SetCluster("sol.ru");
    }

    // dq
    {
        auto& dqCfg = *gatewaysConfig.MutableDq();
        auto* setting = dqCfg.AddDefaultSettings();
        setting->SetName("EnableComputeActor");
        setting->SetValue("1");
    }

    auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry())->Clone();
    TVector<TDataProviderInitializer> dataProvidersInit;

    // pq
    auto pqGateway = MakeIntrusive<TDummyPqGateway>();
    pqGateway->AddDummyTopic(TDummyTopic("lb", "my_in_topic"));
    pqGateway->AddDummyTopic(TDummyTopic("lb", "my_out_topic"));
    dataProvidersInit.push_back(GetPqDataProviderInitializer(std::move(pqGateway)));

    // solomon
    auto solomonGateway = CreateSolomonGateway(gatewaysConfig.GetSolomon());
    dataProvidersInit.push_back(GetSolomonDataProviderInitializer(std::move(solomonGateway)));

    // dq
    auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
        NYql::GetCommonDqFactory(),
        NKikimr::NMiniKQL::GetYqlFactory()
    });

    auto dqTaskTransformFactory = NYql::CreateCompositeTaskTransformFactory({
        NYql::CreateCommonDqTaskTransformFactory()
    });

    const auto driverConfig = NYdb::TDriverConfig().SetLog(CreateLogBackend("cerr"));
    NYdb::TDriver driver(driverConfig);
    auto dqGateway = CreateLocalDqGateway(functionRegistry.Get(), dqCompFactory, dqTaskTransformFactory, {}, false/*spilling*/, CreateAsyncIoFactory(driver));

    auto storage = NYql::CreateAsyncFileStorage({});
    dataProvidersInit.push_back(NYql::GetDqDataProviderInitializer(&CreateDqExecTransformer, dqGateway, dqCompFactory, {}, storage));

    TExprContext moduleCtx;
    IModuleResolver::TPtr moduleResolver;
    YQL_ENSURE(GetYqlDefaultModuleResolver(moduleCtx, moduleResolver));

    TProgramFactory factory(true, functionRegistry.Get(), 0ULL, dataProvidersInit, "ut");

    factory.SetGatewaysConfig(&gatewaysConfig);
    factory.SetModules(moduleResolver);

    TProgramPtr program = factory.Create("program", code);
    program->ConfigureYsonResultFormat(NYson::EYsonFormat::Text);

    Cerr << "Parse SQL..." << Endl;
    NSQLTranslation::TTranslationSettings sqlSettings;
    sqlSettings.SyntaxVersion = 1;
    sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
    sqlSettings.Flags.insert("DqEngineEnable");
    sqlSettings.Flags.insert("DqEngineForce");

    sqlSettings.ClusterMapping["lb"] = PqProviderName;
    sqlSettings.ClusterMapping["sol"] = SolomonProviderName;
    if (!program->ParseSql(sqlSettings)) {
        program->PrintErrorsTo(*errorsOutput);
        return false;
    }
    program->AstRoot()->PrettyPrintTo(Cerr, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);


    Cerr << "Compile..." << Endl;
    if (!program->Compile("user")) {
        program->PrintErrorsTo(*errorsOutput);
        return false;
    }

    auto exprOut = printExpr ? &Cout : nullptr;
    auto traceOpt = printTrace ? &Cerr : nullptr;

    TProgram::TStatus status = TProgram::TStatus::Error;
    if (optimizeOnly) {
        Cerr << "Optimize..." << Endl;
        status = program->Optimize("user", traceOpt, nullptr, exprOut);
    } else {
        Cerr << "Run..." << Endl;
        status = program->Run("user", traceOpt, nullptr, exprOut);
    }

    if (status == TProgram::TStatus::Error) {
        if (printTrace) {
            program->Print(traceOpt, nullptr);
        }
        program->PrintErrorsTo(*errorsOutput);
        return false;
    }

    driver.Stop(true);

    Cerr << "Done." << Endl;
    return true;
}

Y_UNIT_TEST_SUITE(YqlPqSimpleTests) {

    Y_UNIT_TEST(SelectWithNoSchema) {
        auto code = R"(
USE lb;
PRAGMA pq.Consumer="my_test_consumer";
INSERT INTO my_out_topic
SELECT Data FROM my_in_topic WHERE Data < "100";
        )";
        TString errorMessage;
        auto res = RunPqProgram(code, true, true, true, &errorMessage);
        UNIT_ASSERT_C(res, errorMessage);
    }

    Y_UNIT_TEST(SelectWithSchema) {
        auto code = R"(
USE lb;
PRAGMA pq.Consumer="my_test_consumer";

INSERT INTO my_out_topic
SELECT CAST(y as string) || x FROM lb.object(my_in_topic, "json") WITH SCHEMA (Int32 as y, String as x)
        )";
        TString errorMessage;
        auto res = RunPqProgram(code, true, true, true, &errorMessage);
        UNIT_ASSERT_C(res, errorMessage);
    }

    Y_UNIT_TEST(SelectStarWithSchema) {
        auto code = R"(
USE lb;
PRAGMA pq.Consumer="my_test_consumer";

$q = SELECT * FROM lb.object(my_in_topic, "json") WITH SCHEMA (Int32 as y, String as x);
INSERT INTO my_out_topic
SELECT x FROM $q
        )";
        TString errorMessage;
        auto res = RunPqProgram(code, true, true, true, &errorMessage);
        UNIT_ASSERT_C(res, errorMessage);
    }

}

} // NYql
