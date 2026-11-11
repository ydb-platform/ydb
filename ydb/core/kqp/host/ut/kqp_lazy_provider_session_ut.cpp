#include <ydb/core/kqp/common/simple/query_ref.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <yt/yql/providers/yt/provider/yql_yt_forwarding_gateway.h>
#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {
namespace {

class TCountingYtGateway final: public NYql::TYtForwardingGatewayBase {
public:
    using TYtForwardingGatewayBase::TYtForwardingGatewayBase;

    void OpenSession(TOpenSessionOptions&& options) override {
        ++Opened;
        TYtForwardingGatewayBase::OpenSession(std::move(options));
    }

    NThreading::TFuture<void> CloseSession(TCloseSessionOptions&& options) override {
        ++Closed;
        TYtForwardingGatewayBase::CloseSession(std::move(options)).GetValueSync();
        Closing = NThreading::NewPromise<void>();
        return Closing.GetFuture();
    }

    size_t Opened = 0;
    size_t Closed = 0;
    NThreading::TPromise<void> Closing;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpLazyProviderSessions) {
    Y_UNIT_TEST_TWIN(ReusedHostWaitsForOnlyActivatedSessionsOnSuccessAndFailure, Script) {
        TActorSystemStub actorSystem;
        auto registry = NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry());
        auto ytGateway = MakeIntrusive<TCountingYtGateway>(NYql::CreateYtFileGateway(NYql::NFile::TYtFileServices::Make(registry.Get())));
        const TString cluster = "local_ut";
        const TString database = "/Root";
        const TKqpSettings settings;
        const NKikimrConfig::TQueryServiceConfig queryServiceConfig;
        NYql::TExprContext moduleCtx;
        NYql::IModuleResolver::TPtr moduleResolver;
        UNIT_ASSERT(NYql::GetYqlDefaultModuleResolver(moduleCtx, moduleResolver));
        auto countersRoot = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto counters = MakeIntrusive<TKqpRequestCounters>();
        counters->Counters = new TKqpCounters(countersRoot);
        counters->TxProxyMon = new NTxProxy::TTxProxyMon(countersRoot);

        for (bool newRbo : {false, true}) {
            // Generic scripts currently require fallback to the legacy optimizer.
            if (Script && newRbo) {
                continue;
            }
            auto config = MakeIntrusive<NYql::TKikimrConfiguration>();
            config->Init(settings.DefaultSettings.GetDefaultSettings(), cluster, settings.Settings, true);
            config->SetEnableNewRBO(newRbo);
            auto gateway = CreateKikimrIcGateway(cluster,
                Script ? NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT : NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY,
                database, database, std::make_shared<TKqpTableMetadataLoader>(cluster, actorSystem.System.Get(), config),
                actorSystem.System.Get(), 0, counters, queryServiceConfig);
            auto host = CreateKqpHost(gateway, cluster, database, config, moduleResolver,
                TKqpFederatedQuerySetup{.HttpGateway = NYql::IHTTPGateway::Make(), .YtGateway = ytGateway},
                nullptr, std::make_shared<TGUCSettings>(), queryServiceConfig, Nothing(), registry.Get());

            struct TCase {
                TString Query;
                bool ActivatesYt;
                bool Success;
            };
            for (const auto& test : TVector<TCase>{
                     {"SELECT 1;", false, true},
                     {"PRAGMA yt.MaxInputTables = '3'; SELECT 1;", true, true},
                     {"PRAGMA yt.MaxInputTables = 'invalid'; SELECT 1;", true, false},
                     {"SELECT 1;", false, true},
                     {"PRAGMA yt.MaxInputTables = '3'; SELECT 1;", true, true}}) {
                const size_t opened = ytGateway->Opened;
                const size_t closed = ytGateway->Closed;
                IKqpHost::TPrepareSettings prepareSettings;
                prepareSettings.YqlSelect = newRbo ? NSQLTranslation::EYqlSelect::Force : NSQLTranslation::EYqlSelect::Disable;
                auto preparing = Script ? host->PrepareGenericScript(test.Query, prepareSettings)
                                        : host->PrepareGenericQuery(test.Query, prepareSettings);
                // Activation happens inside transformation, after the result has
                // captured its cleanup callback, rather than in host setup.
                UNIT_ASSERT_VALUES_EQUAL(ytGateway->Opened, opened);
                while (!preparing->HasResult()) {
                    auto next = preparing->Continue();
                    if (ytGateway->Closing.Initialized() && !ytGateway->Closing.HasValue()) {
                        UNIT_ASSERT(!preparing->HasResult());
                        UNIT_ASSERT(!next.HasValue());
                        ytGateway->Closing.SetValue();
                    }
                    next.GetValueSync();
                }
                const auto result = preparing->GetResult();
                UNIT_ASSERT_VALUES_EQUAL_C(result.Success(), test.Success,
                    TStringBuilder() << "newRbo=" << newRbo << ", query=" << test.Query << ": " << result.Issues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(ytGateway->Opened, opened + test.ActivatesYt);
                UNIT_ASSERT_VALUES_EQUAL(ytGateway->Closed, closed + test.ActivatesYt);
            }
        }
    }
}

} // namespace NKikimr::NKqp
