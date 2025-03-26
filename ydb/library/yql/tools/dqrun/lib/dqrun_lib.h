#pragma once

#include <yt/yql/tools/ytrun/lib/ytrun_lib.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>

#include <yql/essentials/tools/yql_facade_run/yql_facade_run.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <yql/essentials/core/dq_integration/yql_dq_helper.h>
#include <yql/essentials/core/dq_integration/transform/yql_dq_task_transform.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/utils/log/log_level.h>

#include <ydb/library/yql/providers/generic/connector/libcpp/client.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>
#include <ydb/library/yql/providers/dq/interface/yql_dq_task_preprocessor.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/library/actors/core/log_iface.h>
#include <ydb/library/yql/utils/actor_system/manager.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NFq::NConfig {
    class TConfig;
}

namespace NYql {

class TDqRunTool: public TYtRunTool {
public:
    TDqRunTool();
    ~TDqRunTool();

protected:
    struct TActorIds;

    TProgram::TStatus DoRunProgram(TProgramPtr program) override;
    IOptimizerFactory::TPtr CreateCboFactory() override;
    IDqHelper::TPtr CreateDqHelper() override;
    IYtGateway::TPtr CreateYtGateway() override;

    NYdb::TDriver GetYdbDriver();
    NYql::NFile::TYtFileServices::TPtr GetYtFileServices();
    IHTTPGateway::TPtr GetHttpGateway();
    IMetricsRegistryPtr GetMetricsRegistry();
    static NActors::NLog::EPriority YqlToActorsLogLevel(NYql::NLog::ELevel yqlLevel);
    std::tuple<std::unique_ptr<TActorSystemManager>, TActorIds> RunActorSystem();
    NYql::IDatabaseAsyncResolver::TPtr GetDbResolver();
    NConnector::IClient::TPtr GetGenericClient();
    IPqGateway::TPtr GetPqGateway();
    TVector<std::pair<TActorId, TActorSetupCmd>> GetFqServices();
    NKikimr::NMiniKQL::TComputationNodeFactory CreateCompNodeFactory();
    NYql::TTaskTransformFactory CreateDqTaskTransformFactory();
    NYql::TDqTaskPreprocessorFactoryCollection CreateDqTaskPreprocessorFactories();
    NYql::NDq::IDqAsyncIoFactory::TPtr CreateAsyncIoFactory();

protected:
    bool AnalyzeQuery_ = false;
    bool NoForceDq_ = false;
    bool EmulateYt_ = false;
    TMaybe<TString> DqHost_;
    TMaybe<int> DqPort_;
    int DqThreads_ = 16;
    bool EnableSpilling_ = false;

    IOutputStream* MetricsStream_ = nullptr;
    THolder<IOutputStream> MetricsStreamHolder_;

    TString MrJobBin_;
    TString MrJobUdfsDir_;
    bool KeepTemp_ = false;
    TString TmpDir_;
    THashMap<TString, TString> TablesMapping_;

    THashMap<TString, TString> TopicMapping_;
    THolder<NFq::NConfig::TConfig> FqConfig_;

    ISecuredServiceAccountCredentialsFactory::TPtr CredentialsFactory_;
    TMaybe<NYdb::TDriver> Driver_;
    NFile::TYtFileServices::TPtr YtFileServices_;
    IHTTPGateway::TPtr HttpGateway_;
    IMetricsRegistryPtr MetricsRegistry_;
    NYql::IDatabaseAsyncResolver::TPtr DbResolver_;
    NConnector::IClient::TPtr GenericClient_;
    IPqGateway::TPtr PqGateway_;
};

}
