#pragma once

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>
#include <yt/yql/providers/yt/lib/secret_masker/secret_masker.h>

#include <yql/essentials/tools/yql_facade_run/yql_facade_run.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <yql/essentials/core/dq_integration/yql_dq_helper.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYql {

class TYtRunTool: public TFacadeRunner {
public:
    TYtRunTool(TString name = "ytrun");
    ~TYtRunTool() = default;

protected:
    int DoMain(int argc, const char *argv[]) override;
    TProgram::TStatus DoRunProgram(TProgramPtr program) override;

    virtual IYtGateway::TPtr CreateYtGateway();
    virtual IOptimizerFactory::TPtr CreateCboFactory();
    virtual IDqHelper::TPtr CreateDqHelper();
    virtual ISecretMasker::TPtr CreateSecretMasker();

protected:
    TString MrJobBin_;
    TString MrJobUdfsDir_;
    size_t NumYtThreads_ = 1;
    bool KeepTemp_ = false;
    TString DefYtServer_;
    NFmr::IFmrWorker::TPtr FmrWorker_;
    TString FmrCoordinatorServerUrl_;
    bool DisableLocalFmrWorker_ = false;
    TString FmrOperationSpecFilePath_;
    TString TableDataServiceDiscoveryFilePath_;
    TString FmrJobBin_;
};

} // NYql
