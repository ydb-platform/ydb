#pragma once

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>
#include <yt/yql/providers/yt/fmr/worker/impl/yql_yt_worker_impl.h>

#include <yql/essentials/tools/yql_facade_run/yql_facade_run.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <yql/essentials/core/dq_integration/yql_dq_helper.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYql {

constexpr TStringBuf FastMapReduceGatewayName = "fmr";

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

protected:
    TString MrJobBin_;
    TString MrJobUdfsDir_;
    size_t NumThreads_ = 1;
    bool KeepTemp_ = false;
    TString DefYtServer_;
    NFmr::IFmrWorker::TPtr FmrWorker_;
};

} // NYql
