#pragma once

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <yql/essentials/tools/yql_facade_run/yql_facade_run.h>
#ifndef DONT_ADD_SPARK
#include <yql/spark/tools/tool_lib/tool_lib.h>
#endif

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYql {

class TYqlRunTool: public TFacadeRunner {
public:
    TYqlRunTool();

protected:
    virtual IOptimizerFactory::TPtr CreateCboFactory();
    int DoRun(TProgramFactory& factory) override;

    virtual IYtGateway::TPtr CreateYtGateway();

private:
    THashMap<TString, TString> TablesMapping_;
    THashMap<TString, TString> TablesDirMapping_;
    bool KeepTemp_ = false;
    TString TmpDir_;
#ifndef DONT_ADD_SPARK
    NSparkTool::TSparkSettings SparkSettings_;
#endif
};

} // NYql
