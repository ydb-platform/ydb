#pragma once

#include <yql/essentials/tools/yql_facade_run/yql_facade_run.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NYql {

class TYqlRunTool: public TFacadeRunner {
public:
    TYqlRunTool();

private:
    THashMap<TString, TString> TablesMapping_;
    THashMap<TString, TString> TablesDirMapping_;
    bool KeepTemp_ = false;
    TString TmpDir_;
};

} // NYql
