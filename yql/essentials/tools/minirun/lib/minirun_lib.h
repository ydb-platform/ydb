#pragma once

#include <yql/essentials/tools/yql_facade_run/yql_facade_run.h>

namespace NYql {

class TMiniRunTool: public TFacadeRunner {
public:
    explicit TMiniRunTool(TString toolName = "minirun");
};

} // namespace NYql
