#pragma once

#include <yql/essentials/tools/yql_facade_run/yql_facade_run.h>
#include <yql/essentials/sql/sql.h>

#include <library/cpp/getopt/last_getopt.h>

#include <memory>

namespace NSparkTool {
    struct TSparkSettings;
}

namespace NYql {

void InitSparkSettings(std::shared_ptr<NSparkTool::TSparkSettings>& settings);
void AddSparkOptions(NLastGetopt::TOpts& opts, std::shared_ptr<NSparkTool::TSparkSettings>& settings, bool withSyntax);
void ValidateSparkSettings(std::shared_ptr<NSparkTool::TSparkSettings>& settings);
void ApplySparkSettings(TFacadeRunOptions& runOpts, std::shared_ptr<NSparkTool::TSparkSettings>& settings);
void AddSparkTranslator(NSQLTranslation::TTranslatorsRegistry& translatorsRegistry, std::shared_ptr<NSparkTool::TSparkSettings>& settings);

}
