#include "yqlrun_lib_spark.h"

namespace NYql {

void InitSparkSettings(std::shared_ptr<NSparkTool::TSparkSettings>& settings) {
    Y_UNUSED(settings);
}

void AddSparkOptions(NLastGetopt::TOpts& opts, std::shared_ptr<NSparkTool::TSparkSettings>& settings, bool withSyntax) {
    Y_UNUSED(opts);
    Y_UNUSED(settings);
    Y_UNUSED(withSyntax);
}

void ValidateSparkSettings(std::shared_ptr<NSparkTool::TSparkSettings>& settings) {
    Y_UNUSED(settings);
}

void ApplySparkSettings(TFacadeRunOptions& runOpts, std::shared_ptr<NSparkTool::TSparkSettings>& settings) {
    Y_UNUSED(runOpts);
    Y_UNUSED(settings);
}

void AddSparkTranslator(NSQLTranslation::TTranslatorsRegistry& translatorsRegistry, std::shared_ptr<NSparkTool::TSparkSettings>& settings) {
    Y_UNUSED(translatorsRegistry);
    Y_UNUSED(settings);
}

}
