#include "yqlrun_lib.h"

#include <yt/yql/providers/yt/provider/yql_yt_provider_impl.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/cbo/simple/cbo_simple.h>

#include <util/generic/yexception.h>
#include <util/folder/iterator.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/output.h>

namespace {

class TPeepHolePipelineConfigurator : public NYql::IPipelineConfigurator {
public:
    TPeepHolePipelineConfigurator() = default;

    void AfterCreate(NYql::TTransformationPipeline* pipeline) const final {
        Y_UNUSED(pipeline);
    }

    void AfterTypeAnnotation(NYql::TTransformationPipeline* pipeline) const final {
        Y_UNUSED(pipeline);
    }

    void AfterOptimize(NYql::TTransformationPipeline* pipeline) const final {
        pipeline->Add(NYql::CreateYtWideFlowTransformer(nullptr), "WideFlow");
        pipeline->Add(NYql::CreateYtBlockInputTransformer(nullptr), "BlockInput");
        pipeline->Add(NYql::MakePeepholeOptimization(pipeline->GetTypeAnnotationContext()), "PeepHole");
        pipeline->Add(NYql::CreateYtBlockOutputTransformer(nullptr), "BlockOutput");
    }
};

TPeepHolePipelineConfigurator PEEPHOLE_CONFIG_INSTANCE;

} // unnamed

namespace NYql {

TYqlRunTool::TYqlRunTool()
    : TFacadeRunner("yqlrun")
{
    GetRunOptions().UseRepeatableRandomAndTimeProviders = true;
    GetRunOptions().ResultsFormat = NYson::EYsonFormat::Pretty;

    GetRunOptions().AddOptExtension([this](NLastGetopt::TOpts& opts) {
        opts.AddLongOption('t', "table", "Table mapping").RequiredArgument("table@file")
            .KVHandler([&](TString name, TString path) {
                if (name.empty() || path.empty()) {
                    throw yexception() << "Incorrect table mapping, expected form table@file, e.g. yt.plato.Input@input.txt";
                }
                TablesMapping_[name] = path;
            }, '@');

        opts.AddLongOption("tables-dir", "Table dirs mapping").RequiredArgument("cluster@dir")
            .KVHandler([&](TString cluster, TString dir) {
                if (cluster.empty() || dir.empty()) {
                    throw yexception() << "Incorrect table directory mapping, expected form cluster@dir, e.g. yt.plato@/tmp/tables";
                }
                TablesDirMapping_[cluster] = dir;
                for (const auto& entry: TDirIterator(TFsPath(dir))) {
                    if (auto entryPath = TFsPath(entry.fts_path); entryPath.IsFile() && entryPath.GetExtension() == "txt") {
                        auto tableName = TString(cluster).append('.').append(entryPath.RelativeTo(TFsPath(dir)).GetPath());
                        tableName = tableName.substr(0, tableName.size() - 4); // remove .txt extension
                        TablesMapping_[tableName] = entryPath.GetPath();
                    }
                }
            }, '@');

        opts.AddLongOption('C', "cluster", "Cluster to service mapping").RequiredArgument("name@service")
            .KVHandler([&](TString cluster, TString provider) {
                if (cluster.empty() || provider.empty()) {
                    throw yexception() << "Incorrect service mapping, expected form cluster@provider, e.g. plato@yt";
                }
                AddClusterMapping(std::move(cluster), std::move(provider));
            }, '@');

        opts.AddLongOption("ndebug", "Do not show debug info in error output").NoArgument().SetFlag(&GetRunOptions().NoDebug);
        opts.AddLongOption("keep-temp", "Keep temporary tables").NoArgument().SetFlag(&KeepTemp_);
        opts.AddLongOption("show-progress", "Report operation progress").NoArgument()
            .Handler0([&]() {
                SetOperationProgressWriter([](const TOperationProgress& progress) {
                    Cerr << "Operation: [" << progress.Category << "] " << progress.Id << ", state: " << progress.State << "\n";
                });
            });
        opts.AddLongOption("tmp-dir", "Directory for temporary tables").RequiredArgument("DIR").StoreResult(&TmpDir_);
        opts.AddLongOption("test-format", "Compare formatted query's AST with the original query's AST (only syntaxVersion=1 is supported)").NoArgument().SetFlag(&GetRunOptions().TestSqlFormat);
        opts.AddLongOption("validate-result-format", "Check that result-format can parse Result").NoArgument().SetFlag(&GetRunOptions().ValidateResultFormat);
    });

    GetRunOptions().AddOptHandler([this](const NLastGetopt::TOptsParseResult& res) {
        Y_UNUSED(res);

        if (GetRunOptions().GatewaysConfig) {
            auto ytConfig = GetRunOptions().GatewaysConfig->GetYt();
            FillClusterMapping(ytConfig, TString{YtProviderName});
        }
    });

    GetRunOptions().SetSupportedGateways({TString{YtProviderName}});
    GetRunOptions().GatewayTypes.emplace(YtProviderName);
    AddClusterMapping(TString{"plato"}, TString{YtProviderName});

    AddProviderFactory([this]() -> NYql::TDataProviderInitializer {
        auto yqlNativeServices = NFile::TYtFileServices::Make(GetFuncRegistry().Get(), TablesMapping_, GetFileStorage(), TmpDir_, KeepTemp_, TablesDirMapping_);
        auto ytNativeGateway = CreateYtFileGateway(yqlNativeServices);
        return GetYtNativeDataProviderInitializer(ytNativeGateway, MakeSimpleCBOOptimizerFactory(), {});
    });

    SetPeepholePipelineConfigurator(&PEEPHOLE_CONFIG_INSTANCE);

}


} // NYql
