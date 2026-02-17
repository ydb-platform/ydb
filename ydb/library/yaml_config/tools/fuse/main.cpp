#include <ydb/library/yaml_config/public/yaml_config.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>
#include <util/generic/string.h>

auto main(int argc, char* argv[]) -> int {
    NLastGetopt::TOpts opts;

    TString yamlConfigPath;
    TString consoleConfigPath;

    opts.AddLongOption("yaml-config", "Path to base YAML config (simple format)")
        .Required()
        .RequiredArgument("PATH")
        .StoreResult(&yamlConfigPath);

    opts.AddLongOption("console-config", "Path to console config (full format)")
        .Required()
        .RequiredArgument("PATH")
        .StoreResult(&consoleConfigPath);

    opts.SetFreeArgsNum(0);

    NLastGetopt::TOptsParseResult parseResult(&opts, argc, argv);

    try {
        TString baseYaml = TFileInput(yamlConfigPath).ReadAll();
        TString consoleYaml = TFileInput(consoleConfigPath).ReadAll();

        auto result = NKikimr::NYamlConfig::FuseConfigs(baseYaml, consoleYaml);

        Cout << result;

        return 0;

    } catch (const yexception& e) {
        Cerr << "Error: " << e.what() << Endl;
        return 1;
    }
}
