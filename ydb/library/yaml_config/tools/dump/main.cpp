#include <ydb/library/yaml_config/deprecated/yaml_config_parser.h>
#include <ydb/library/yaml_config/tools/util/defaults.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>

#include <util/generic/string.h>
#include <util/stream/input.h>

auto main(int argc, char* argv[]) -> int {
    const TString yaml = Cin.ReadAll();
    if (argc == 2 && TString("--deprecated") == argv[1]) {
        NKikimrConfig::TAppConfig config;
        NKikimr::NYaml::NDeprecated::Parse(yaml, config);
        Cout << NKikimr::NYaml::NInternal::ProtoToJson(config);
        return 0;
    }
    Cout << NKikimr::NYaml::NInternal::ProtoToJson(NKikimr::NYaml::Parse(yaml));
    return 0;
}
