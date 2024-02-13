#include <ydb/library/yaml_config/deprecated/yaml_config_parser.h>
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_config/tools/util/defaults.h>

#include <util/generic/string.h>
#include <util/stream/input.h>

auto main(int argc, char* argv[]) -> int {
    const TString yaml = Cin.ReadAll();
    NKikimrBlobStorage::TConfigRequest config;
    if (argc == 2 && TString("--deprecated") == argv[1]) {
        config = NKikimr::NYaml::NDeprecated::BuildInitDistributedStorageCommand(yaml);
    } else {
        config = NKikimr::NYaml::BuildInitDistributedStorageCommand(yaml);
    }
    Cout << NKikimr::NYaml::NInternal::ProtoToJson(config);
    return 0;
}
