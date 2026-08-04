#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        NKikimr::NYaml::Parse(input);
    } catch (...) {}
    return 0;
}
