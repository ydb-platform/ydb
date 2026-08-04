#include <ydb/library/yaml_json/yaml_to_json.h>
#include <util/generic/string.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        YAML::Node yaml = YAML::Load(input.c_str());
        NKikimr::NYaml::Yaml2Json(yaml, true);
    } catch (...) {}
    return 0;
}
