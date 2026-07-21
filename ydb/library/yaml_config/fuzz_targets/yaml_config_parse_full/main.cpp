// Fuzzer for the full YAML cluster config parser (NKikimr::NYaml::Parse).
// This is the same entry point used when processing a new config submitted
// via the admin gRPC ConsoleService / ReplaceYamlConfig RPC, and through
// the DynamicConfig tablet. An attacker with admin rights (or a
// misconfigured endpoint) can supply arbitrary YAML.
// The parser does extensive proto/JSON field traversal and type coercions.
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 512 * 1024) return 0; // guard against OOM
    TString input(reinterpret_cast<const char*>(data), size);
    try {
        auto config = NKikimr::NYaml::Parse(input, /*transform=*/false);
        (void)config;
    } catch (...) {}
    return 0;
}
