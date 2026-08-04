// Fuzzer for NKikimr::NYaml::BuildInitDistributedStorageCommand.
// This function is called when the admin submits a YAML-format storage
// configuration via the BSC (BlobStorageController) or admin HTTP UI.
// It parses YAML into NKikimrBlobStorage::TConfigRequest - a protobuf
// that controls physical storage layout. Bugs here affect critical infrastructure.
#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 256 * 1024) return 0;
    TString input(reinterpret_cast<const char*>(data), size);
    try {
        auto cmd = NKikimr::NYaml::BuildInitDistributedStorageCommand(input);
        (void)cmd;
    } catch (...) {}
    try {
        auto cmd = NKikimr::NYaml::BuildReplaceDistributedStorageCommand(input);
        (void)cmd;
    } catch (...) {}
    return 0;
}
