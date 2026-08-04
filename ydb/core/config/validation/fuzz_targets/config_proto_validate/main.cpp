// Fuzzer for NKikimr::NConfig::ValidateConfig — YDB cluster config validation.
// NKikimrConfig::TAppConfig is parsed from protobuf and then validated;
// the validators check cross-field invariants (auth config, column shard
// settings, monitoring, database config). Config arrives over gRPC Console
// service and is attacker-controllable in multi-tenant deployments.
// We parse proto first and skip non-proto inputs to avoid Y_ABORT_UNLESS paths.
#include <ydb/core/config/validation/validators.h>
#include <ydb/core/protos/config.pb.h>
#include <util/generic/string.h>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input(reinterpret_cast<const char*>(data), size);
    NKikimrConfig::TAppConfig config;
    if (!config.ParseFromString(input)) return 0;
    try {
        std::vector<TString> msgs;
        NKikimr::NConfig::ValidateConfig(config, msgs);
        NKikimr::NConfig::ValidateAuthConfig(config.GetAuthConfig(), msgs);
        NKikimr::NConfig::ValidateColumnShardConfig(config.GetColumnShardConfig(), msgs);
        NKikimr::NConfig::ValidateMonitoringConfig(config, msgs);
        NKikimr::NConfig::ValidateDatabaseConfig(config, msgs);
    } catch (...) {}
    return 0;
}
