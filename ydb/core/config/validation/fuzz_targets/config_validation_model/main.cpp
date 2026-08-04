#include <ydb/core/config/validation/validators.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/config.pb.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/yassert.h>

namespace {

using NKikimr::NConfig::EValidationResult;

constexpr size_t MaxRawProto = 1024;

TString Duration(FuzzedDataProvider& provider) {
    const TString valid[] = {"0s", "1s", "10s", "1m", "1h", "24h"};
    if (provider.ConsumeBool()) {
        return valid[provider.ConsumeIntegralInRange<size_t>(0, std::size(valid) - 1)];
    }
    TString invalid = provider.ConsumeRandomLengthString(provider.ConsumeIntegralInRange<size_t>(0, 16));
    if (invalid.empty()) {
        invalid = "bad";
    }
    return invalid;
}

NKikimrProto::TAuthConfig MakeAuth(FuzzedDataProvider& provider, bool* expectedError) {
    NKikimrProto::TAuthConfig auth;
    *expectedError = false;

    if (provider.ConsumeBool()) {
        auto* complexity = auth.MutablePasswordComplexity();
        const ui32 lower = provider.ConsumeIntegralInRange<ui32>(0, 8);
        const ui32 upper = provider.ConsumeIntegralInRange<ui32>(0, 8);
        const ui32 numbers = provider.ConsumeIntegralInRange<ui32>(0, 8);
        const ui32 special = provider.ConsumeIntegralInRange<ui32>(0, 8);
        const ui32 minLength = provider.ConsumeIntegralInRange<ui32>(0, 32);
        complexity->SetMinLowerCaseCount(lower);
        complexity->SetMinUpperCaseCount(upper);
        complexity->SetMinNumbersCount(numbers);
        complexity->SetMinSpecialCharsCount(special);
        complexity->SetMinLength(minLength);
        *expectedError |= minLength < lower + upper + numbers + special;
    }

    if (provider.ConsumeBool()) {
        auto* lockout = auth.MutableAccountLockout();
        lockout->SetAttemptThreshold(provider.ConsumeIntegralInRange<ui32>(0, 128));
        const TString duration = Duration(provider);
        lockout->SetAttemptResetDuration(duration);
        TDuration parsed;
        *expectedError |= !TDuration::TryParse(duration, parsed);
    }

    return auth;
}

NKikimrConfig::TAppConfig MakeMonitoringConfig(FuzzedDataProvider& provider, bool* expectedError) {
    NKikimrConfig::TAppConfig config;
    const bool requireCounters = provider.ConsumeBool();
    const bool requireHealth = provider.ConsumeBool();
    const bool enforce = provider.ConsumeBool();

    auto* monitoring = config.MutableMonitoringConfig();
    monitoring->SetRequireCountersAuthentication(requireCounters);
    monitoring->SetRequireHealthcheckAuthentication(requireHealth);

    if (enforce) {
        config.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
    }
    *expectedError = (requireCounters || requireHealth) && !enforce;
    return config;
}

void CheckResult(EValidationResult result) {
    Y_ABORT_UNLESS(result == EValidationResult::Ok
        || result == EValidationResult::Warn
        || result == EValidationResult::Error);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider provider(data, size);

    bool authError = false;
    const auto auth = MakeAuth(provider, &authError);
    std::vector<TString> authMessages;
    const auto authResult = NKikimr::NConfig::ValidateAuthConfig(auth, authMessages);
    CheckResult(authResult);
    Y_ABORT_UNLESS((authResult == EValidationResult::Error) == authError);
    Y_ABORT_UNLESS(authMessages.empty() || authResult != EValidationResult::Ok);

    bool monitoringError = false;
    const auto monitoringConfig = MakeMonitoringConfig(provider, &monitoringError);
    std::vector<TString> monitoringMessages;
    const auto monitoringResult = NKikimr::NConfig::ValidateMonitoringConfig(monitoringConfig, monitoringMessages);
    CheckResult(monitoringResult);
    Y_ABORT_UNLESS((monitoringResult == EValidationResult::Error) == monitoringError);
    Y_ABORT_UNLESS(monitoringMessages.empty() || monitoringResult != EValidationResult::Ok);

    NKikimrConfig::TAppConfig appConfig;
    *appConfig.MutableAuthConfig() = auth;
    if (provider.ConsumeBool()) {
        *appConfig.MutableMonitoringConfig() = monitoringConfig.GetMonitoringConfig();
    }
    std::vector<TString> configMessages;
    CheckResult(NKikimr::NConfig::ValidateConfig(appConfig, configMessages));

    NKikimrConfig::TAppConfig raw;
    const size_t rawSize = std::min(size, MaxRawProto);
    if (raw.ParseFromArray(data, rawSize)) {
        std::vector<TString> rawMessages;
        CheckResult(NKikimr::NConfig::ValidateAuthConfig(raw.GetAuthConfig(), rawMessages));
        rawMessages.clear();
        CheckResult(NKikimr::NConfig::ValidateColumnShardConfig(raw.GetColumnShardConfig(), rawMessages));
        rawMessages.clear();
        CheckResult(NKikimr::NConfig::ValidateMonitoringConfig(raw, rawMessages));
    }

    return 0;
}
