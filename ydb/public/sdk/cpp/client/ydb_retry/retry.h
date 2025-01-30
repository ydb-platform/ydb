#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>
#include <util/datetime/base.h>

namespace NYdb::inline V2::NRetry {

struct TBackoffSettings {
    using TSelf = TBackoffSettings;

    FLUENT_SETTING_DEFAULT_DEPRECATED(TDuration, SlotDuration, TDuration::Seconds(1));
    FLUENT_SETTING_DEFAULT_DEPRECATED(ui32, Ceiling, 6);
    FLUENT_SETTING_DEFAULT_DEPRECATED(double, UncertainRatio, 0.5);
};

struct TRetryOperationSettings {
    using TSelf = TRetryOperationSettings;

    FLUENT_SETTING_DEFAULT_DEPRECATED(ui32, MaxRetries, 10);
    FLUENT_SETTING_DEFAULT_DEPRECATED(bool, RetryNotFound, true);
    FLUENT_SETTING_DEFAULT_DEPRECATED(TDuration, GetSessionClientTimeout, TDuration::Seconds(5));
    FLUENT_SETTING_DEFAULT_DEPRECATED(TDuration, MaxTimeout, TDuration::Max());
    FLUENT_SETTING_DEFAULT_DEPRECATED(TBackoffSettings, FastBackoffSettings, DefaultFastBackoffSettings());
    FLUENT_SETTING_DEFAULT_DEPRECATED(TBackoffSettings, SlowBackoffSettings, DefaultSlowBackoffSettings());
    FLUENT_SETTING_FLAG_DEPRECATED(Idempotent);
    FLUENT_SETTING_FLAG_DEPRECATED(Verbose);
    FLUENT_SETTING_FLAG_DEPRECATED(RetryUndefined);

    static TBackoffSettings DefaultFastBackoffSettings() {
        return TBackoffSettings()
            .Ceiling(10)
            .SlotDuration(TDuration::MilliSeconds(5))
            .UncertainRatio(0.5);
    }

    static TBackoffSettings DefaultSlowBackoffSettings() {
        return TBackoffSettings()
            .Ceiling(6)
            .SlotDuration(TDuration::Seconds(1))
            .UncertainRatio(0.5);
    }
};

} // namespace NYdb::NRetry
