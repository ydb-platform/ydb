#include "config.h"

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

void TPartitionReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_row_count", &TThis::MaxRowCount)
        .Default(1000);
    registrar.Parameter("max_data_weight", &TThis::MaxDataWeight)
        .Default(16_MB);
    registrar.Parameter("data_weight_per_row_hint", &TThis::DataWeightPerRowHint)
        .Default();

    registrar.Parameter("use_native_tablet_node_api", &TThis::UseNativeTabletNodeApi)
        .Default(false);
    registrar.Parameter("use_pull_consumer", &TThis::UsePullConsumer)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->UsePullConsumer && !config->UseNativeTabletNodeApi) {
            THROW_ERROR_EXCEPTION("PullConsumer can only be used with the native tablet node api for pulling rows");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TQueueAutoTrimConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("retained_rows", &TThis::RetainedRows)
        .Default();
    registrar.Parameter("retained_lifetime_duration", &TThis::RetainedLifetimeDuration)
        .Default();

    registrar.Postprocessor([] (TThis* trimConfig) {
        if (!trimConfig->Enable) {
            if (trimConfig->RetainedLifetimeDuration) {
                THROW_ERROR_EXCEPTION("Autotrimming is disabled, option \"retained_lifetime_duration\" can only be used while autotrimming is enabled");
            }
            if (trimConfig->RetainedRows) {
                THROW_ERROR_EXCEPTION("Autotrimming is disabled, option \"retained_rows\" can only be used while autotrimming is enabled");
            }
        }

        if (trimConfig->RetainedLifetimeDuration && trimConfig->RetainedLifetimeDuration->GetValue() % TDuration::Seconds(1).GetValue() != 0) {
            THROW_ERROR_EXCEPTION("The value of \"retained_lifetime_duration\" must be a multiple of 1000 (1 second)");
        }
    });
}

bool operator==(const TQueueAutoTrimConfig& lhs, const TQueueAutoTrimConfig& rhs)
{
    return lhs.Enable == rhs.Enable && lhs.RetainedRows == rhs.RetainedRows;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
