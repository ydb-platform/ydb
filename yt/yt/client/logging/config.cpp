#include "config.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableLogWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("table_path", &TThis::TablePath);

    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("low_backlog_weight_watermark", &TThis::LowBacklogWeightWatermark)
        .GreaterThanOrEqual(0)
        .Default(1_MB);
    registrar.Parameter("high_backlog_weight_watermark", &TThis::HighBacklogWeightWatermark)
        .GreaterThanOrEqual(0)
        .Default(16_MB);

    registrar.Parameter("max_batch_row_count", &TThis::MaxBatchRowCount)
        .GreaterThanOrEqual(0)
        .Default(50'000);
    registrar.Parameter("max_batch_weight", &TThis::MaxBatchWeight)
        .GreaterThanOrEqual(0)
        .Default(2_MB);

    registrar.Parameter("write_backoff", &TThis::WriteBackoff)
        .Default({
            .InvocationCount = std::numeric_limits<int>::max(),
            .MinBackoff = TDuration::MilliSeconds(100),
            .MaxBackoff = TDuration::Seconds(5),
        });

    registrar.Parameter("type_conversion", &TThis::TypeConversion)
        .DefaultNew();
    registrar.Parameter("insert_rows_format", &TThis::InsertRowsFormat)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->Type = WriterType;
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->LowBacklogWeightWatermark > config->HighBacklogWeightWatermark) {
            THROW_ERROR_EXCEPTION("Value of \"low_backlog_weight_watermark\" cannot be greater than \"high_backlog_weight_watermark\"")
                << TErrorAttribute("low_backlog_weight_watermark", config->LowBacklogWeightWatermark)
                << TErrorAttribute("high_backlog_weight_watermark", config->HighBacklogWeightWatermark);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
