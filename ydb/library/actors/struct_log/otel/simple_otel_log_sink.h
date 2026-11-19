#pragma once

#include "basic_otel_log_sink.h"

namespace NActors::NStructuredLog {

/// OTLP log sink that encodes the full log line in the record body (no per-field OTEL attributes).
class TSimpleOtelLogSink : public TBaseOtelLogSink {
public:
    explicit TSimpleOtelLogSink(TBaseOtelLogSink::TOtelSettings settings);

protected:
    void FillLogRecordAttributes(
        google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs,
        const TLogMessage& message) override;
    void FillScopeAttributes(
        google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs) override;
    void FillResourceAttributes(
        google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs) override;
};

}  // namespace NActors::NStructuredLog