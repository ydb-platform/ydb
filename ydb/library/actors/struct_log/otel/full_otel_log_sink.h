#pragma once

#include "basic_otel_log_sink.h"

namespace NActors::NStructuredLog {

/// OTLP log sink with structured fields mapped to log record attributes.
class TFullOtelLogSink : public TBaseOtelLogSink {
public:
    TFullOtelLogSink(TBaseOtelLogSink::TOtelSettings settings, TString logType = "debug");

protected:
    void FillLogRecordAttributes(
        google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs,
        const TLogMessage& message) override;
    void FillScopeAttributes(
        google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs) override;
    void FillResourceAttributes(
        google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs) override;

    const TString LogType;
};

/// Full debug log stream (source `full`) to full-otel.
class TDebugFullOtelLogSink : public TFullOtelLogSink {
public:
    explicit TDebugFullOtelLogSink(TBaseOtelLogSink::TOtelSettings settings)
        : TFullOtelLogSink(std::move(settings), "debug")
    {
    }
};

}  // namespace NActors::NStructuredLog