#include "simple_otel_log_sink.h"

namespace NActors::NStructuredLog {

TSimpleOtelLogSink::TSimpleOtelLogSink(TBaseOtelLogSink::TOtelSettings settings)
    : TBaseOtelLogSink(std::move(settings))
{
}

void TSimpleOtelLogSink::FillLogRecordAttributes(
    google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs,
    const TLogMessage& message) {

    // Fill standard attributes
    if (message.FileName) {
        TStringStream location;
        location << message.FileName << ":" << message.LineNumber;
        AddAttr(attrs, "location", location.Str());
    }

    AddAttr(attrs, "nodeId", message.NodeId);
    AddAttr(attrs, "component", TBaseOtelLogSink::GetComponentName(message.Component));

    message.StructuredMessage.ForEachTyped([&](const std::vector<TKeyName>& key, const auto& value) {

        TStringStream attrName;
        attrName << "meta";
        for (const auto& part : key) {
            attrName << '.' << part.ToString();
        }

        AddAttr(attrs, attrName.Str(), value);
    });
}

void TSimpleOtelLogSink::FillScopeAttributes(
    google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs) {
    Y_UNUSED(attrs);
}

void TSimpleOtelLogSink::FillResourceAttributes(
    google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs) {
    Y_UNUSED(attrs);
}

}  // namespace NActors::NStructuredLog