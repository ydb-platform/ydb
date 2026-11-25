#include "full_otel_log_sink.h"

#include <ydb/library/actors/core/log_settings.h>

#include <library/cpp/svnversion/svnversion.h>

namespace NActors::NStructuredLog {

TFullOtelLogSink::TFullOtelLogSink(TBaseOtelLogSink::TOtelSettings settings, TString logType)
    : TBaseOtelLogSink(std::move(settings))
    , LogType(std::move(logType))
{
}

void TFullOtelLogSink::FillLogRecordAttributes(
    google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs,
    const TLogMessage& message) {

    if (message.FileName) {
        TStringStream location;
        location << message.FileName << ":" << message.LineNumber;
        AddAttr(attrs, "meta.location", location.Str());
    }

    AddAttr(attrs, "labels.component", GetComponentName(message.Component));
    AddAttr(attrs, "meta.microseconds", message.Time.MicroSeconds());

    AddAttr(attrs, "meta.priority", TString{NLog::PriorityToString(message.Priority)});
    AddAttr(attrs, "meta.npriority", static_cast<int>(message.Priority));
    AddAttr(attrs, "meta.revision", GetProgramSvnRevision());

    message.StructuredMessage.ForEachTyped([&](const std::vector<TKeyName>& key, const auto& value) {
        TStringStream attrName;
        attrName << "meta.message";
        for (const auto& part : key) {
            attrName << '.' << part.ToString();
        }

        AddAttr(attrs, attrName.Str(), value);
    });
}

void TFullOtelLogSink::FillScopeAttributes(
    google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs) {
    Y_UNUSED(attrs);
}

void TFullOtelLogSink::FillResourceAttributes(
    google::protobuf::RepeatedPtrField<otel_common_v1::KeyValue>& attrs) {
    AddAttr(attrs, "labels.log_type", LogType);
    AddAttr(attrs, "labels.tag", TString("KIKIMR"));

    if (LogSettings) {
        AddAttr(attrs, "labels.database",
            LogSettings->TenantName ? LogSettings->TenantName : TString("static"));
        AddAttr(attrs, "meta.node_id", LogSettings->NodeId);
        AddAttr(attrs, "cluster", LogSettings->ClusterName);
        AddAttr(attrs, "host", LogSettings->ShortHostName);
    }
}

}  // namespace NActors::NStructuredLog