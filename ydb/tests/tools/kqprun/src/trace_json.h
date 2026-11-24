#pragma once

#include <contrib/proto/opentelemetry/opentelemetry/proto/trace/v1/trace.pb.h>
#include <google/protobuf/util/json_util.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>
#include <util/string/hex.h>

namespace NKqpRun {

inline TString SpanToOtlpJson(const opentelemetry::proto::trace::v1::Span& span) {
    google::protobuf::util::JsonPrintOptions options;
    options.always_print_enums_as_ints = true;
    TString json;
    const auto status = google::protobuf::util::MessageToJsonString(span, &json, options);
    Y_ENSURE(status.ok(), status.ToString());

    NJson::TJsonValue value;
    NJson::ReadJsonTree(json, &value, true);

    // OTLP uses hex for IDs, while other bytes fields keep protobuf's base64 encoding.
    // https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
    const auto writeId = [](NJson::TJsonValue& target, TStringBuf name, TStringBuf bytes) {
        if (!bytes.empty()) {
            target[name] = HexEncode(bytes);
        }
    };
    writeId(value, "traceId", span.trace_id());
    writeId(value, "spanId", span.span_id());
    writeId(value, "parentSpanId", span.parent_span_id());
    for (int i = 0; i < span.links_size(); ++i) {
        auto& link = value["links"][static_cast<size_t>(i)];
        writeId(link, "traceId", span.links(i).trace_id());
        writeId(link, "spanId", span.links(i).span_id());
    }

    NJsonWriter::TBuf writer;
    writer.WriteJsonValue(&value, false, PREC_AUTO);
    return writer.Str();
}

}   // namespace NKqpRun
