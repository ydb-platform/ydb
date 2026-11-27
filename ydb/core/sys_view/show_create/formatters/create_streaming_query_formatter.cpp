#include "create_streaming_query_formatter.h"
#include "formatters_common.h"

#include <ydb/core/kqp/gateway/behaviour/streaming_query/common/utils.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <util/string/ascii.h>
#include <util/string/hex.h>

#include <algorithm>

namespace NKikimr::NSysView {
namespace {

void AppendStringSetting(TStringStream& sql, TStringBuf name, TStringBuf value) {
    sql << ",\n    " << name << " = ";

    if (std::ranges::all_of(value, [](char c) { return c >= ' ' && c <= '~' && c != '\'' && c != '\\'; })) {
        sql << '\'' << value << '\'';
    } else {
        // The default and ANSI lexers interpret escapes differently.
        sql << "String::HexDecode('" << HexEncode(value) << "')";
    }
}

void AppendStreamingDisposition(TStringStream& sql, const NYql::NPq::NProto::StreamingDisposition& disposition) {
    if (disposition.has_oldest()) {
        sql << ",\n    READ_FROM = EARLIEST";
    } else if (disposition.has_fresh()) {
        sql << ",\n    READ_FROM = LATEST";
    } else if (disposition.has_from_time()) {
        sql << ",\n    READ_FROM = Timestamp('" << NProtoInterop::CastFromProto(disposition.from_time().timestamp()) << "')";
    }

    if (disposition.has_output_start_time()) {
        sql << ",\n    OUTPUT_FROM = Timestamp('" << NProtoInterop::CastFromProto(disposition.output_start_time()) << "')";
    }
}

}   // namespace

TString FormatCreateStreamingQuery(const TString& path, const NKikimrSchemeOp::TStreamingQueryDescription& description) {
    NKqp::TStreamingQuerySettings settings;
    settings.FromProto(description.GetProperties());

    if (settings.QueryText.empty()) {
        ythrow TFormatFail(Ydb::StatusIds::SCHEME_ERROR, "Streaming query has no stored query text");
    }

    TStringStream sql;
    sql << "CREATE STREAMING QUERY ";
    EscapeName(path, sql);
    sql << " WITH (\n    RUN = " << (settings.Run ? "TRUE" : "FALSE");

    if (settings.ResourcePool) {
        AppendStringSetting(sql, "RESOURCE_POOL", settings.ResourcePool);
    }

    if (settings.CheckpointIntervalString) {
        AppendStringSetting(sql, "CHECKPOINT_INTERVAL", settings.CheckpointIntervalString);
    }

    if (settings.WatermarkLateEventsPolicy && settings.WatermarkLateEventsPolicy != "drop") {
        AppendStringSetting(sql, "WATERMARK_LATE_EVENTS_POLICY", settings.WatermarkLateEventsPolicy);
    }

    if (settings.StreamingDisposition) {
        AppendStreamingDisposition(sql, *settings.StreamingDisposition);
    }

    sql << "\n) AS DO BEGIN";

    if (!IsAsciiSpace(settings.QueryText.front())) {
        sql << '\n';
    }

    sql << settings.QueryText;

    if (!IsAsciiSpace(settings.QueryText.back())) {
        sql << '\n';
    }

    sql << "END DO;";
    return sql.Str();
}

}   // namespace NKikimr::NSysView
