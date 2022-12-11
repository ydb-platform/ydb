#pragma once

#include "events.h"
#include "http_req.h"

namespace NKikimr::NHttpProxy {

using namespace Ydb::DataStreams::V1;

template<class TProtoRequest>
void FillInputCustomMetrics(const TProtoRequest& request, const THttpRequestContext& httpContext, const TActorContext& ctx) {
        Y_UNUSED(request, httpContext, ctx);
}
template<class TProtoResult>
void FillOutputCustomMetrics(const TProtoResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
        Y_UNUSED(result, httpContext, ctx);
}

TVector<std::pair<TString, TString>> BuildLabels(const TString& method, const THttpRequestContext& httpContext, const TString& name) {
    if (method.empty()) {
        return { {"database", httpContext.DatabaseName}, {"cloud_id", httpContext.CloudId},
                {"folder_id", httpContext.FolderId}, {"database_id", httpContext.DatabaseId},
                {"topic", httpContext.StreamName}, {"name", name}};

    }
    return { {"database", httpContext.DatabaseName}, {"method", method}, {"cloud_id", httpContext.CloudId},
            {"folder_id", httpContext.FolderId}, {"database_id", httpContext.DatabaseId},
            {"topic", httpContext.StreamName}, {"name", name}};
}

template <>
void FillInputCustomMetrics<PutRecordsRequest>(const PutRecordsRequest& request, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    i64 bytes = 0;
    for (auto& rec : request.records()) {
        bytes += rec.data().size() +  rec.partition_key().size() + rec.explicit_hash_key().size();
    }
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{bytes, true, true,
                 BuildLabels("PutRecords", httpContext, "api.http.data_streams.request.bytes")
             });
}

template <>
void FillInputCustomMetrics<PutRecordRequest>(const PutRecordRequest& request, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{1, true, true,
                 BuildLabels("", httpContext, "api.http.data_streams.put_record.messages")
             });

    i64 bytes = request.data().size() +  request.partition_key().size() + request.explicit_hash_key().size();

    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{bytes, true, true,
                 BuildLabels("PutRecord", httpContext, "api.http.data_streams.request.bytes")
             });
}


template <>
void FillOutputCustomMetrics<PutRecordResult>(const PutRecordResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    Y_UNUSED(result);
    Y_UNUSED(httpContext);
    Y_UNUSED(ctx);
}


template <>
void FillOutputCustomMetrics<PutRecordsResult>(const PutRecordsResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    i64 failed = result.failed_record_count();
    i64 success = result.records_size() - failed;
    if (success > 0) {
        ctx.Send(MakeMetricsServiceID(),
                 new TEvServerlessProxy::TEvCounter{success, true, true,
                     BuildLabels("", httpContext, "api.http.data_streams.put_records.successfull_messages")
                 });
    }

    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{result.records_size(), true, true,
                 BuildLabels("", httpContext, "api.http.data_streams.put_records.total_messages")
             });
    if (failed > 0) {
        ctx.Send(MakeMetricsServiceID(),
                 new TEvServerlessProxy::TEvCounter{failed, true, true,
                     BuildLabels("", httpContext, "api.http.data_streams.put_records.failed_messages")
                 });
    }
}

template <>
void FillOutputCustomMetrics<GetRecordsResult>(const GetRecordsResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    auto records_n = result.records().size();
    auto bytes = std::accumulate(result.records().begin(), result.records().end(), 0l,
                                 [](i64 sum, decltype(*result.records().begin()) &r) {
                                     return sum + r.data().size() +
                                         r.partition_key().size() +
                                         r.sequence_number().size() +
                                         sizeof(r.timestamp()) +
                                         sizeof(r.encryption())
                                         ;
                                 });

    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{records_n, true, true,
                 BuildLabels("", httpContext, "api.http.data_streams.get_records.messages")}
             );
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{bytes, true, true,
                 BuildLabels("GetRecords", httpContext, "api.http.data_streams.response.bytes")}
             );
}

} // namespace NKikimr::NHttpProxy
