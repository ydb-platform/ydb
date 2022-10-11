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
        return {{"cloud", httpContext.CloudId}, {"folder", httpContext.FolderId},
                {"database", httpContext.DatabaseId}, {"stream", httpContext.StreamName},
                {"name", name}};

    }
    return {{"method", method}, {"cloud", httpContext.CloudId}, {"folder", httpContext.FolderId},
            {"database", httpContext.DatabaseId}, {"stream", httpContext.StreamName},
            {"name", name}};
}

template <>
void FillInputCustomMetrics<PutRecordsRequest>(const PutRecordsRequest& request, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{request.records_size(), true, true,
                 BuildLabels("", httpContext, "stream.incoming_records_per_second")
             });

    i64 bytes = 0;
    for (auto& rec : request.records()) {
        bytes += rec.data().size() +  rec.partition_key().size() + rec.explicit_hash_key().size();
    }

    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{bytes, true, true,
                 BuildLabels("", httpContext, "stream.incoming_bytes_per_second")
             });

    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{bytes, true, true,
                 BuildLabels("", httpContext, "stream.put_records.bytes_per_second")
             });
}

template <>
void FillInputCustomMetrics<PutRecordRequest>(const PutRecordRequest& request, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{1, true, true,
                 BuildLabels("", httpContext, "stream.incoming_records_per_second")
             });
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{1, true, true,
                 BuildLabels("", httpContext, "stream.put_record.records_per_second")
             });

    i64 bytes = request.data().size() +  request.partition_key().size() + request.explicit_hash_key().size();

    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{bytes, true, true,
                 BuildLabels("", httpContext, "stream.incoming_bytes_per_second")
             });

    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{bytes, true, true,
                 BuildLabels("", httpContext, "stream.put_record.bytes_per_second")
             });
}


template <>
void FillOutputCustomMetrics<PutRecordResult>(const PutRecordResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    Y_UNUSED(result);
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{1, true, true,
                 BuildLabels("", httpContext, "stream.put_record.success_per_second")
             });
}


template <>
void FillOutputCustomMetrics<PutRecordsResult>(const PutRecordsResult& result, const THttpRequestContext& httpContext, const TActorContext& ctx) {
    i64 failed = result.failed_record_count();
    i64 success = result.records_size() - failed;
    if (success > 0) {
        ctx.Send(MakeMetricsServiceID(),
                 new TEvServerlessProxy::TEvCounter{1, true, true,
                     BuildLabels("", httpContext, "stream.put_records.success_per_second")
                 });
        ctx.Send(MakeMetricsServiceID(),
                 new TEvServerlessProxy::TEvCounter{success, true, true,
                     BuildLabels("", httpContext, "stream.put_records.successfull_records_per_second")
                 });
    }

    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{result.records_size(), true, true,
                 BuildLabels("", httpContext, "stream.put_records.total_records_per_second")
             });
    if (failed > 0) {
        ctx.Send(MakeMetricsServiceID(),
                 new TEvServerlessProxy::TEvCounter{failed, true, true,
                     BuildLabels("", httpContext, "stream.put_records.failed_records_per_second")
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
             new TEvServerlessProxy::TEvCounter{1, true, true,
                 BuildLabels("", httpContext, "stream.get_records.success_per_second")}
             );
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{records_n, true, true,
                 BuildLabels("", httpContext, "stream.get_records.records_per_second")}
             );
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{bytes, true, true,
                 BuildLabels("", httpContext, "stream.get_records.bytes_per_second")}
             );
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{records_n, true, true,
                 BuildLabels("", httpContext, "stream.outgoing_records_per_second")}
             );
    ctx.Send(MakeMetricsServiceID(),
             new TEvServerlessProxy::TEvCounter{bytes, true, true,
                 BuildLabels("", httpContext, "stream.outgoing_bytes_per_second")}
             );
}

} // namespace NKikimr::NHttpProxy
