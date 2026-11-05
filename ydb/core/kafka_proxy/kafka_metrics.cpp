#include "kafka_events.h"
#include "kafka_metrics.h"

namespace NKafka {

TVector<std::pair<TString, TString>> BuildLabels(const NKafka::TContext::TPtr context, const TString& method, const TString& topic, const TString& name, const TString& errorCode) {
    if (context->Authenticated()) {
        return {{"counters", context->IsServerless ? "datastreams_serverless" : "datastreams"},
                {"database", context->DatabasePath},
                {"method", method},
                {"cloud_id", context->CloudId},
                {"folder_id", context->FolderId},
                {"database_id", context->DatabaseId},
                {"topic", topic},
                {"error_code", errorCode},
                {"name", name}};
    } else {
        return {{"counters", "datastreams"},
                {"method", method},
                {"topic", topic},
                {"error_code", errorCode},
                {"name", name}};

    }
}

TVector<std::pair<TString, TString>> BuildGroupLabels(const NKafka::TContext::TPtr context, const TString& groupId, const TString& name) {
    if (context->Authenticated()) {
        return {{"counters", context->IsServerless ? "datastreams_serverless" : "datastreams"},
                {"database", context->DatabasePath},
                {"cloud_id", context->CloudId},
                {"folder_id", context->FolderId},
                {"database_id", context->DatabaseId},
                {"consumer_group", groupId},
                {"name", name}};
    } else {
        return {{"counters", "datastreams"},
                {"consumer_group", groupId},
                {"name", name}};
    }
}

TActorId MakeKafkaMetricsServiceID() {
    static const char x[12] = "kafka_mtrcs";
    return TActorId(0, TStringBuf(x, 12));
}

} // namespace NKafka
