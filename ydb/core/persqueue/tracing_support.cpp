#include "tracing_support.h"

#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NPQ {

NWilson::TSpan GenerateSpan(const TStringBuf name, NJaegerTracing::TSamplingThrottlingControl& tracingControl)
{
    NJaegerTracing::TRequestDiscriminator discriminator;
    discriminator.RequestType = NJaegerTracing::ERequestType::TOPIC_PROPOSE_TRANSACTION;
    discriminator.Database = {};

    NWilson::TTraceId traceId;
    tracingControl.HandleTracing(traceId, discriminator);

    if (traceId) {
        return {TWilsonTopic::ExecuteTransaction, std::move(traceId), TString(name), NWilson::EFlags::AUTO_END};
    }

    return {};
}

}
