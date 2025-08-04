#include "tracing_support.h"

#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NPQ {

NWilson::TSpan GenerateSpan(const TStringBuf name, NJaegerTracing::TSamplingThrottlingControl& tracingControl)
{
    NJaegerTracing::TRequestDiscriminator discriminator;
    discriminator.RequestType = NJaegerTracing::ERequestType::TOPIC_PROPOSE_TRANSACTION;
    discriminator.Database = {};

    NWilson::TTraceId traceId = tracingControl.HandleTracing(discriminator, {});

    if (traceId) {
        return {TWilsonTopic::TopicTopLevel, std::move(traceId), TString(name), NWilson::EFlags::AUTO_END};
    }

    return {};
}

NWilson::TSpan GenerateSpan(const TStringBuf name, ui8 verbosity)
{
    return {
        TWilsonTopic::TopicTopLevel,
        NWilson::TTraceId::NewTraceId(verbosity, Max<ui32>()),
        TString(name),
        NWilson::EFlags::AUTO_END
    };
}

}
