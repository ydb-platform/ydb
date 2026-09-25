#include "actors.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKafka {

// Range advertised for ApiVersions itself. Parser accepts a wider PresentVersions range;
// versions outside PresentVersions use the KIP-511 v0 / UNSUPPORTED_VERSION fallback.
static constexpr TKafkaVersion AdvertisedApiVersionsMax = 2;

inline bool IsApiVersionsRequestVersionSupported(TKafkaVersion version) {
    return version >= TApiVersionsRequestData::MessageMeta::PresentVersions.Min
        && version <= TApiVersionsRequestData::MessageMeta::PresentVersions.Max;
}

// KIP-511: unsupported ApiVersions requests are answered with a v0 body so any client can parse it.
inline TKafkaVersion ApiVersionsResponseWriteVersion(TKafkaVersion requestVersion) {
    return IsApiVersionsRequestVersionSupported(requestVersion) ? requestVersion : TKafkaVersion{0};
}

TApiVersionsResponseData::TPtr GetApiVersions(TKafkaVersion requestVersion);

class TKafkaApiVersionsActor: public NActors::TActorBootstrapped<TKafkaApiVersionsActor> {
public:
    TKafkaApiVersionsActor(const TContext::TPtr context, const ui64 correlationId, const TMessagePtr<TApiVersionsRequestData>& message,
                           TKafkaVersion requestApiVersion)
        : Context(context)
        , CorrelationId(correlationId)
        , Message(message)
        , RequestApiVersion(requestApiVersion) {
    }

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    const TContext::TPtr Context;
    const ui64 CorrelationId;
    const TMessagePtr<TApiVersionsRequestData> Message;
    const TKafkaVersion RequestApiVersion;
};

} // NKafka
