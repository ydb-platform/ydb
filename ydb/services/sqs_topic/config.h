#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>

namespace NKikimr::NSqsTopic::V1 {

    TMaybe<NKikimrPQ::TPQTabletConfig::TConsumer> GetConsumerConfig(
        const NKikimrPQ::TPQTabletConfig& pqConfig,
        const TStringBuf consumerName,
        const NActors::TActorContext& ctx);

    TString ResolveConsumerNameFromQueueUrl(const TStringBuf consumerFromUrl, const NActors::TActorContext& ctx);
} // namespace NKikimr::NSqsTopic::V1
