#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>

namespace NKikimr::NSqsTopic::V1 {

    TMaybe<NKikimrPQ::TPQTabletConfig::TConsumer> GetConsumerConfig(const NKikimrPQ::TPQTabletConfig& pqConfig, const TStringBuf consumerName);
} // namespace NKikimr::NSqsTopic::V1
