#pragma once

#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/library/actors/core/actor.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <optional>

namespace NPersQueue {
class TTopicsListController;
}

namespace NKikimr::NPQ::NDataplane::NWrite {
struct TWriteSessionProtocolOpts;
}

namespace NKikimr::NGRpcProxy::V1 {

NPQ::NDataplane::NWrite::TWriteSessionProtocolOpts TopicWriteSessionProtocol();

NActors::IActor* CreateWriteSessionTopicApiActor(
    NGRpcService::TEvStreamTopicWriteRequest* request,
    ui64 cookie,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const std::optional<TString> clientDC,
    const NPersQueue::TTopicsListController& topicsController);

} // namespace NKikimr::NGRpcProxy::V1
