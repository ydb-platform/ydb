#pragma once

#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/actors/core/event_pb.h>

namespace NKikimr::NKqp {

// Serializable cross-node events for query text lookup in TLI deferred lock scenarios.
// TEvLookupQueryText is sent from the victim's SessionActor (or its helper) to the
// TKqpQueryTextCacheService on the breaker's node.

struct TEvLookupQueryText : public NActors::TEventPB<
    TEvLookupQueryText,
    NKikimrKqp::TEvLookupQueryTextRequest,
    TKqpQueryTextCacheEvents::EvLookupQueryText> {};

struct TEvLookupQueryTextResponse : public NActors::TEventPB<
    TEvLookupQueryTextResponse,
    NKikimrKqp::TEvLookupQueryTextResponse,
    TKqpQueryTextCacheEvents::EvLookupQueryTextResponse> {};

} // namespace NKikimr::NKqp
