#pragma once

#include "defs.h"
#include <ydb/core/scheme/scheme_pathid.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <library/cpp/actors/core/log.h>

#include <util/generic/vector.h>
#include <util/generic/set.h>

#if defined SB_LOG_T || \
    defined SB_LOG_D || \
    defined SB_LOG_I || \
    defined SB_LOG_N || \
    defined SB_LOG_W || \
    defined SB_LOG_E || \
    defined SB_LOG_CRIT
#error log macro redefinition
#endif

#define SB_LOG_T(service, stream) LOG_TRACE_S((TlsActivationContext->AsActorContext()), NKikimrServices::service, stream)
#define SB_LOG_D(service, stream) LOG_DEBUG_S((TlsActivationContext->AsActorContext()), NKikimrServices::service, stream)
#define SB_LOG_I(service, stream) LOG_INFO_S((TlsActivationContext->AsActorContext()), NKikimrServices::service, stream)
#define SB_LOG_N(service, stream) LOG_NOTICE_S((TlsActivationContext->AsActorContext()), NKikimrServices::service, stream)
#define SB_LOG_W(service, stream) LOG_WARN_S((TlsActivationContext->AsActorContext()), NKikimrServices::service, stream)
#define SB_LOG_E(service, stream) LOG_ERROR_S((TlsActivationContext->AsActorContext()), NKikimrServices::service, stream)
#define SB_LOG_CRIT(service, stream) LOG_CRIT_S((TlsActivationContext->AsActorContext()), NKikimrServices::service, stream)

namespace NKikimr {
namespace NSchemeBoard {

using TDomainId = TPathId;

TActorId MakeInterconnectProxyId(const ui32 nodeId);
ui64 GetPathVersion(const NKikimrScheme::TEvDescribeSchemeResult& record);
TPathId GetPathId(const NKikimrScheme::TEvDescribeSchemeResult& record);
TDomainId GetDomainId(const NKikimrScheme::TEvDescribeSchemeResult& record);
TSet<ui64> GetAbandonedSchemeShardIds(const NKikimrScheme::TEvDescribeSchemeResult &record);

TIntrusivePtr<TEventSerializedData> SerializeEvent(IEventBase* ev);

void MultiSend(const TVector<const TActorId*>& recipients, const TActorId& sender, TAutoPtr<IEventBase> ev, ui32 flags = 0, ui64 cookie = 0);

template <typename TEvent>
void MultiSend(const TVector<const TActorId*>& recipients, const TActorId& sender, THolder<TEvent> ev, ui32 flags = 0, ui64 cookie = 0) {
    MultiSend(recipients, sender, static_cast<IEventBase*>(ev.Release()), flags, cookie);
}

// Only string or embed message fields are supported (it used wire type = 2 internally)
TString PreSerializedProtoField(TString data, int fieldNo);

} // NSchemeBoard
} // NKikimr
