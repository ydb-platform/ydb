#pragma once

#include "defs.h"
#include <ydb/core/scheme/scheme_pathid.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/scheme_board.pb.h>

#include <ydb/library/actors/core/log.h>

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

// Extract metainfo directly from DescribeSchemeResult message
ui64 GetPathVersion(const NKikimrScheme::TEvDescribeSchemeResult& record);
TPathId GetPathId(const NKikimrScheme::TEvDescribeSchemeResult& record);
TDomainId GetDomainId(const NKikimrScheme::TEvDescribeSchemeResult& record);
TSet<ui64> GetAbandonedSchemeShardIds(const NKikimrScheme::TEvDescribeSchemeResult& record);

// Extract metainfo from TEvUpdate message
ui64 GetPathVersion(const NKikimrSchemeBoard::TEvUpdate& record);

// Extract metainfo from TEvNotify message
ui64 GetPathVersion(const NKikimrSchemeBoard::TEvNotify& record);
NSchemeBoard::TDomainId GetDomainId(const NKikimrSchemeBoard::TEvNotify& record);
TSet<ui64> GetAbandonedSchemeShardIds(const NKikimrSchemeBoard::TEvNotify& record);

void MultiSend(const TVector<const TActorId*>& recipients, const TActorId& sender, TAutoPtr<IEventBase> ev, ui32 flags = 0, ui64 cookie = 0);

template <typename TEvent>
void MultiSend(const TVector<const TActorId*>& recipients, const TActorId& sender, THolder<TEvent> ev, ui32 flags = 0, ui64 cookie = 0) {
    MultiSend(recipients, sender, static_cast<IEventBase*>(ev.Release()), flags, cookie);
}

// Work with TEvDescribeSchemeResult and its parts
TString SerializeDescribeSchemeResult(const NKikimrScheme::TEvDescribeSchemeResult& proto);
TString SerializeDescribeSchemeResult(const TString& preSerializedPart, const NKikimrScheme::TEvDescribeSchemeResult& protoPart);
NKikimrScheme::TEvDescribeSchemeResult DeserializeDescribeSchemeResult(const TString& serialized);
NKikimrScheme::TEvDescribeSchemeResult* DeserializeDescribeSchemeResult(const TString& serialized, google::protobuf::Arena* arena);
TString JsonFromDescribeSchemeResult(const TString& serialized);

} // NSchemeBoard
} // NKikimr
