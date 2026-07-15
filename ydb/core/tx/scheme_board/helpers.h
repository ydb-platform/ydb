#pragma once

#include <ydb/core/base/statestorage.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/scheme_board.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>

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

struct TClusterState {
    ui64 Generation = 0;
    ui64 Guid = 0;

    TClusterState() = default;
    explicit TClusterState(const TStateStorageInfo* info);
    explicit TClusterState(const NKikimrSchemeBoard::TClusterState& proto);
    void ToProto(NKikimrSchemeBoard::TClusterState& proto) const;

    explicit operator bool() const;
    bool operator==(const TClusterState& other) const;
    void Out(IOutputStream& out) const;
};

// Two bad root schemeshard owner-ids that were misconfigured to have higher values than their tenants
// These need special handling to be treated as "always lower" in path resolution
constexpr ui64 BAD_ROOT_SCHEMESHARD_ID_1 = 72075186232723360ULL;
constexpr ui64 BAD_ROOT_SCHEMESHARD_ID_2 = 72075186232623600ULL;

// Returns true if lhs path id is less than rhs path id.
// Bad root schemeshard owner-ids are treated as always less than any other non-zero owner-id,
// to handle legacy misconfigured clusters where root schemeshard has a higher owner-id
// than tenant schemeshards.
// Otherwise behaves identically to the standard TPathId less-than comparison.
inline bool PathIdLessThan(const TPathId& lhs, const TPathId& rhs) {
    // Preserve standard less-than semantics when either OwnerId is zero.
    // Bad root schemeshard ids are treated as "always less", which would invert
    // the ordering against OwnerId == 0 compared to the standard operator<.
    if (!lhs.OwnerId || !rhs.OwnerId) {
        return lhs < rhs;
    }

    const bool lhsIsBad = lhs.OwnerId == BAD_ROOT_SCHEMESHARD_ID_1 || lhs.OwnerId == BAD_ROOT_SCHEMESHARD_ID_2;
    const bool rhsIsBad = rhs.OwnerId == BAD_ROOT_SCHEMESHARD_ID_1 || rhs.OwnerId == BAD_ROOT_SCHEMESHARD_ID_2;

    if (lhsIsBad && rhsIsBad) {
        return lhs < rhs;
    }

    if (lhsIsBad) {
        return true;
    }

    if (rhsIsBad) {
        return false;
    }

    return lhs < rhs;
}

} // NSchemeBoard
} // NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimr::NSchemeBoard::TClusterState, o, x) {
    return x.Out(o);
}
