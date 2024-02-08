#pragma once

#include <ydb/library/actors/interconnect/interconnect_common.h>


namespace NKikimr {

    class TKikimrScopeId {
        NActors::TScopeId ScopeId;

    public:
        TKikimrScopeId() = default;
        TKikimrScopeId(const TKikimrScopeId&) = default;
        TKikimrScopeId &operator=(const TKikimrScopeId& other) = default;

        explicit TKikimrScopeId(const NActors::TScopeId& scopeId)
            : ScopeId(scopeId)
        {}

        explicit TKikimrScopeId(ui64 schemeshardId, ui64 pathItemId)
            : ScopeId(schemeshardId, pathItemId)
        {}

        NActors::TScopeId GetInterconnectScopeId() const {
            return ScopeId;
        }

        ui64 GetSchemeshardId() const {
            return ScopeId.first;
        }

        ui64 GetPathItemId() const {
            Y_ABORT_UNLESS(GetSchemeshardId() != 0);
            return ScopeId.second;
        }

        ui64 GetDomainId() const {
            Y_ABORT_UNLESS(GetSchemeshardId() == 0);
            return ScopeId.second;
        }

        bool IsEmpty() const {
            return ScopeId.first == 0 && ScopeId.second == 0;
        }

        bool IsSystemScope() const {
            return GetSchemeshardId() == 0 && !IsEmpty();
        }

        bool IsTenantScope() const {
            return GetSchemeshardId() != 0;
        }

        friend bool operator ==(const TKikimrScopeId& x, const TKikimrScopeId& y) {
            return x.ScopeId == y.ScopeId;
        }

        friend bool operator !=(const TKikimrScopeId& x, const TKikimrScopeId& y) {
            return x.ScopeId != y.ScopeId;
        }

        static const TKikimrScopeId DynamicTenantScopeId;
    };

} // NKikimr
