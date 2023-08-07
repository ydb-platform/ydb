#pragma once

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

namespace NKikimr::NReplication::NController {

struct TEvPrivate {
    enum EEv {
        EvDiscoveryTargetsResult = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvAssignStreamName,
        EvCreateStreamResult,
        EvCreateDstResult,
        EvDropStreamResult,
        EvDropDstResult,
        EvDropReplication,
        EvResolveTenantResult,
        EvUpdateTenantNodes,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

    struct TEvDiscoveryTargetsResult: public TEventLocal<TEvDiscoveryTargetsResult, EvDiscoveryTargetsResult> {
        using TAddEntry = std::pair<NYdb::NScheme::TSchemeEntry, TString>; // src, dst
        using TFailedEntry = std::pair<TString, NYdb::TStatus>; // src, error

        const ui64 ReplicationId;
        TVector<TAddEntry> ToAdd;
        TVector<ui64> ToDelete;
        TVector<TFailedEntry> Failed;

        explicit TEvDiscoveryTargetsResult(ui64 rid, TVector<TAddEntry>&& toAdd, TVector<ui64>&& toDel);
        explicit TEvDiscoveryTargetsResult(ui64 rid, TVector<TFailedEntry>&& failed);
        TString ToString() const override;

        bool IsSuccess() const;
    };

    struct TEvAssignStreamName: public TEventLocal<TEvAssignStreamName, EvAssignStreamName> {
        const ui64 ReplicationId;
        const ui64 TargetId;

        explicit TEvAssignStreamName(ui64 rid, ui64 tid);
        TString ToString() const override;
    };

    struct TEvCreateStreamResult: public TEventLocal<TEvCreateStreamResult, EvCreateStreamResult> {
        const ui64 ReplicationId;
        const ui64 TargetId;
        const NYdb::TStatus Status;

        explicit TEvCreateStreamResult(ui64 rid, ui64 tid, NYdb::TStatus&& status);
        TString ToString() const override;

        bool IsSuccess() const;
    };

    struct TEvCreateDstResult: public TEventLocal<TEvCreateDstResult, EvCreateDstResult> {
        const ui64 ReplicationId;
        const ui64 TargetId;
        const TPathId DstPathId;
        const NKikimrScheme::EStatus Status;
        const TString Error;

        explicit TEvCreateDstResult(ui64 rid, ui64 tid, const TPathId& dstPathId);
        explicit TEvCreateDstResult(ui64 rid, ui64 tid, NKikimrScheme::EStatus status, const TString& error);
        TString ToString() const override;

        bool IsSuccess() const;
    };

    struct TEvDropStreamResult: public TEventLocal<TEvDropStreamResult, EvDropStreamResult> {
        const ui64 ReplicationId;
        const ui64 TargetId;
        const NYdb::TStatus Status;

        explicit TEvDropStreamResult(ui64 rid, ui64 tid, NYdb::TStatus&& status);
        TString ToString() const override;

        bool IsSuccess() const;
    };

    struct TEvDropDstResult: public TEventLocal<TEvDropDstResult, EvDropDstResult> {
        const ui64 ReplicationId;
        const ui64 TargetId;
        const NKikimrScheme::EStatus Status;
        const TString Error;

        explicit TEvDropDstResult(ui64 rid, ui64 tid,
            NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess, const TString& error = {});
        TString ToString() const override;

        bool IsSuccess() const;
    };

    struct TEvDropReplication: public TEventLocal<TEvDropReplication, EvDropReplication> {
        const ui64 ReplicationId;

        explicit TEvDropReplication(ui64 rid);
        TString ToString() const override;
    };

    struct TEvResolveTenantResult: public TEventLocal<TEvResolveTenantResult, EvResolveTenantResult> {
        const ui64 ReplicationId;
        const TString Tenant;
        const bool Success;

        explicit TEvResolveTenantResult(ui64 rid, const TString& tenant);
        explicit TEvResolveTenantResult(ui64 rid, bool success);
        TString ToString() const override;

        bool IsSuccess() const;
    };

    struct TEvUpdateTenantNodes: public TEventLocal<TEvUpdateTenantNodes, EvUpdateTenantNodes> {
        const TString Tenant;

        explicit TEvUpdateTenantNodes(const TString& tenant);
        TString ToString() const override;
    };

}; // TEvPrivate

}
