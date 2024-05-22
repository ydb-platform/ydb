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
        EvRunWorkers,
        EvResolveSecretResult,
        EvAlterDstResult,

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

    template <typename TDerived, ui32 EventType>
    struct TGenericYdbProxyResult: public TEventLocal<TDerived, EventType> {
        using TBase = TGenericYdbProxyResult<TDerived, EventType>;

        const ui64 ReplicationId;
        const ui64 TargetId;
        const NYdb::TStatus Status;

        explicit TGenericYdbProxyResult(ui64 rid, ui64 tid, NYdb::TStatus&& status)
            : ReplicationId(rid)
            , TargetId(tid)
            , Status(std::move(status))
        {
        }

        TString ToString() const override {
            return TStringBuilder() << this->ToStringHeader() << " {"
                << " ReplicationId: " << ReplicationId
                << " TargetId: " << TargetId
                << " Status: " << Status.GetStatus()
                << " Issues: " << Status.GetIssues().ToOneLineString()
            << " }";
        }

        bool IsSuccess() const {
            return Status.IsSuccess();
        }
    };

    struct TEvCreateStreamResult: public TGenericYdbProxyResult<TEvCreateStreamResult, EvCreateStreamResult> {
        using TBase::TBase;
    };

    struct TEvDropStreamResult: public TGenericYdbProxyResult<TEvDropStreamResult, EvDropStreamResult> {
        using TBase::TBase;
    };

    template <typename TDerived, ui32 EventType>
    struct TGenericSchemeResult: public TEventLocal<TDerived, EventType> {
        using TBase = TGenericSchemeResult<TDerived, EventType>;

        const ui64 ReplicationId;
        const ui64 TargetId;
        const NKikimrScheme::EStatus Status;
        const TString Error;

        explicit TGenericSchemeResult(ui64 rid, ui64 tid, NKikimrScheme::EStatus status, const TString& error = {})
            : ReplicationId(rid)
            , TargetId(tid)
            , Status(status)
            , Error(error)
        {
        }

        TString ToStringBody() const {
            return TStringBuilder()
                << " ReplicationId: " << ReplicationId
                << " TargetId: " << TargetId
                << " Status: " << NKikimrScheme::EStatus_Name(Status)
                << " Error: " << Error;
        }

        bool IsSuccess() const {
            return Status == NKikimrScheme::StatusSuccess;
        }
    };

    struct TEvCreateDstResult: public TGenericSchemeResult<TEvCreateDstResult, EvCreateDstResult> {
        const TPathId DstPathId;

        explicit TEvCreateDstResult(ui64 rid, ui64 tid, const TPathId& dstPathId);
        explicit TEvCreateDstResult(ui64 rid, ui64 tid, NKikimrScheme::EStatus status, const TString& error);
        TString ToString() const override;
    };

    struct TEvDropDstResult: public TGenericSchemeResult<TEvDropDstResult, EvDropDstResult> {
        explicit TEvDropDstResult(ui64 rid, ui64 tid,
            NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess, const TString& error = {});
        TString ToString() const override;
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

    struct TEvRunWorkers: public TEventLocal<TEvRunWorkers, EvRunWorkers> {
    };

    struct TEvResolveSecretResult: public TEventLocal<TEvResolveSecretResult, EvResolveSecretResult> {
        const ui64 ReplicationId;
        const TString SecretValue;
        const bool Success;
        const TString Error;

        explicit TEvResolveSecretResult(ui64 rid, const TString& secretValue);
        explicit TEvResolveSecretResult(ui64 rid, bool success, const TString& error);
        TString ToString() const override;

        bool IsSuccess() const;
    };

    struct TEvAlterDstResult: public TGenericSchemeResult<TEvAlterDstResult, EvAlterDstResult> {
        explicit TEvAlterDstResult(ui64 rid, ui64 tid,
            NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess, const TString& error = {});
        TString ToString() const override;
    };

}; // TEvPrivate

}
