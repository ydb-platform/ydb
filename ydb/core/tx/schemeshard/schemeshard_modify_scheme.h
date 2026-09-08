#pragma once

#include "defs.h"
#include "schemeshard_identificators.h"

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/tx_scheme.pb.h>

namespace NKikimr::NSchemeShard::TEvSchemeShard {

enum EEv {
    EvModifySchemeTransaction = EventSpaceBegin(TKikimrEvents::ES_FLAT_TX_SCHEMESHARD),  // 271122432
    EvModifySchemeTransactionResult = EvModifySchemeTransaction + 1 * 512,
    EvDescribeScheme,
    EvDescribeSchemeResult,
    EvFindTabletSubDomainPathId,
    EvFindTabletSubDomainPathIdResult,
    EvSubDomainPathIdFound,

    EvInitRootShard = EvModifySchemeTransaction + 5 * 512,
    EvInitRootShardResult,
    EvUpdateConfig,
    EvUpdateConfigResult,
    EvNotifyTxCompletion,
    EvNotifyTxCompletionRegistered, // 271124997 (0x10290a05)
    EvNotifyTxCompletionResult,
    EvMeasureSelfResponseTime,
    EvWakeupToMeasureSelfResponseTime,
    EvInitTenantSchemeShard,
    EvInitTenantSchemeShardResult, // 271125002
    EvSyncTenantSchemeShard,
    EvUpdateTenantSchemeShard,
    EvMigrateSchemeShard,
    EvMigrateSchemeShardResult,
    EvPublishTenantAsReadOnly,
    EvPublishTenantAsReadOnlyResult,
    EvRewriteOwner,
    EvRewriteOwnerResult,
    EvPublishTenant,
    EvPublishTenantResult,  // 271125012
    EvLogin,
    EvLoginResult,

    EvBackupDatashard = EvModifySchemeTransaction + 6 * 512,
    EvBackupDatashardResult,
    EvCancelTx,
    EvCancelTxResult,
    EvProcessingRequest,
    EvProcessingResponse,

    EvOwnerActorAck,

    EvListUsers,
    EvListUsersResult,

    EvTenantShredRequest,
    EvTenantShredResponse,
    EvWakeupToRunShred,
    EvMeasureShredBSC,
    EvWakeupToRunShredBSC,
    EvCompleteShred,
    EvShredInfoRequest,
    EvShredInfoResponse,
    EvShredManualStartupRequest,

    EvEnd
};

static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLAT_TX_SCHEMESHARD), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLAT_TX_SCHEMESHARD)");

struct TEvModifySchemeTransaction : public TEventPB<TEvModifySchemeTransaction,
                                                    NKikimrScheme::TEvModifySchemeTransaction,
                                                    EvModifySchemeTransaction> {
    TEvModifySchemeTransaction()
    {}

    TEvModifySchemeTransaction(ui64 txid, ui64 tabletId)
    {
        Record.SetTxId(txid);
        Record.SetTabletId(tabletId);
    }

    TString ToString() const {
        TStringStream str;
        str << "{TEvModifySchemeTransaction";
        if (Record.HasTxId()) {
            str << " txid# " << Record.GetTxId();
        }
        if (Record.HasTabletId()) {
            str << " TabletId# " << Record.GetTabletId();
        }
        str << "}";
        return str.Str();
    }
};

struct TEvCancelTx : public TEventPB<TEvCancelTx, NKikimrScheme::TEvCancelTx, EvCancelTx> {
};

struct TEvCancelTxResult : public TEventPB<TEvCancelTxResult, NKikimrScheme::TEvCancelTxResult, EvCancelTxResult> {
    TEvCancelTxResult() = default;

    TEvCancelTxResult(ui64 targetTxId, ui64 txId) {
        Record.SetTargetTxId(targetTxId);
        Record.SetTxId(txId);
    }
};

using EStatus = NKikimrScheme::EStatus;

struct TEvModifySchemeTransactionResult : public TEventPB<TEvModifySchemeTransactionResult,
                                                          NKikimrScheme::TEvModifySchemeTransactionResult,
                                                          EvModifySchemeTransactionResult> {
    TEvModifySchemeTransactionResult()
    {}

    TEvModifySchemeTransactionResult(TTxId txid, TTabletId schemeshardId) {
        Record.SetTxId(ui64(txid));
        Record.SetSchemeshardId(ui64(schemeshardId));
    }

    TEvModifySchemeTransactionResult(EStatus status, ui64 txid, ui64 schemeshardId, const TStringBuf& reason = TStringBuf())
        : TEvModifySchemeTransactionResult(TTxId(txid), TTabletId(schemeshardId))
    {
        Record.SetStatus(status);
        if (reason.size() > 0) {
            Record.SetReason(reason.data(), reason.size());
        }
    }

    bool IsAccepted() const {
        return Record.GetReason().empty() && (Record.GetStatus() == EStatus::StatusAccepted);
    }

    bool IsConditionalAccepted() const {
        //happens on retries, we answer like StatusAccepted with error message and do nothing in operation
        return !Record.GetReason().empty() && (Record.GetStatus() == EStatus::StatusAccepted);
    }

    bool IsDone() const {
        return Record.GetReason().empty() && (Record.GetStatus() == EStatus::StatusSuccess);
    }

    void SetStatus(EStatus status, const TString& reason = {}) {
        Record.SetStatus(status);
        if (reason) {
            Record.SetReason(reason);
        }
    }

    void SetError(EStatus status, const TString& errStr) {
        Record.SetStatus(status);
        Record.SetReason(errStr);
    }

    void AddWarning(const TString& text);
    void AddNotice(const TString& text);

    void SetPathCreateTxId(ui64 txId) { Record.SetPathCreateTxId(txId); }
    void SetPathDropTxId(ui64 txId) { Record.SetPathDropTxId(txId); }
    void SetPathId(ui64 pathId) { Record.SetPathId(pathId); }

    TString ToString() const {
        TStringStream str;
        str << "{TEvModifySchemeTransactionResult";
        if (Record.HasStatus()) {
            str << " Status# " << Record.GetStatus();
        }
        if (Record.HasTxId()) {
            str << " txid# " << Record.GetTxId();
        }
        if (Record.HasReason()) {
            str << " Reason# " << Record.GetReason();
        }
        str << "}";
        return str.Str();
    }
};

} // namespace NKikimr::NSchemeShard::TEvSchemeShard
