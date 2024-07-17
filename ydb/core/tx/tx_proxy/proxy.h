#pragma once
#include "defs.h"
#include "mon.h"

#include <ydb/public/lib/base/defs.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/locks/sys_tables.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/scheme/tablet_scheme.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/protos/tx_proxy.pb.h>

#include <util/generic/set.h>
#include <util/generic/hash.h>

namespace NKikimr {

namespace NMiniKQL {
    struct TKeyDescriptionRange;
    struct TKeyDescriptionSuffixItem;
    struct TKeyDescription;
}

namespace NTxProxy {
    struct TTxProxyServices {
        TActorId Proxy;
        TActorId SchemeCache;
        TActorId LeaderPipeCache;
        TActorId FollowerPipeCache;
    };
}

struct TEvTxUserProxy {
    enum EEv {
        EvProposeTransaction = EventSpaceBegin(TKikimrEvents::ES_TX_USERPROXY), // reply would be with generic TEvTxProxy::TEvProposeTransactionStatus
        EvNavigate, // --/-- TEvSchemeShard::TEvNavigateSchemePartResult

        EvProposeTransactionStatus = EvProposeTransaction + 1 * 512,
        EvNavigateStatus,
        EvInvalidateTable,
        EvInvalidateTableResult,

        EvCancelBackupRequestDeprecated,
        EvCancelBackupResultDeprecated,

        EvProposeKqpTransaction,

        EvAllocateTxId,
        EvAllocateTxIdResult,

        // deprecated
        EvExportRequest = EvProposeTransaction + 2 * 512,
        EvExportResponse,

        EvUploadRowsResponse,

        EvGetProxyServicesRequest,
        EvGetProxyServicesResponse,

        EvResolveTablesResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_USERPROXY), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_USERPROXY)");

    struct TEvProposeTransaction : public TEventPB<TEvProposeTransaction, NKikimrTxUserProxy::TEvProposeTransaction, EvProposeTransaction> {
        enum EProxyFlags {
            ProxyTrackWallClock = 1 << 0,
            ProxyReportAccepted = 1 << 1,
            ProxyReportResolved = 1 << 2,
            ProxyReportPrepared = 1 << 3,
            ProxyReportPlanned = 1 << 4,
        };

        TEvProposeTransaction()
        {}

        TEvProposeTransaction(ui64 proxyFlags)
        {
            Record.SetProxyFlags(proxyFlags);
        }

        bool HasSchemeProposal() const {
                return HasModifyScheme() || HasTransactionalModification();
        }

        bool HasModifyScheme() const {
            const NKikimrTxUserProxy::TTransaction &tx = Record.GetTransaction();
            return tx.HasModifyScheme();
        }

        bool HasTransactionalModification() const {
            const NKikimrTxUserProxy::TTransaction &tx = Record.GetTransaction();
            return tx.TransactionalModificationSize();
        }

        bool HasMakeProposal() const {
                const NKikimrTxUserProxy::TTransaction &tx = Record.GetTransaction();
                return (tx.HasMiniKQLTransaction() && tx.GetMiniKQLTransaction().HasProgram())
                        || tx.HasReadTableTransaction();
        }

        bool HasSnapshotProposal() const {
            const auto& tx = Record.GetTransaction();
            return (tx.HasCreateVolatileSnapshot() ||
                    tx.HasRefreshVolatileSnapshot() ||
                    tx.HasDiscardVolatileSnapshot());
        }

        bool HasCommitWritesProposal() const {
            const auto& tx = Record.GetTransaction();
            return tx.HasCommitWrites();
        }

        bool NeedTxId() const {
            if (HasSchemeProposal() || HasMakeProposal()) {
                return true;
            }

            if (HasSnapshotProposal()) {
                const auto& tx = Record.GetTransaction();
                if (tx.HasRefreshVolatileSnapshot() || tx.HasDiscardVolatileSnapshot()) {
                    return false;
                }

                return true;
            }

            if (HasCommitWritesProposal()) {
                return true;
            }

            return false;
        }
    };

    struct TEvNavigate : public TEventPB<TEvNavigate, NKikimrTxUserProxy::TEvNavigate, EvNavigate> {
        TEvNavigate()
        {}
    };

    using TResultStatus = NTxProxy::TResultStatus;

    struct TEvProposeTransactionStatus : public TEventPB<TEvProposeTransactionStatus, NKikimrTxUserProxy::TEvProposeTransactionStatus, EvProposeTransactionStatus> {
        using EStatus = TResultStatus::EStatus;

        TEvProposeTransactionStatus()
        {}

        TEvProposeTransactionStatus(EStatus status) {
            Record.SetStatus(static_cast<ui32>(status));
        }

        EStatus Status() const {
            return static_cast<EStatus>(Record.GetStatus());
        }

        TString ToString() const;
    };

    struct TEvInvalidateTable : public TEventPB<TEvInvalidateTable, NKikimrTxUserProxy::TEvInvalidateTable, EvInvalidateTable> {
        TEvInvalidateTable() = default;
        TEvInvalidateTable(const TTableId& tableId) {
            Record.SetSchemeShardId(tableId.PathId.OwnerId);
            Record.SetTableId(tableId.PathId.LocalPathId);
        }
    };

    struct TEvInvalidateTableResult : public TEventPB<TEvInvalidateTableResult, NKikimrTxUserProxy::TEvInvalidateTableResult, EvInvalidateTableResult> {};

    struct TEvProposeKqpTransaction : public TEventLocal<TEvProposeKqpTransaction, EvProposeKqpTransaction> {
        TActorId ExecuterId;

        TEvProposeKqpTransaction(const TActorId& executerId)
            : ExecuterId(executerId) {}
    };

    struct TEvAllocateTxId : public TEventLocal<TEvAllocateTxId, EvAllocateTxId> {
        // empty
    };

    struct TEvAllocateTxIdResult : public TEventLocal<TEvAllocateTxIdResult, EvAllocateTxIdResult> {
        const ui64 TxId;
        const NTxProxy::TTxProxyServices Services;
        const TIntrusivePtr<NTxProxy::TTxProxyMon> TxProxyMon;

        TEvAllocateTxIdResult(
                ui64 txId,
                const NTxProxy::TTxProxyServices& services,
                const TIntrusivePtr<NTxProxy::TTxProxyMon>& txProxyMon)
            : TxId(txId)
            , Services(services)
            , TxProxyMon(txProxyMon)
        { }
    };

    struct TEvGetProxyServicesRequest : public TEventLocal<TEvGetProxyServicesRequest, EvGetProxyServicesRequest> {
        // empty
    };

    struct TEvGetProxyServicesResponse : public TEventLocal<TEvGetProxyServicesResponse, EvGetProxyServicesResponse> {
        const NTxProxy::TTxProxyServices Services;

        TEvGetProxyServicesResponse(
                const NTxProxy::TTxProxyServices& services)
            : Services(services)
        { }
    };

    struct TEvUploadRowsResponse: public TEventLocal<TEvUploadRowsResponse, EvUploadRowsResponse> {
        Ydb::StatusIds::StatusCode Status;
        NYql::TIssues Issues;

        TEvUploadRowsResponse(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues)
            : Status(status)
            , Issues(issues)
        {}
    };
};

struct TEvTxProxyReq {
    enum EEv {
        EvMakeRequest = EventSpaceBegin(TKikimrEvents::ES_TX_PROXY_REQ),
        EvSchemeRequest,
        EvNavigateScheme,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_PROXY_REQ), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_PROXY_REQ)");

    struct TEvMakeRequest : public TEventLocal<TEvMakeRequest, EvMakeRequest> {
        TEvTxUserProxy::TEvProposeTransaction::TPtr Ev;

        TEvMakeRequest(TEvTxUserProxy::TEvProposeTransaction::TPtr &ev)
            : Ev(ev)
        {}
    };

    struct TEvSchemeRequest : public TEventLocal<TEvSchemeRequest, EvSchemeRequest> {
        TEvTxUserProxy::TEvProposeTransaction::TPtr Ev;

        TEvSchemeRequest(TEvTxUserProxy::TEvProposeTransaction::TPtr &ev)
            : Ev(ev)
        {}
    };

    struct TEvNavigateScheme : public TEventLocal<TEvNavigateScheme, EvNavigateScheme> {
        TEvTxUserProxy::TEvNavigate::TPtr Ev;

        TEvNavigateScheme(TEvTxUserProxy::TEvNavigate::TPtr &ev)
            : Ev(ev)
        {}
    };
};

namespace NTxProxy {

    using TTableColumnInfo = TSysTables::TTableColumnInfo;

    struct TSchemeCacheConfig;

    struct TRequestControls {
    private:
        bool Registered;

    public:
        TControlWrapper PerRequestDataSizeLimit;
        TControlWrapper PerShardIncomingReadSetSizeLimit;
        TControlWrapper DefaultTimeoutMs;
        TControlWrapper MaxShardCount;
        TControlWrapper MaxReadSetCount;

        TRequestControls()
            : Registered(false)
            , PerRequestDataSizeLimit(53687091200, 0, Max<i64>())
            , PerShardIncomingReadSetSizeLimit(209715200, 0, 5368709120)
            , DefaultTimeoutMs(600000, 0, 3600000)
            , MaxShardCount(10000, 0, 1000000)
            , MaxReadSetCount(1000000, 0, 100000000)
        {}

        void Reqister(const TActorContext &ctx) {
            if (Registered) {
                return;
            }

            AppData(ctx)->Icb->RegisterSharedControl(PerRequestDataSizeLimit,
                                                     "TxLimitControls.PerRequestDataSizeLimit");
            AppData(ctx)->Icb->RegisterSharedControl(PerShardIncomingReadSetSizeLimit,
                                                     "TxLimitControls.PerShardIncomingReadSetSizeLimit");
            AppData(ctx)->Icb->RegisterSharedControl(DefaultTimeoutMs,
                                                     "TxLimitControls.DefaultTimeoutMs");
            AppData(ctx)->Icb->RegisterSharedControl(MaxShardCount,
                                                     "TxLimitControls.MaxShardCount");
            AppData(ctx)->Icb->RegisterSharedControl(MaxReadSetCount,
                                                     "TxLimitControls.MaxReadSetCount");

            Registered = true;
        }
    };

    IActor* CreateTxProxyDataReq(const TTxProxyServices &services, const ui64 txid, const TIntrusivePtr<TTxProxyMon>& txProxyMon, const TRequestControls& requestControls);
    IActor* CreateTxProxyFlatSchemeReq(const TTxProxyServices &services, const ui64 txid, TAutoPtr<TEvTxProxyReq::TEvSchemeRequest> request, const TIntrusivePtr<TTxProxyMon>& txProxyMon);
    IActor* CreateTxProxyDescribeFlatSchemeReq(const TTxProxyServices &services, const TIntrusivePtr<TTxProxyMon>& txProxyMon);
    IActor* CreateTxProxySnapshotReq(const TTxProxyServices &services, const ui64 txid, TEvTxUserProxy::TEvProposeTransaction::TPtr&& ev, const TIntrusivePtr<TTxProxyMon>& mon);
    IActor* CreateTxProxyCommitWritesReq(const TTxProxyServices &services, const ui64 txid, TEvTxUserProxy::TEvProposeTransaction::TPtr&& ev, const TIntrusivePtr<TTxProxyMon>& mon);
}

IActor* CreateTxProxy(const TVector<ui64> &allocators);
TActorId MakeTxProxyID();

}
