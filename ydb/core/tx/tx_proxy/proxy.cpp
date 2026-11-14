#include "proxy.h"

namespace NKikimr {

TActorId MakeTxProxyID() {
    return TActorId(0, TStringBuf("TxProxyServ"));
}

}


TString NKikimr::TEvTxUserProxy::TEvProposeTransactionStatus::ToString() const {
    TStringStream str;
    str << "{TEvProposeTransactionStatus";
    // Transaction
    if (Record.HasTxId()) {
        str << " txid# " << Record.GetTxId();
    }
    if (Record.HasStatus()) {
        str << " Status# " << Record.GetStatus();
    }
    str << "}";
    return str.Str();
}

namespace NKikimr {

bool TEvTxUserProxy::TEvProposeTransaction::HasModifyScheme() const {
    const NKikimrTxUserProxy::TTransaction &tx = Record.GetTransaction();
    return tx.HasModifyScheme();
}

bool TEvTxUserProxy::TEvProposeTransaction::HasTransactionalModification() const {
    const NKikimrTxUserProxy::TTransaction &tx = Record.GetTransaction();
    return tx.TransactionalModificationSize();
}

bool TEvTxUserProxy::TEvProposeTransaction::HasMakeProposal() const {
        const NKikimrTxUserProxy::TTransaction &tx = Record.GetTransaction();
        return (tx.HasMiniKQLTransaction() && tx.GetMiniKQLTransaction().HasProgram())
                || tx.HasReadTableTransaction();
}

bool TEvTxUserProxy::TEvProposeTransaction::HasSnapshotProposal() const {
    const auto& tx = Record.GetTransaction();
    return (tx.HasCreateVolatileSnapshot() ||
            tx.HasRefreshVolatileSnapshot() ||
            tx.HasDiscardVolatileSnapshot());
}

bool TEvTxUserProxy::TEvProposeTransaction::HasCommitWritesProposal() const {
    const auto& tx = Record.GetTransaction();
    return tx.HasCommitWrites();
}

bool TEvTxUserProxy::TEvProposeTransaction::NeedTxId() const {
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

TEvTxUserProxy::TEvInvalidateTable::TEvInvalidateTable(const TTableId& tableId) {
    Record.SetSchemeShardId(tableId.PathId.OwnerId);
    Record.SetTableId(tableId.PathId.LocalPathId);
}

} // namespace NKikimr

namespace NKikimr::NTxProxy {

void TRequestControls::Reqister(const TActorContext &ctx) {
    if (Registered) {
        return;
    }

    auto& icb = *AppData(ctx)->Icb;
    TControlBoard::RegisterSharedControl(PerRequestDataSizeLimit,
                                         icb.TxLimitControls.PerRequestDataSizeLimit);
    TControlBoard::RegisterSharedControl(PerShardIncomingReadSetSizeLimit,
                                         icb.TxLimitControls.PerShardIncomingReadSetSizeLimit);
    TControlBoard::RegisterSharedControl(DefaultTimeoutMs,
                                         icb.TxLimitControls.DefaultTimeoutMs);
    TControlBoard::RegisterSharedControl(MaxShardCount,
                                         icb.TxLimitControls.MaxShardCount);
    TControlBoard::RegisterSharedControl(MaxReadSetCount,
                                         icb.TxLimitControls.MaxReadSetCount);

    Registered = true;
}

} // namespace NKikimr::NTxProxy
