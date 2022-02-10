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
