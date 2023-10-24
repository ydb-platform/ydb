#pragma once

#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr {

// function returns status if we are ready to reply with a specific status to the client
// and Ydb::StatusIds::StatusCode:: otherwise - when operation or transaction is in the progress.
Ydb::StatusIds::StatusCode YdbStatusFromProxyStatus(TEvTxUserProxy::TEvProposeTransactionStatus* msg);

bool IsTxProxyInProgress(const Ydb::StatusIds::StatusCode& code);

}