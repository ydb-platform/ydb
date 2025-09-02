#include "event.h"

namespace NKikimr::NColumnShard::NSubscriber {

TString TEventTxCompleted::DoDebugString() const {
        return "tx_id=" + std::to_string(TxId);
}

} //namespace NKikimr::NColumnShard::NSubscriber
