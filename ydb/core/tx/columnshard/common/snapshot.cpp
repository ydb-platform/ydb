#include "snapshot.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap {

TString TSnapshot::DebugString() const {
    return TStringBuilder() << "plan_step=" << PlanStep << ";tx_id=" << TxId << ";";
}

};
