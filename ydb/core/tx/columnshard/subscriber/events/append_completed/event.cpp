#include "event.h"
#include <util/string/join.h>

namespace NKikimr::NColumnShard::NSubscriber {

TString TEventAppendCompleted::DoDebugString() const {
    return "event on completion of TChangesWithAppend";
}

}
