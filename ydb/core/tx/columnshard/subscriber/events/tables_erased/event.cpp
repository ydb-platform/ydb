#include "event.h"
#include <util/string/join.h>

namespace NKikimr::NColumnShard::NSubscriber {

TString TEventTablesErased::DoDebugString() const {
    return "paths=" + JoinSeq(",", PathIds);
}

}