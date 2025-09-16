#include "actor.h"

namespace NKikimr::NPQ {

const TString& TCachedLogPrefix::GetLogPrefix() const {
    if (!LogPrefix_) {
        LogPrefix_ = DoGetLogPrefix();
    }
    return *LogPrefix_;
}

}
