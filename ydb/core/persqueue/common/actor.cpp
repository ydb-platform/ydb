#include "actor.h"

namespace NKikimr::NPQ {

const TString& TConstantLogPrefix::GetLogPrefix() const {
    if (!LogPrefix_) {
        LogPrefix_ = BuildLogPrefix();
    }
    return *LogPrefix_;
}

}
