#include "actor.h"

namespace NKikimr::NPQ {

const TString& TConstantLogPrefix::GetLogPrefix() const {
    if (!LogPrefix_.Defined()) {
        LogPrefix_ = BuildLogPrefix();
    }
    return *LogPrefix_;
}

}
