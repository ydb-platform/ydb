#pragma once

#include "vm_defs.h"

namespace NSc::NUt {

    TMaybe<TIdx> ParseIdx(TVMState& st);
    TMaybe<TPos> ParsePos(TVMState& state);
    TMaybe<TRef> ParseRef(TVMState& state);
    TMaybe<TSrc> ParseSrc(TVMState& state);
    TMaybe<TDst> ParseDst(TVMState& state);
    TMaybe<TPath> ParsePath(TVMState& state);

    TMaybe<TVMAction> ParseNextAction(TVMState& state);

}
