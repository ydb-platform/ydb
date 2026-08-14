#include "dq_checkpoints_states.h"

#include <util/stream/str.h>

namespace NYql::NDq {

bool TComputeActorState::ParseFromString(const TString& in) {
    TStringStream str(in);
    Load(&str);
    return true;
}

bool TComputeActorState::SerializeToString(TString* out) const { 
    TStringStream result;
    Save(&result);
    *out = result.Str();
    return true;
}

} // namespace NYql::NDq
