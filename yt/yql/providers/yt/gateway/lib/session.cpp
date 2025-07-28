#include "session.h"

namespace NYql {

TSessionBase::TSessionBase(const TString& sessionId, const TString& userName, TIntrusivePtr<IRandomProvider> randomProvider)
    : UserName_(userName)
    , RandomProvider_(randomProvider)
    , SessionId_(sessionId)
{
}

} // NYql
