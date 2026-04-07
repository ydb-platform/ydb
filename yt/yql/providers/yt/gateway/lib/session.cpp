#include "session.h"
#include <yt/yql/providers/yt/gateway/lib/yt_helpers.h>

namespace NYql {

TSessionBase::TSessionBase(
    const TString& sessionId,
    const TString& userName,
    TIntrusivePtr<IRandomProvider> randomProvider,
    TIntrusivePtr<ITimeProvider> timeProvider,
    const TYqlOperationOptions& operationOptions,
    const TOperationProgressWriter& progressWriter
)
    : UserName_(userName)
    , RandomProvider_(randomProvider)
    , TimeProvider_(timeProvider)
    , SessionId_(sessionId)
    , OperationOptions_(operationOptions)
    , ProgressWriter_(progressWriter)
{
}

NYT::TNode TSessionBase::CreateSpecWithDesc(const TVector<std::pair<TString, TString>>& code) const {
    return YqlOpOptionsToSpec(OperationOptions_, UserName_, code);
}

} // NYql
