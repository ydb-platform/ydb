#pragma once

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>

namespace NYql {

struct TSessionBase: public TThrRefBase {
    using TPtr = TIntrusivePtr<TSessionBase>;

    TSessionBase(const TString& sessionId, const TString& userName, TIntrusivePtr<IRandomProvider> randomProvider);

    virtual ~TSessionBase() = default;

    const TString UserName_;
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    TString SessionId_;
};

} // NYql
