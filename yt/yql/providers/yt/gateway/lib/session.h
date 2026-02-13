#pragma once

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>
#include <yt/yql/providers/yt/gateway/lib/transaction_cache.h>

namespace NYql {

struct TSessionBase: public TThrRefBase {
    using TPtr = TIntrusivePtr<TSessionBase>;

    TSessionBase(
        const TString& sessionId,
        const TString& userName,
        TIntrusivePtr<IRandomProvider> randomProvider,
        TIntrusivePtr<ITimeProvider> timeProvider,
        const TYqlOperationOptions& operationOptions,
        const TOperationProgressWriter& progressWriter
    );

    virtual ~TSessionBase() = default;

    NYT::TNode CreateSpecWithDesc(const TVector<std::pair<TString, TString>>& code = {}) const;

    const TString UserName_;
    const TIntrusivePtr<IRandomProvider> RandomProvider_;
    const TIntrusivePtr<ITimeProvider> TimeProvider_;
    TString SessionId_;
    const TYqlOperationOptions OperationOptions_;
    const TOperationProgressWriter ProgressWriter_;
};

} // NYql
