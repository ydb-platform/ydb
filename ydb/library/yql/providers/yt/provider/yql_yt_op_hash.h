#pragma once

#include "yql_yt_provider.h"

#include <ydb/library/yql/providers/yt/lib/hash/yql_op_hash.h>

namespace NYql {

class TYtNodeHashCalculator: public TNodeHashCalculator {
public:
    TYtNodeHashCalculator(const TYtState::TPtr& state, const TString& cluster, const TYtSettings::TConstPtr& config);

    static TString MakeSalt(const TYtSettings::TConstPtr& config, const TString& cluster);

private:
    TString GetOutputHash(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) const;
    TString GetOutTableHash(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) const;
    TString GetTableHash(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) const;
    TString GetOperationHash(const TExprNode& node, TArgIndex& argIndex, ui32 frameLevel) const;

private:
    const bool DontFailOnMissingParentOpHash;
    const TYtState::TPtr State;
    const TString Cluster;
    const TYtSettings::TConstPtr Configuration;
};

} // NYql
