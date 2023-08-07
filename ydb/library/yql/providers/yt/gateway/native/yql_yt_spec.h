#pragma once

#include "yql_yt_exec_ctx.h"

#include <ydb/library/yql/providers/yt/gateway/lib/transaction_cache.h>

#include <ydb/library/yql/providers/yt/lib/expr_traits/yql_expr_traits.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>

#include <yt/cpp/mapreduce/interface/operation.h>
#include <library/cpp/yson/node/node.h>

#include <util/generic/flags.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/typetraits.h>

namespace NYql {

namespace NNative {

enum class EYtOpProp: ui32 {
    IntermediateData      = 1 << 0,
    TemporaryAutoMerge    = 1 << 1,
    PublishedAutoMerge    = 1 << 2,
    WithMapper            = 1 << 3,
    WithReducer           = 1 << 4,
    WithUserJobs          = 1 << 5,
    AllowSampling         = 1 << 6,
    TemporaryChunkCombine = 1 << 7,
    PublishedChunkCombine = 1 << 8,
};

Y_DECLARE_FLAGS(EYtOpProps, EYtOpProp);
Y_DECLARE_OPERATORS_FOR_FLAGS(EYtOpProps);

///////////////////////////////////////////////////////////////////////////////////////////////////////

TMaybe<TString> GetPool(
    const TExecContextBase& execCtx,
    const TYtSettings::TConstPtr& settings);

void FillSpec(NYT::TNode& spec,
    const TExecContextBase& execCtx,
    const TYtSettings::TConstPtr& settings,
    const TTransactionCache::TEntry::TPtr& entry,
    double extraCpu,
    const TMaybe<double>& secondExtraCpu,
    EYtOpProps opProps = 0);

void FillSecureVault(NYT::TNode& spec, const IYtGateway::TSecureParams& secureParams);

void FillUserJobSpecImpl(NYT::TUserJobSpec& spec,
    const TExecContextBase& execCtx,
    const TYtSettings::TConstPtr& settings,
    const TExpressionResorceUsage& extraUsage,
    ui64 fileMemUsage,
    ui64 llvmMemUsage,
    bool localRun,
    const TString& cmdPrefix);

void FillOperationOptionsImpl(NYT::TOperationOptions& opOpts,
    const TYtSettings::TConstPtr& settings,
    const TTransactionCache::TEntry::TPtr& entry);

///////////////////////////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {
    Y_HAS_MEMBER(SecureParams);
}

template <class TOptions>
inline void FillSpec(NYT::TNode& spec,
    const TExecContext<TOptions>& execCtx,
    const TTransactionCache::TEntry::TPtr& entry,
    double extraCpu,
    const TMaybe<double>& secondExtraCpu,
    EYtOpProps opProps = 0)
{
    FillSpec(spec, execCtx, execCtx.Options_.Config(), entry, extraCpu, secondExtraCpu, opProps);
    if constexpr (NPrivate::THasSecureParams<TOptions>::value) {
        FillSecureVault(spec, execCtx.Options_.SecureParams());
    }
}

template <class TDerived, class TExecParamsPtr>
inline void FillOperationSpec(NYT::TUserOperationSpecBase<TDerived>& spec, const TExecParamsPtr& execCtx) {
    if (auto val = execCtx->Options_.Config()->DefaultMaxJobFails.Get()) {
        spec.MaxFailedJobCount(*val);
    }
    if (auto val = execCtx->Options_.Config()->CoreDumpPath.Get()) {
        spec.CoreTablePath(*val);
    }
}

template <class TExecParamsPtr>
inline void FillUserJobSpec(NYT::TUserJobSpec& spec,
    const TExecParamsPtr& execCtx,
    const TExpressionResorceUsage& extraUsage,
    ui64 fileMemUsage,
    ui64 llvmMemUsage,
    bool localRun,
    const TString& cmdPrefix = {})
{
    FillUserJobSpecImpl(spec, *execCtx, execCtx->Options_.Config(), extraUsage, fileMemUsage, llvmMemUsage, localRun, cmdPrefix);
}

template <class TExecParamsPtr>
inline void FillOperationOptions(NYT::TOperationOptions& opOpts,
    const TExecParamsPtr& execCtx,
    const TTransactionCache::TEntry::TPtr& entry)
{
    FillOperationOptionsImpl(opOpts, execCtx->Options_.Config(), entry);
}

} // NNative

} // NYql
