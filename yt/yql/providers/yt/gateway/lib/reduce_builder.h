#pragma once

#include "exec_ctx.h"

#include <yt/yql/providers/yt/job/yql_job_user_base.h>
#include <yt/yql/providers/yt/lib/lambda_builder/lambda_builder.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

class TReduceJobBuilder: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TReduceJobBuilder>;

    virtual ~TReduceJobBuilder() = default;

    template<class ExecCtxPtr>
    TString SetReduceLambdaCode(TYqlUserJobBase* reduceJob, NNodes::TYtReduce reduce, ExecCtxPtr execCtx, TExprContext& ctx);

    template<class ExecCtxPtr>
    void SetReduceJobParams(
        TYqlUserJobBase* reduceJob,
        ExecCtxPtr execCtx,
        const TVector<ui32>& groups,
        const TVector<TString>& tables,
        const TVector<ui64>& rowOffsets,
        const THashSet<TString>& auxColumns
    );

    void SetInputType(TYqlUserJobBase* reduceJob, NNodes::TYtReduce reduce);
};

} // namespace NYql

#include "reduce_builder-inl.h"
