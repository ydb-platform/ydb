#pragma once

#include "kqp_prepare.h"

namespace NKikimr {
namespace NKqp {

enum class ETableReadType {
    Unspecified,
    FullScan,
    Scan,
    Lookup,
    MultiLookup,
};

enum class ETableWriteType {
    Unspecified,
    Upsert,
    MultiUpsert,
    Erase,
    MultiErase,
};

bool HasEffects(const NYql::NNodes::TKiProgram& program);
bool HasResults(const NYql::NNodes::TKiProgram& program);

NYql::NNodes::TCoList GetEmptyEffectsList(NYql::TPositionHandle pos, NYql::TExprContext& ctx);

TMkqlExecuteResult ExecuteMkql(NYql::NNodes::TKiProgram program, TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, NYql::TExprContext& ctx, TIntrusivePtr<TKqpTransactionState> txState,
    TIntrusivePtr<TKqlTransformContext> transformCtx, bool hasDataEffects);

void LogMkqlResult(const NKikimrMiniKQL::TResult& result, NYql::TExprContext& ctx);

} // namespace NKqp 
} // namespace NKikimr
