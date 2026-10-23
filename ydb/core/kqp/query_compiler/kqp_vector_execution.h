#pragma once

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/protos/kqp_physical.pb.h>

namespace NKikimr::NKqp {

// Recognize the normalized physical expression, never SQL text. Failure leaves
// the ordinary stages intact. The descriptor has the lifetime of the cached plan.
void TryCompileVectorExecution(const NYql::NNodes::TKqpPhysicalTx& tx,
    NKqpProto::TKqpPhyTx& proto);

} // namespace NKikimr::NKqp
